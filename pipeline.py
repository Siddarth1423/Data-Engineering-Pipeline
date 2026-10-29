#!/usr/bin/env python3
import argparse
import csv
import json
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

REQUIRED = [
    "event_id", "customer_id", "product_id", "event_ts",
    "event_type", "quantity", "unit_price",
]
ALLOWED_TYPES = {"view", "add_to_cart", "purchase", "refund"}
MONEY_QUANTUM = Decimal("0.01")


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def connect(db_path):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS products (
            product_id TEXT PRIMARY KEY,
            category TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            product_id TEXT NOT NULL REFERENCES products(product_id),
            event_ts TEXT NOT NULL,
            event_type TEXT NOT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price TEXT NOT NULL,
            amount TEXT NOT NULL,
            source_file TEXT NOT NULL,
            ingested_at TEXT NOT NULL,
            run_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rejects (
            reject_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            source_file TEXT NOT NULL,
            source_row INTEGER NOT NULL,
            source_record TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            source_file TEXT NOT NULL,
            target_db TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            input_rows INTEGER NOT NULL DEFAULT 0,
            valid_rows INTEGER NOT NULL DEFAULT 0,
            inserted_rows INTEGER NOT NULL DEFAULT 0,
            rejected_rows INTEGER NOT NULL DEFAULT 0,
            failure_reason TEXT
        );
    """.replace("TEXT NOT NOT NULL", "TEXT NOT NULL"))
    connection.executemany(
        "INSERT OR IGNORE INTO products(product_id, category) VALUES (?, ?)",
        [(f"P{i:05d}", category) for i, category in enumerate(
            (["home", "electronics", "books", "sports", "clothing"] * 20), 1
        )],
    )
    connection.commit()
    return connection


def read_json_records(path):
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError("JSON record is not an object")
                yield line_number, record, None
            except json.JSONDecodeError as error:
                yield line_number, {}, f"malformed_json: {error.msg}"


def read_csv_records(path, expected_header=None):
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames or []
        if expected_header is not None and set(header) != set(expected_header):
            raise ValueError(
                "unexpected_schema: expected columns "
                + ",".join(expected_header)
                + "; received "
                + ",".join(header)
            )
        for line_number, record in enumerate(reader, 2):
            if None in record:
                yield line_number, record, "malformed_csv: extra columns"
            else:
                yield line_number, record, None


def records(path):
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from read_csv_records(path, REQUIRED)
    elif suffix in (".json", ".jsonl", ".ndjson"):
        yield from read_json_records(path)
    else:
        raise ValueError("unsupported input type; use CSV or JSON Lines")


def chunks(iterator, size):
    chunk = []
    for item in iterator:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def parse_iso8601(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("event_ts is required")
    text = value.strip()
    parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    if parsed.tzinfo is None:
        raise ValueError("event_ts must include timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate(record):
    if isinstance(record, dict):
        missing = [key for key in REQUIRED if key not in record]
        extra = sorted(set(record) - set(REQUIRED))
        if missing:
            return None, "missing_required_fields: " + ",".join(missing)
        if extra:
            return None, "unexpected_columns: " + ",".join(extra)
    else:
        return None, "record_must_be_object"

    normalized = {}
    for key in ("event_id", "customer_id", "product_id"):
        value = record[key]
        if value is None or str(value).strip() == "":
            return None, f"{key} is required"
        normalized[key] = str(value).strip()

    try:
        normalized["event_ts"] = parse_iso8601(record["event_ts"])
    except (ValueError, TypeError) as error:
        return None, f"invalid_event_ts: {error}"

    event_type = str(record["event_type"]).strip().lower()
    if event_type not in ALLOWED_TYPES:
        return None, "invalid_event_type"
    normalized["event_type"] = event_type

    try:
        quantity = int(record["quantity"])
        if quantity < 1:
            raise ValueError
        if str(quantity) != str(record["quantity"]).strip():
            raise ValueError
    except (ValueError, TypeError):
        return None, "invalid_quantity"
    normalized["quantity"] = quantity

    try:
        price = Decimal(str(record["unit_price"])).quantize(MONEY_QUANTUM)
        if price <= 0 or price != Decimal(str(record["unit_price"])):
            return None, "unit_price must be positive with up to 2 decimals"
    except (InvalidOperation, ValueError, ArithmeticError):
        return None, "invalid_unit_price"
    normalized["unit_price"] = format(price, ".2f")
    normalized["amount"] = format(price * quantity, ".2f")
    return normalized, None


def serialize_record(record):
    try:
        return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError):
        return repr(record)


def ensure_schema(connection, source_file, run_id, target_db):
    connection.execute(
        "INSERT INTO runs(run_id, source_file, target_db, status, started_at) VALUES (?, ?, ?, 'running', ?)",
        (run_id, str(source_file), str(target_db), utc_now()),
    )
    connection.commit()


def run_pipeline(source, db_path, chunk_size=1000, run_id=None):
    source = Path(source)
    db_path = Path(db_path)
    run_id = run_id or uuid.uuid5(uuid.NAMESPACE_URL, f"{source.resolve()}:{utc_now()}").hex
    connection = connect(db_path)
    ensure_schema(connection, source, run_id, db_path)
    counts = {"input": 0, "valid": 0, "inserted": 0, "rejected": 0}
    seen = set()
    source_name = source.name

    try:
        for chunk in chunks(records(source), chunk_size):
            prepared = []
            for source_row, record, read_error in chunk:
                counts["input"] += 1
                if read_error:
                    reason = read_error
                    valid = None
                else:
                    valid, reason = validate(record)
                if valid is not None and valid["event_id"] in seen:
                    reason = "duplicate_event_id_in_batch"
                    valid = None
                if valid is not None:
                    seen.add(valid["event_id"])
                    counts["valid"] += 1
                    prepared.append((source_row, valid))
                    continue
                reject(connection, run_id, source_name, source_row, record, reason, counts)

            with connection:
                for source_row, record in prepared:
                    product = connection.execute(
                        "SELECT 1 FROM products WHERE product_id = ?", (record["product_id"],)
                    ).fetchone()
                    if not product:
                        reject(connection, run_id, source_name, source_row, record,
                               "invalid_product_reference", counts)
                        counts["valid"] -= 1
                        continue
                    existing = connection.execute(
                        "SELECT customer_id, product_id, event_ts, event_type, quantity, unit_price, amount "
                        "FROM events WHERE event_id = ?", (record["event_id"],)
                    ).fetchone()
                    normalized_existing = tuple(existing) if existing else None
                    normalized_incoming = tuple(record[key] for key in (
                        "customer_id", "product_id", "event_ts", "event_type",
                        "quantity", "unit_price", "amount",
                    ))
                    if normalized_existing and normalized_existing != normalized_incoming:
                        reject(connection, run_id, source_name, source_row, record,
                               "event_id_conflict_with_existing_target", counts)
                        counts["valid"] -= 1
                        continue
                    connection.execute("""
                        INSERT INTO events (
                            event_id, customer_id, product_id, event_ts, event_type,
                            quantity, unit_price, amount, source_file, ingested_at, run_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(event_id) DO UPDATE SET
                            source_file=excluded.source_file,
                            ingested_at=excluded.ingested_at,
                            run_id=excluded.run_id
                    """, (
                        record["event_id"], record["customer_id"], record["product_id"],
                        record["event_ts"], record["event_type"], record["quantity"],
                        record["unit_price"], record["amount"], source_name, utc_now(), run_id,
                    ))
                    counts["inserted"] += 1

        connection.execute("""
            UPDATE runs SET status='success', ended_at=?, input_rows=?, valid_rows=?,
                inserted_rows=?, rejected_rows=?
            WHERE run_id=?
        """, (utc_now(), counts["input"], counts["valid"], counts["inserted"],
              counts["rejected"], run_id))
        connection.commit()
        return run_id, counts
    except Exception as error:
        connection.execute("""
            UPDATE runs SET status='failed', ended_at=?, input_rows=?, valid_rows=?,
                inserted_rows=?, rejected_rows=?, failure_reason=?
            WHERE run_id=?
        """, (utc_now(), counts["input"], counts["valid"], counts["inserted"],
              counts["rejected"], f"{type(error).__name__}: {error}", run_id))
        connection.commit()
        raise
    finally:
        connection.close()


def reject(connection, run_id, source_file, source_row, record, reason, counts):
    counts["rejected"] += 1
    connection.execute("""
        INSERT INTO rejects(reject_id, run_id, source_file, source_row, source_record, reason, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (uuid.uuid4().hex, run_id, source_file, source_row, serialize_record(record),
          reason, utc_now()))


def reconcile(db_path, output=None):
    connection = connect(db_path)
    target_count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    run_counts = connection.execute("""
        SELECT run_id, status, input_rows, valid_rows, inserted_rows, rejected_rows
        FROM runs ORDER BY started_at
    """).fetchall()
    totals = connection.execute("""
        SELECT COALESCE(SUM(input_rows), 0), COALESCE(SUM(valid_rows), 0),
               COALESCE(SUM(rejected_rows), 0)
        FROM runs WHERE status != 'running'
    """).fetchone()
    report = {
        "target_event_count": target_count,
        "runs": [dict(row) for row in run_counts],
        "aggregate_input_rows": totals[0],
        "aggregate_valid_rows": totals[1],
        "aggregate_rejected_rows": totals[2],
        "reconciliation_passed": totals[0] == totals[1] + totals[2],
        "duplicate_target_keys": connection.execute("""
            SELECT COUNT(*) FROM (
                SELECT event_id FROM events GROUP BY event_id HAVING COUNT(*) > 1
            )
        """).fetchone()[0],
    }
    connection.close()
    text = json.dumps(report, indent=2, sort_keys=True)
    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(text + "\n", encoding="utf-8")
    return report


def benchmark(source, db_path, chunk_sizes=(250, 1000)):
    results = []
    for chunk_size in chunk_sizes:
        started = time.perf_counter()
        run_id, counts = run_pipeline(source, db_path, chunk_size=chunk_size)
        elapsed = time.perf_counter() - started
        results.append({
            "chunk_size": chunk_size,
            "elapsed_seconds": round(elapsed, 4),
            "input_rows": counts["input"],
            "rows_per_second": round(counts["input"] / elapsed, 1) if elapsed else None,
            "run_id": run_id,
        })
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description="Streaming CSV/JSONL event pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("source")
    run.add_argument("--db", default="output/pipeline.db")
    run.add_argument("--chunk-size", type=int, default=1000)
    run.add_argument("--run-id")

    reconcile_parser = subparsers.add_parser("reconcile")
    reconcile_parser.add_argument("--db", default="output/pipeline.db")
    reconcile_parser.add_argument("--output", default="output/reconciliation.json")

    benchmark_parser = subparsers.add_parser("benchmark")
    benchmark_parser.add_argument("source")
    benchmark_parser.add_argument("--db", default="output/benchmark.db")

    args = parser.parse_args(argv)
    if args.command == "run":
        run_id, counts = run_pipeline(args.source, args.db, args.chunk_size, args.run_id)
        print(json.dumps({"run_id": run_id, "counts": counts}, sort_keys=True))
    elif args.command == "reconcile":
        report = reconcile(args.db, args.output)
        print(json.dumps(report, sort_keys=True))
        if not report["reconciliation_passed"] or report["duplicate_target_keys"]:
            return 1
    elif args.command == "benchmark":
        results = benchmark(args.source, args.db)
        print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
