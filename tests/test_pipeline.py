import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pipeline


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "test.db"

    def tearDown(self):
        self.temp.cleanup()

    def write_csv(self, name, rows, fieldnames=None):
        fieldnames = fieldnames or pipeline.REQUIRED
        path = self.root / name
        with open(path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def valid(self, event_id="EVT1", product_id="P00001"):
        return {
            "event_id": event_id,
            "customer_id": "CUST1",
            "product_id": product_id,
            "event_ts": "2026-01-01T10:00:00Z",
            "event_type": "purchase",
            "quantity": "2",
            "unit_price": "19.99",
        }

    def test_happy_path_and_empty_input(self):
        source = self.write_csv("happy.csv", [self.valid()])
        run_id, counts = pipeline.run_pipeline(source, self.db, run_id="happy")
        self.assertEqual(counts["input"], 1)
        self.assertEqual(counts["valid"], 1)
        self.assertEqual(counts["inserted"], 1)
        connection = sqlite3.connect(self.db)
        self.assertEqual(connection.execute("select count(*) from events").fetchone()[0], 1)
        connection.close()

        empty = self.write_csv("empty.csv", [])
        pipeline.run_pipeline(empty, self.db, run_id="empty")
        report = pipeline.reconcile(self.db)
        self.assertTrue(report["reconciliation_passed"])

    def test_malformed_null_duplicate_invalid_type_and_bad_reference(self):
        rows = [
            self.valid("EVT1"),
            self.valid("EVT1"),
            {**self.valid("EVT2"), "customer_id": ""},
            {**self.valid("EVT3"), "event_type": "cancelled"},
            {**self.valid("EVT4"), "product_id": "P99999"},
            {**self.valid("EVT5"), "quantity": "1.5"},
        ]
        source = self.write_csv("bad.csv", rows)
        _, counts = pipeline.run_pipeline(source, self.db, run_id="bad")
        connection = pipeline.connect(self.db)
        self.assertEqual(counts["rejected"], 5)
        self.assertEqual(connection.execute("select count(*) from events").fetchone()[0], 1)
        reasons = {row[0] for row in connection.execute("select reason from rejects")}
        connection.close()
        self.assertIn("duplicate_event_id_in_batch", reasons)
        self.assertIn("invalid_product_reference", reasons)

    def test_malformed_json_and_unexpected_json_schema(self):
        source = self.root / "events.jsonl"
        source.write_text(
            "{bad json}\n" + json.dumps(self.valid("EVT1")) + "\n"
            + json.dumps({**self.valid("EVT2"), "unexpected": True}) + "\n",
            encoding="utf-8",
        )
        _, counts = pipeline.run_pipeline(source, self.db, run_id="json")
        self.assertEqual(counts["input"], 3)
        self.assertEqual(counts["valid"], 1)
        self.assertEqual(counts["rejected"], 2)

    def test_unexpected_csv_schema_is_recorded_as_failed_run(self):
        source = self.write_csv("wrong.csv", [], ["event_id", "customer_id"])
        with self.assertRaises(ValueError):
            pipeline.run_pipeline(source, self.db, run_id="wrong-schema")
        connection = pipeline.connect(self.db)
        status = connection.execute(
            "select status from runs where run_id = 'wrong-schema'"
        ).fetchone()[0]
        connection.close()
        self.assertEqual(status, "failed")

    def test_rerun_is_idempotent(self):
        source = self.write_csv("rerun.csv", [self.valid()])
        pipeline.run_pipeline(source, self.db, run_id="first")
        pipeline.run_pipeline(source, self.db, run_id="retry")
        connection = sqlite3.connect(self.db)
        count = connection.execute("select count(*) from events").fetchone()[0]
        connection.close()
        self.assertEqual(count, 1)

    def test_nested_output_directory_is_created(self):
        source = self.write_csv("nested.csv", [self.valid()])
        nested_db = self.root / "nested" / "output" / "pipeline.db"
        pipeline.run_pipeline(source, nested_db, run_id="nested")
        self.assertTrue(nested_db.exists())

    def test_timezone_and_price_domain_edges(self):
        no_timezone = {**self.valid("EVT1"), "event_ts": "2026-01-01T10:00:00"}
        negative_price = {**self.valid("EVT2"), "unit_price": "-1.00"}
        source = self.write_csv("domains.csv", [no_timezone, negative_price])
        _, counts = pipeline.run_pipeline(source, self.db, run_id="domains")
        self.assertEqual(counts["rejected"], 2)


if __name__ == "__main__":
    unittest.main()
