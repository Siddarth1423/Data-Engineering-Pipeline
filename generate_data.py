#!/usr/bin/env python3
import argparse
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

EVENT_TYPES = ["view", "add_to_cart", "purchase", "refund"]
CATEGORIES = ["home", "electronics", "books", "sports", "clothing"]


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "event_id", "customer_id", "product_id", "event_ts",
            "event_type", "quantity", "unit_price",
        ])
        writer.writeheader()
        writer.writerows(rows)


def generate(count, seed=2026, anomaly=False):
    random.seed(seed)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(count):
        offset = random.randint(0, 89 * 24 * 60)
        rows.append({
            "event_id": f"EVT{index + 1:09d}",
            "customer_id": f"CUST{random.randint(1, max(1, count // 5)):06d}",
            "product_id": f"P{random.randint(1, 100):05d}",
            "event_ts": (base + timedelta(minutes=offset)).isoformat().replace("+00:00", "Z"),
            "event_type": random.choice(EVENT_TYPES),
            "quantity": 1 + random.randint(0, 4),
            "unit_price": f"{random.randint(100, 9999) / 100:.2f}",
        })
    if anomaly:
        rows.extend([
            dict(rows[0]),
            {**rows[1], "product_id": "P99999", "event_id": "EVT_BAD_REF"},
            {**rows[2], "event_ts": "", "event_id": "EVT_BAD_NULL"},
        ])
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--anomalies", action="store_true")
    parser.add_argument("--output", default="data/generated/events_100k.csv")
    args = parser.parse_args()
    write_csv(Path(args.output), generate(args.count, args.seed, args.anomalies))
    print(f"Wrote {args.count + (3 if args.anomalies else 0)} rows to {args.output}")


if __name__ == "__main__":
    main()
