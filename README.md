# Streaming CSV/JSON event pipeline

## Project submission materials

AI use disclosure is in [AI_DISCLOSURE.md](AI_DISCLOSURE.md). The complete prompt history is in [prompts/prompt_history.md](prompts/prompt_history.md). The repository is an individual submission and contains no credentials or secrets.

## Problem
This vertical slice streams CSV or JSON Lines inputs without retaining the input in memory. It validates events, quarantines bad records, loads a SQLite analytical target, and records run/reject metadata.

## Architecture
Source CSV/JSONL -> streaming reader -> chunk buffer -> schema/business validation -> duplicate/reference checks -> SQLite upsert -> reconciliation/report.

This intentionally uses SQLite and the Python standard library so setup is local and reproducible. Memory is bounded by `--chunk-size`; a chunk plus SQLite state should remain near `O(chunk_size)`.

## Entities and grain
- `products`: synthetic product dimension, one row per `product_id`.
- `events`: one immutable business event per unique `event_id` (grain).
- `rejects`: one row per rejected source record, retaining the serialized source record and reason.
- `runs`: one row per execution attempt, with counts and status.

Stable IDs: generated synthetic identifiers use `EVT#########`, `CUST######`, and `P#####`. All timestamps are UTC ISO-8601. `run_id` and `source_file` are audit fields.

## Schema rules
- `event_id`, `customer_id`, `product_id`, `event_ts`, `event_type`, `quantity`, `unit_price` are required.
- `event_id`, customer, and product must be non-empty.
- `event_ts` must contain a timezone.
- Types: view, add_to_cart, purchase, refund.
- Quantity: positive integer.
- Unit price: positive decimal with exactly at most 2 fractional places; derived amount = quantity * unit price.
- Event ID is unique. A batch duplicate is rejected; an existing identical key is a no-op. Conflicting existing data is rejected.
- Product ID must exist in products.

## Setup
Python 3.10 or newer is required. No third-party packages are used.

```text
python generate_data.py --count 100000 --seed 2026 --output data/generated/events_100k.csv
python pipeline.py run data/sample/events.csv --db output/pipeline.db --run-id sample-v1
python pipeline.py reconcile --db output/pipeline.db
python -m unittest discover -s tests -v
python pipeline.py benchmark data/generated/events_100k.csv
```

On Windows, use `py -3` instead of `python` if needed.

## Retry, restart, and backfill
Delete/recreate the database only for a clean run. Re-running with the same source is safe: identical keys are UPSERTed without multiplying rows. Conflicting keys are quarantined. A failed run leaves committed earlier chunks and is visible as failed. Retry by assigning a new deterministic-or-random run ID (explicit run IDs help operators deduplicate execution evidence). Backfills should use a new source file and preserve immutable event history; overwrite semantics are intentionally rejected.

## Data dictionary
See [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## Validation and reconciliation
`pipeline.py reconcile` checks that input rows equal valid plus rejected rows for aggregate completed runs and verifies no duplicate target keys. The report is written to `output/reconciliation.json`. Sample query evidence is in `queries.sql`.

## Performance and optimization
The benchmark measures wall-clock throughput for multiple chunk sizes. The retained workload and one source pass also avoid repeatedly parsing product reference data: the local SQLite primary-key lookup is bounded and indexed. A larger deployment should replace repeated product lookups with a dictionary/cache or a database staging join, preserving bounded memory.

## Known limitations
SQLite is single-node; JSON Lines only, not nested JSON arrays. Product data is synthetic and initialized locally. A batch is transactional per chunk, not globally transactional. Derived money uses Decimal before SQLite string storage; SQL consumers cast amount if needed. There is no web/API or distributed scheduler integration.
