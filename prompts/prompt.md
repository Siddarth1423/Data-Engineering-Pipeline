# Master implementation prompt

Act as a senior Python data engineer and independently implement a complete data engineering project in the current workspace.

## Objective

Process large CSV and JSON Lines inputs without loading an entire source file into memory. Demonstrate bounded memory use, correctness, reproducibility, maintainability, observability, and safe reruns.

## Requirements

1. Inspect the workspace and preserve useful existing files.
2. Implement streaming ingestion with configurable chunk size and paths.
3. Support CSV and JSONL.
4. Use local SQLite unless another target is already established.
5. Validate required fields, unexpected columns, empty IDs, timezone-aware ISO-8601 timestamps, supported event types, positive integer quantities, exact decimal prices, duplicate keys, existing conflicts, and product references.
6. Quarantine rejected records with run ID, source file, source row, original record, reason, and timestamp.
7. Record run status, start/end time, input count, valid count, loaded count, rejected count, and failure reason.
8. Make loads idempotent so identical reruns do not duplicate target records and conflicts are rejected.
9. Never silently ignore malformed input, schema changes, partial failures, or database errors.
10. Provide synthetic sample data and a seeded generator for 100,000+ records where practical.
11. Test happy path, empty input, malformed records, missing fields, duplicates, nulls, invalid references, unexpected schemas, timestamp/money edge cases, retry, and reruns.
12. Reconcile source counts, valid counts, reject counts, target counts, and unique keys.
13. Benchmark at least two chunk sizes and document one optimization.
14. Provide README, architecture, setup instructions, data dictionary, assumptions, limitations, retry/restart/backfill guidance, queries, and design trade-offs.
15. Add `AI_DISCLOSURE.md`, complete prompt history, and a repository-ready `.gitignore`.
16. Never commit credentials, personal data, secrets, proprietary code, generated databases, caches, or unnecessary large files.
17. Run available tests and pipeline commands, and report actual outcomes only. Do not fabricate results.
18. Before completion, inspect the repository and list files created or changed.
