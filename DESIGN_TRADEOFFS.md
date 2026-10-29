# Design and trade-offs

The pipeline favors a standard-library implementation and SQLite to make correctness and setup easy. It streams CSV/JSONL and buffers only one configured chunk. The target is local and useful for the requested vertical slice, but not a distributed warehouse.

Money is parsed with `Decimal` before storage to avoid binary floating-point error; storage remains text for exact display and SQL consumers can cast it. Event loads are UPSERTs. Duplicate keys within a source are rejected, while identical reruns are allowed and conflicting reruns are quarantined.

Chunks are committed independently, so a later unexpected failure can leave earlier chunks loaded. This is useful for visibility and restartability; a future implementation can add SHA-256 file/checkpoint tables for exact resume.

Largest trade-offs are reference lookup cost, SQLite size limits, and lack of nested JSON support. Add a staging table and SQL join for a large distributed target, and add Parquet/object storage for analytical scale.
