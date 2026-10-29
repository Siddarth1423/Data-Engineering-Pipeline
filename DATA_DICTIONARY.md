# Data dictionary

| Table | Column | Type | Nullable | Definition |
|---|---|---|---|---|
| products | product_id | TEXT PK | no | Stable synthetic product key |
| products | category | TEXT | no | Product category |
| events | event_id | TEXT PK | no | Unique immutable source event key |
| events | customer_id | TEXT | no | Stable customer identifier |
| events | product_id | TEXT FK | no | References products |
| events | event_ts | TEXT UTC ISO-8601 | no | Event occurrence timestamp |
| events | event_type | TEXT | no | view, add_to_cart, purchase, refund |
| events | quantity | INTEGER | no | Positive quantity |
| events | unit_price | TEXT decimal | no | Positive price represented as decimal string |
| events | amount | TEXT decimal | no | quantity * unit_price |
| events | source_file | TEXT | no | Source file base name |
| events | ingested_at | TEXT UTC ISO-8601 | no | Load timestamp |
| events | run_id | TEXT | no | Audit execution identifier |
| rejects | reject_id | TEXT PK | no | Quarantine record key |
| rejects | run_id | TEXT | no | Executing run |
| rejects | source_file | TEXT | no | Source file |
| rejects | source_row | INTEGER | no | Physical CSV row or JSON line |
| rejects | source_record | TEXT JSON | no | Traceable serialized source record |
| rejects | reason | TEXT | no | Machine-readable rejection reason |
| rejects | created_at | TEXT | no | Reject timestamp |
| runs | run_id | TEXT PK | no | Stable execution identifier |
| runs | source_file | TEXT | no | Source path |
| runs | target_db | TEXT | no | Target SQLite database |
| runs | status | TEXT | no | running, success, or failed |
| runs | started_at/ended_at | TEXT UTC ISO-8601 | yes | Execution bounds |
| runs | input_rows | INTEGER | no | Parsed records attempted |
| runs | valid_rows | INTEGER | no | Records passing schema/rules before reference and conflict checks |
| runs | inserted_rows | INTEGER | no | UPSERT attempts, including identical reruns |
| runs | rejected_rows | INTEGER | no | Quarantined records |
| runs | failure_reason | TEXT | yes | Exception type and message |

Relationships: events.product_id -> products.product_id. Runs do not have a foreign key to source files because source files are filesystem inputs.
