-- Target and run reconciliation
SELECT COUNT(*) AS target_event_count FROM events;
SELECT COUNT(DISTINCT event_id) AS distinct_target_keys FROM events;

-- Rejects by reason
SELECT reason, COUNT(*) AS reject_count
FROM rejects
GROUP BY reason
ORDER BY reject_count DESC, reason;

-- Run audit
SELECT run_id, status, input_rows, valid_rows, inserted_rows, rejected_rows,
       started_at, ended_at, failure_reason
FROM runs
ORDER BY started_at;

-- Deterministic business fact: revenue by category by event type
SELECT p.category, e.event_type, SUM(CAST(e.amount AS REAL)) AS amount
FROM events e
JOIN products p ON p.product_id = e.product_id
WHERE e.event_type IN ('purchase', 'refund')
GROUP BY p.category, e.event_type
ORDER BY p.category, e.event_type;
