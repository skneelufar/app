# Reconciliation report

**Overall: PASS**

| # | Check | Result | Detail |
|---|-------|--------|--------|
| 1 | ingest: rows_in == accepted + rejected | PASS | rows_in=58624 accepted=54311 rejected=4313 |
| 2 | ingest: raw_events rows == accepted event rows | PASS | raw_events=53911 accepted_events=53911 |
| 3 | ingest: rejects table rows == rejected counted in runs | PASS | rejects=4313 runs_rejected=4313 |
| 4 | traceability: rejects and raw rows map to a run, file and line | PASS | untraceable rejects=0 raw=0 |
| 5 | full_refresh: equals independent oracle at watermark 55528 | PASS | rows=20000 expected=20000 missing=0 extra=0 mismatched=0 |
| 6 | full_refresh: primary key unique | PASS | rows=20000 distinct_order_id=20000 |
| 7 | full_refresh: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 8 | full_refresh: source-to-target count explained | PASS | raw_events=53911 - superseded_versions=33911 = distinct_orders=20000; target=20000 |
| 9 | incremental_merge: equals independent oracle at watermark 55528 | PASS | rows=20000 expected=20000 missing=0 extra=0 mismatched=0 |
| 10 | incremental_merge: primary key unique | PASS | rows=20000 distinct_order_id=20000 |
| 11 | incremental_merge: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 12 | incremental_merge: source-to-target count explained | PASS | raw_events=53911 - superseded_versions=33911 = distinct_orders=20000; target=20000 |
| 13 | partition_replace: equals independent oracle at watermark 55528 | PASS | rows=20000 expected=20000 missing=0 extra=0 mismatched=0 |
| 14 | partition_replace: primary key unique | PASS | rows=20000 distinct_order_id=20000 |
| 15 | partition_replace: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 16 | partition_replace: source-to-target count explained | PASS | raw_events=53911 - superseded_versions=33911 = distinct_orders=20000; target=20000 |
| 17 | equivalence: full_refresh == incremental_merge (all rows, all business columns) | PASS | rows 20000 vs 20000 |
| 18 | equivalence: per-date count/amount/net aggregates full_refresh == incremental_merge | PASS | 30 partitions compared |
| 19 | equivalence: full_refresh == partition_replace (all rows, all business columns) | PASS | rows 20000 vs 20000 |
| 20 | equivalence: per-date count/amount/net aggregates full_refresh == partition_replace | PASS | 30 partitions compared |
| 21 | labels: every labelled invalid record rejected with the expected reason (and nothing else rejected) | PASS | {"expected": 4313, "actual": 4313, "missed": 0, "unexpected": 0, "wrong_reason": 0} |

## Source-to-target

- landed events (raw_events): 53911
- distinct orders: 20000 (superseded versions: 33911)
- rejected input rows: 4313

### Rejects by reason

| reason | rows |
|--------|------|
| amount_out_of_range | 180 |
| duplicate_event | 1618 |
| invalid_amount | 180 |
| invalid_currency | 179 |
| invalid_order_id | 180 |
| invalid_order_ts | 180 |
| invalid_status | 180 |
| invalid_updated_at | 180 |
| malformed_row | 179 |
| missing_amount | 180 |
| missing_customer_id | 180 |
| missing_order_id | 180 |
| negative_amount | 180 |
| order_ts_changed | 177 |
| unknown_customer | 180 |
| updated_before_order_ts | 180 |

## Key aggregates per target

| strategy | watermark | rows | sum amount (cents) | sum net amount (cents) |
|----------|-----------|------|--------------------|------------------------|
| full_refresh | 55528 | 20000 | 605017047 | 522514237 |
| incremental_merge | 55528 | 20000 | 605017047 | 522514237 |
| partition_replace | 55528 | 20000 | 605017047 | 522514237 |
