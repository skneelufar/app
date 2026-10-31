# Reconciliation report

**Overall: PASS**

| # | Check | Result | Detail |
|---|-------|--------|--------|
| 1 | ingest: rows_in == accepted + rejected | PASS | rows_in=611 accepted=561 rejected=50 |
| 2 | ingest: raw_events rows == accepted event rows | PASS | raw_events=541 accepted_events=541 |
| 3 | ingest: rejects table rows == rejected counted in runs | PASS | rejects=50 runs_rejected=50 |
| 4 | traceability: rejects and raw rows map to a run, file and line | PASS | untraceable rejects=0 raw=0 |
| 5 | full_refresh: equals independent oracle at watermark 559 | PASS | rows=200 expected=200 missing=0 extra=0 mismatched=0 |
| 6 | full_refresh: primary key unique | PASS | rows=200 distinct_order_id=200 |
| 7 | full_refresh: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 8 | full_refresh: source-to-target count explained | PASS | raw_events=541 - superseded_versions=341 = distinct_orders=200; target=200 |
| 9 | incremental_merge: equals independent oracle at watermark 559 | PASS | rows=200 expected=200 missing=0 extra=0 mismatched=0 |
| 10 | incremental_merge: primary key unique | PASS | rows=200 distinct_order_id=200 |
| 11 | incremental_merge: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 12 | incremental_merge: source-to-target count explained | PASS | raw_events=541 - superseded_versions=341 = distinct_orders=200; target=200 |
| 13 | partition_replace: equals independent oracle at watermark 559 | PASS | rows=200 expected=200 missing=0 extra=0 mismatched=0 |
| 14 | partition_replace: primary key unique | PASS | rows=200 distinct_order_id=200 |
| 15 | partition_replace: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 16 | partition_replace: source-to-target count explained | PASS | raw_events=541 - superseded_versions=341 = distinct_orders=200; target=200 |
| 17 | equivalence: full_refresh == incremental_merge (all rows, all business columns) | PASS | rows 200 vs 200 |
| 18 | equivalence: per-date count/amount/net aggregates full_refresh == incremental_merge | PASS | 5 partitions compared |
| 19 | equivalence: full_refresh == partition_replace (all rows, all business columns) | PASS | rows 200 vs 200 |
| 20 | equivalence: per-date count/amount/net aggregates full_refresh == partition_replace | PASS | 5 partitions compared |
| 21 | labels: every labelled invalid record rejected with the expected reason (and nothing else rejected) | PASS | {"expected": 50, "actual": 50, "missed": 0, "unexpected": 0, "wrong_reason": 0} |

## Source-to-target

- landed events (raw_events): 541
- distinct orders: 200 (superseded versions: 341)
- rejected input rows: 50

### Rejects by reason

| reason | rows |
|--------|------|
| amount_out_of_range | 2 |
| duplicate_event | 19 |
| invalid_amount | 2 |
| invalid_currency | 2 |
| invalid_order_id | 2 |
| invalid_order_ts | 2 |
| invalid_status | 2 |
| invalid_updated_at | 2 |
| malformed_row | 2 |
| missing_amount | 2 |
| missing_customer_id | 2 |
| missing_order_id | 3 |
| negative_amount | 2 |
| order_ts_changed | 2 |
| unknown_customer | 2 |
| updated_before_order_ts | 2 |

## Key aggregates per target

| strategy | watermark | rows | sum amount (cents) | sum net amount (cents) |
|----------|-----------|------|--------------------|------------------------|
| full_refresh | 559 | 200 | 105297675 | 4367258 |
| incremental_merge | 559 | 200 | 105297675 | 4367258 |
| partition_replace | 559 | 200 | 105297675 | 4367258 |
