# Reconciliation report

**Overall: PASS**

| # | Check | Result | Detail |
|---|-------|--------|--------|
| 1 | ingest: rows_in == accepted + rejected | PASS | rows_in=614 accepted=562 rejected=52 |
| 2 | ingest: raw_events rows == accepted event rows | PASS | raw_events=542 accepted_events=542 |
| 3 | ingest: rejects table rows == rejected counted in runs | PASS | rejects=52 runs_rejected=52 |
| 4 | traceability: rejects and raw rows map to a run, file and line | PASS | untraceable rejects=0 raw=0 |
| 5 | full_refresh: equals independent oracle at watermark 560 | PASS | rows=201 expected=201 missing=0 extra=0 mismatched=0 |
| 6 | full_refresh: primary key unique | PASS | rows=201 distinct_order_id=201 |
| 7 | full_refresh: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 8 | full_refresh: source-to-target count explained | PASS | raw_events=542 - superseded_versions=341 = distinct_orders=201; target=201 |
| 9 | incremental_merge: equals independent oracle at watermark 560 | PASS | rows=201 expected=201 missing=0 extra=0 mismatched=0 |
| 10 | incremental_merge: primary key unique | PASS | rows=201 distinct_order_id=201 |
| 11 | incremental_merge: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 12 | incremental_merge: source-to-target count explained | PASS | raw_events=542 - superseded_versions=341 = distinct_orders=201; target=201 |
| 13 | partition_replace: equals independent oracle at watermark 560 | PASS | rows=201 expected=201 missing=0 extra=0 mismatched=0 |
| 14 | partition_replace: primary key unique | PASS | rows=201 distinct_order_id=201 |
| 15 | partition_replace: referential integrity (customer_id in dim_customers) | PASS | orphan_rows=0 |
| 16 | partition_replace: source-to-target count explained | PASS | raw_events=542 - superseded_versions=341 = distinct_orders=201; target=201 |
| 17 | equivalence: full_refresh == incremental_merge (all rows, all business columns) | PASS | rows 201 vs 201 |
| 18 | equivalence: per-date count/amount/net aggregates full_refresh == incremental_merge | PASS | 5 partitions compared |
| 19 | equivalence: full_refresh == partition_replace (all rows, all business columns) | PASS | rows 201 vs 201 |
| 20 | equivalence: per-date count/amount/net aggregates full_refresh == partition_replace | PASS | 5 partitions compared |
| 21 | labels: every labelled invalid record rejected with the expected reason (and nothing else rejected) | PASS | {"expected": 50, "actual": 50, "missed": 0, "unexpected": 0, "wrong_reason": 0} |

## Source-to-target

- landed events (raw_events): 542
- distinct orders: 201 (superseded versions: 341)
- rejected input rows: 52

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
| malformed_row | 4 |
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
| full_refresh | 560 | 201 | 105299674 | 4367258 |
| incremental_merge | 560 | 201 | 105299674 | 4367258 |
| partition_replace | 560 | 201 | 105299674 | 4367258 |
