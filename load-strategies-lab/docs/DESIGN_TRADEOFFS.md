# Design and trade-off note

## Problem framing
Daily files of order **events** (creates and updates, some late or out of order) feed an analytical table
`orders` (latest state per order). Three ways to publish the same logical result are compared:

| | full_refresh | incremental_merge | partition_replace |
|---|---|---|---|
| What it does | rebuild whole table from all history into a shadow table, then swap | UPSERT only events newer than the watermark | delete + re-insert each `order_date` partition touched by new events |
| Work per run | O(all history) | O(new events) | O(size of touched partitions) |
| Code complexity | lowest (no state, no watermark) | highest: watermark, ordering guard `(updated_at, event_id)`, requires a key | medium: partition key must be immutable (`order_ts` rule), partition bookkeeping |
| Handles late / out-of-order data | trivially | yes, via the guard in the UPSERT | yes, automatically (partition is recomputed from history) |
| Handles deletes / corrected history | yes | no (needs tombstones) | yes within partitions |
| Failure behaviour | atomic: old table keeps serving until swap | atomic: data + watermark in one transaction | **non-atomic across partitions** by default: partial state is visible |
| Recovery | re-run | re-run | re-run (idempotent) or resume; `--atomic-partitions` trades this away |
| Backfill | same command | reset watermark | `--partitions d1,d2` or reset watermark |

## Measured (100,000 orders, 269,072 events, 38 daily batches, SQLite, single machine; `output/benchmark_report.md`)
| strategy | total publish | median batch | rows written (total) |
|---|---:|---:|---:|
| full_refresh | 34.0 s | 889 ms (grows from ~15 ms to ~1.5 s as history grows) | 2,331,536 |
| incremental_merge | 2.7 s | 81 ms (flat) | 216,544 |
| partition_replace | 14.2 s | 414 ms | 854,423 |

Full refresh grows with history; incremental stays proportional to the daily delta; partition replace sits
between because late events dirty older partitions (here ~7 of the 30 date partitions per batch on average) and each is rewritten in full.
All three targets were reconciled against each other and an independent oracle after every run.
Absolute numbers depend on hardware; the shape (linear vs flat) is the finding.

## Key design decisions
1. **Shared landed layer.** Ingestion validates and appends to `raw_events`; strategies only differ in
   *publish*. This isolates the comparison, preserves event history and makes every strategy replayable.
2. **Watermark = ingestion sequence (`seq`), not `updated_at`.** Late data has an old `updated_at` but a new
   `seq`, so it is never skipped. Out-of-order protection is the `(updated_at, event_id)` guard.
3. **Idempotency at three levels:** file (sha256), event (unique `event_id`), publish (deterministic
   "latest version" rule + conditional UPSERT / replace semantics). Re-running anything converges.
4. **Atomicity.** Every file ingest and every publish is one transaction (SQLite DDL is transactional, so the
   full-refresh swap is too). In warehouses without transactional DDL, full refresh would use a shadow table
   plus an atomic rename/view switch and clean up a leftover shadow table on retry.
5. **Rejects are data, not exceptions:** row-level problems are quarantined with file + line + original
   record; file-level problems (schema change) fail the file without touching data.
6. **`order_ts` immutable** (reject `order_ts_changed`; first seen wins). Required for partition replacement to be
   equivalent to full refresh: an order can never migrate between partitions. Without this rule partition
   replacement would leave a stale copy in the old partition.
7. **Failure injection** (`--fail-after N`, `fail_after_rows`) makes recovery behaviour testable and demonstrable.

## Optimization implemented and measured
Composite index `raw_events(order_date, seq)`: lets a partition rebuild read only its own rows instead of
scanning all history: 1.8x faster in the micro-benchmark (5 partitions) and the difference grows with history.
Other reasonable next steps: batch the partition rewrites, vacuum/ANALYZE after big loads, move to columnar
storage with native partition overwrite.

## Known limitations
- SQLite, single writer; no concurrency control between simultaneous pipeline runs.
- No deletes/tombstones in the source; "cancelled" is a status, not a deletion.
- `order_ts_changed` rejects use first-seen-wins, which depends on ingestion order.
- Conflicting events with identical `updated_at` are resolved by `event_id` (deterministic but arbitrary).
- Currency amounts are not converted; sums mix currencies (documented; the reconciliation compares like with like).
- Benchmarks are single-run wall-clock numbers, not statistically repeated, except the index micro-test (best of 5).

## When to use which
- Small/medium tables, need simplicity or corrections to old data → **full refresh**.
- Large tables, mostly inserts/updates with a reliable key and ordering column → **incremental merge**.
- Large, partitioned fact tables where late data lands in a bounded set of partitions and you want
  self-healing recomputation (also deletes) → **partition replacement**.
