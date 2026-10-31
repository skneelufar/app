# Load-strategy lab: full refresh vs incremental merge vs partition replacement

Round-2 data-engineering challenge. One dataset (e-commerce order **events**), one shared validated landing
layer, **three publish strategies** writing three identical target tables, and evidence that they produce
equivalent results, with measured runtime and documented complexity and failure-recovery trade-offs.

* **Zero dependencies**: Python standard library (developed and tested on 3.12; uses nothing newer than 3.9) + SQLite (>= 3.25). `pytest` is optional.
* Everything is deterministic (seeded generator, deterministic IDs) and runs locally in seconds.
* Design/trade-off note: [`docs/DESIGN_TRADEOFFS.md`](docs/DESIGN_TRADEOFFS.md) · schemas: [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md)

## Quick start (clean environment)
```bash
git clone <your-repo-url> && cd <repo>
python3 --version                      # 3.9+
python3 -m unittest discover -s tests  # 44 tests, ~1 s   (or: pip install pytest && pytest)
python3 -m loadlab demo --fresh        # sample data -> ingest -> 3 strategies -> reconciliation (exit 0 = all checks pass)
python3 -m loadlab report --db output/demo.db --strategy partition_replace   # sample analytical queries
bash scripts/failure_demo.sh           # schema change, empty batch, crash mid-publish, retry, rerun
python3 -m loadlab benchmark --orders 100000 --days 30 --out output          # ~1 min; 100k orders / 269k events
```
`make test | demo | failure-demo | benchmark | all` are shortcuts for the same commands.

## Architecture
```
 customers.csv ──► load-customers ──► dim_customers ─────────────┐ (reference check)
                                                                  ▼
 orders_<date>.csv ─► ingest ─► [schema check] ─► [row validation] ─► raw_events   (append-only history,
 (daily, late/dup/bad   one txn   missing column      PK/ref/rules        │          run_id, source_file, line_no)
  rows, shuffled)       per file  => file FAILED      dedupe by event_id  │
                                                          │               │ watermark = seq
                                                          ▼               ▼
                                                      rejects      ┌──────────────┬────────────────────┬──────────────────┐
                                                   (quarantine:    │ full_refresh │ incremental_merge  │ partition_replace│
                                                  file+line+reason)│ rebuild+swap │ conditional UPSERT │ delete+insert    │
                                                                   └──────┬───────┴─────────┬──────────┴────────┬─────────┘
                                                                          ▼                 ▼                   ▼
                                                              orders_full_refresh  orders_incremental_merge  orders_partition_replace
                                                                          └──────────── reconcile ─────────────┘
                                                      pipeline_runs (run_id, times, counts, errors)    oracle + cross-checks + answer key
```
Code map: `generator.py` (data + answer key) · `validate.py` (pure rules) · `ingest.py` · `strategies.py` ·
`reconcile.py` · `benchmark.py` · `cli.py` · `db.py` (schema) · `runlog.py` (run tracking).

## Commands
| command | what it does |
|---|---|
| `generate --out data/sample --orders 200 --days 5 --seed 42 [--edge-cases DIR]` | synthetic inputs + `labels/expected_rejects.csv` (answer key, separate from inputs) |
| `load-customers --db D --file F` | idempotent dimension load |
| `ingest --db D --path FILE_OR_DIR` | validate + land; exit 1 if any file FAILED |
| `publish --db D --strategy {full_refresh,incremental_merge,partition_replace,all}` | publish landed data. Options: `--fail-after N` (crash injection), `--atomic-partitions`, `--partitions 2026-01-02,...` (backfill), `--reset-watermark` (replay) |
| `reconcile --db D [--labels FILE] --out output` | checks + `reconciliation_report.{md,json}`; exit 1 on any failure |
| `status --db D` | recent runs, errors, watermarks |
| `report --db D --strategy S` | sample analytical queries (daily revenue, status mix, top customers) |
| `demo` / `benchmark` | end-to-end run / timed comparison |

Larger data: `python3 -m loadlab generate --out data/large --orders 500000 --customers 5000 --days 60`, then
`ingest`/`publish` on it.

## Business rules and assumptions
* Grain: one source row = one **event** of an order; target = **latest version per order**
  (highest `updated_at`, ties → highest `event_id`; independent of arrival order).
* `order_date` (UTC date of `order_ts`) is the partition key; `order_ts` is **immutable** per order
  (changes are rejected as `order_ts_changed`) so partitions are stable.
* Derived: `amount_cents` (integer), `net_amount_cents` (= amount only for `paid`/`shipped`).
* Validation: required fields, formats, enums, amount range 0.00–1,000,000.00 (both boundaries valid),
  `updated_at >= order_ts`, customer must exist, duplicates dropped, malformed rows quarantined. Full list in the data dictionary.
* Naive timestamps are treated as UTC. Currencies are not converted.

## Idempotency, retry, restart, backfill
* **Rerun same file:** SHA-256 registry → `SKIPPED`; even a re-named/modified copy only adds new events (unique `event_id`).
* **Ingest failure** (crash, bad encoding, schema change): the file's transaction rolls back, nothing lands, run is `FAILED` with the reason, file can be re-delivered/re-run.
* **Publish failure:** `full_refresh` and `incremental_merge` are atomic (old table / old watermark remain). `partition_replace` commits per partition (so a crash leaves a mix of old and new partitions), the watermark advances only at the end, and **restart = run it again** (idempotent). `--atomic-partitions` makes it all-or-nothing.
* **Backfill/replay:** `publish --reset-watermark` (incremental/partition), `publish --partitions d1,d2` (repair specific partitions), or just `full_refresh`.

## Evidence (committed in `output/`)
| file | content |
|---|---|
| `output/test_results.txt` | all 44 tests passing |
| `output/reconciliation_report.md` | sample run: 21 checks pass (oracle equality, PK, RI, source→target counts, cross-strategy equivalence, answer-key match) |
| `output/failure_recovery_demo.txt` | schema change, empty/zero-byte/malformed files, 3 injected crashes, retry, rerun |
| `output/benchmark_report.md` / `.json` | 100k orders / 269k events / 38 batches, per-batch timings, index optimization |
| `output/sample_report.txt` | sample analytical queries |

### Headline benchmark result (details in the design note)
| strategy | total publish time | median per daily batch | note |
|---|---:|---:|---|
| full_refresh | 34.0 s | 889 ms | grows with history (15 ms → 1.5 s) |
| incremental_merge | 2.7 s | 81 ms | flat, proportional to the delta |
| partition_replace | 14.2 s | 414 ms | proportional to touched partitions |

Optimization: composite index `raw_events(order_date, seq)` → 1.8x faster partition reads in the micro-test.

## Testing (44 tests)
Happy path · empty batch (header-only and zero-byte) · malformed rows · missing required fields · nulls ·
duplicates (in-file, cross-file, re-run, renamed copy) · schema change (missing / extra / reordered columns) ·
invalid reference key · boundary values · late-arriving and out-of-order events · tie-break determinism ·
partial failure + retry for ingest and all 3 strategies · idempotent reruns · backfill · tamper detection by reconcile ·
seeded-generator reproducibility · generated bad rows match the answer key exactly.

## Known limitations / next steps
SQLite single-writer (no concurrent-run locking); no delete/tombstone events; first-seen-wins for `order_ts`;
single-run wall-clock benchmarks; no currency conversion. Next: orchestration (Airflow/cron) with run locks,
Parquet + native partition overwrite (Spark/Delta/Iceberg), CDC deletes, data-quality thresholds that fail a run.
