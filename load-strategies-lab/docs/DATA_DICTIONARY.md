# Data dictionary

Naming: `snake_case` tables/columns; timestamps are ISO-8601 UTC (`YYYY-MM-DDTHH:MM:SSZ`);
dates are `YYYY-MM-DD`; money is stored as integer minor units (`*_cents`) to avoid float error.
IDs are deterministic: `order_id` comes from the source (`ord_0000042`), `event_id` is the SHA-1 of the
normalized event payload (same input → same ID), `run_id` is `<stage>-<UTC timestamp>-<random8>`.

## Source file: `orders_<arrival_date>.csv` (one file per daily delivery)
| column | type | rules |
|---|---|---|
| order_id | text | required, `[A-Za-z0-9_-]{1,64}` |
| customer_id | text | required, must exist in `dim_customers` (referential integrity) |
| order_ts | ISO-8601 | required, immutable per order (a changed value is rejected `order_ts_changed`) |
| status | text | required, one of `created, paid, shipped, cancelled, refunded` (case-insensitive) |
| amount | decimal | required, `0.00`–`1000000.00`, max 2 decimals, no thousands separators |
| currency | text | required, `USD, EUR, INR` |
| updated_at | ISO-8601 | required, `>= order_ts`; version clock of the event |

Grain: **one row = one event (version) of an order**. Many events per order. Extra/reordered columns
are tolerated (warning); a missing required column fails the whole file.
Naive timestamps are interpreted as UTC. Source file `customers.csv`: `customer_id, region, signup_date`.

## Tables
| table | grain / key | purpose |
|---|---|---|
| `dim_customers` | PK `customer_id` | reference data, idempotent upsert |
| `raw_events` | PK `seq`, UNIQUE `event_id` | append-only history of every valid, de-duplicated event with `run_id`, `source_file`, `line_no`. Never updated → history preserved |
| `rejects` | `reject_id` | quarantine: `run_id, source_file, line_no, reason, raw_record (JSON array), rejected_at` |
| `ingested_files` | PK `file_sha256` | file-level idempotency |
| `publish_state` | PK `strategy` | per-strategy watermark (`last_seq` = highest `raw_events.seq` already published) |
| `pipeline_runs` | PK `run_id` | operational log: stage, strategy, status, start/end, duration, rows in/out/rejected/written/deleted, partitions, error |
| `orders_full_refresh`, `orders_incremental_merge`, `orders_partition_replace` | PK `order_id` | **identical** target schema, one per strategy |

## Target schema (`orders_*`): one row per order = latest version
| column | type | note |
|---|---|---|
| order_id, customer_id, order_ts, status, currency | text | from the winning event |
| order_date | date text | **partition key**, UTC date of `order_ts` |
| amount_cents | integer ≥ 0 | |
| net_amount_cents | integer | derived: `amount_cents` if status ∈ (`paid`,`shipped`) else `0` |
| updated_at | ISO-8601 | of the winning event |
| event_id | text | lineage back to `raw_events` |
| loaded_run_id | text | run that last wrote the row (excluded from equivalence checks) |

**Winning event rule (all strategies):** highest `updated_at`; ties broken by highest `event_id`
(deterministic regardless of arrival order).

## Reject reasons
`malformed_row`, `missing_<column>` (×7), `invalid_order_id`, `invalid_order_ts`, `invalid_updated_at`,
`invalid_status`, `invalid_amount`, `negative_amount`, `amount_out_of_range`, `invalid_currency`,
`updated_before_order_ts`, `unknown_customer`, `order_ts_changed`, `duplicate_event`.
File-level failures (`SchemaError`, undecodable file) are recorded in `pipeline_runs` with `status=FAILED`.
