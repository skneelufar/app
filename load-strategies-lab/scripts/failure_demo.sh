#!/usr/bin/env bash
# Demonstrates: schema change, empty batch, malformed rows, crash mid-publish, retry, rerun.
# Usage: bash scripts/failure_demo.sh        (needs only python3)
set -u
PY="python3 -m loadlab --log-level ERROR"
DB=output/failure_demo.db
rm -f $DB $DB-wal $DB-shm
$PY generate --out data/sample --edge-cases data/edge_cases >/dev/null

echo "== 1. load customers, ingest first two daily batches, publish all strategies"
$PY load-customers --db $DB --file data/sample/customers.csv | grep -E '"status"|rows_out'
for f in data/sample/batches/orders_2026-01-01.csv data/sample/batches/orders_2026-01-02.csv; do
  $PY ingest --db $DB --path "$f" | grep -E '"status"|rows_(in|out|rejected)'
  $PY publish --db $DB --strategy all > /dev/null
done

echo; echo "== 2. edge-case files (empty, zero-byte, missing column, extra column, malformed)"
$PY ingest --db $DB --path data/edge_cases | grep -E 'source_file|"status"|"error"|rows_(out|rejected)'
echo "(the schema-change file FAILED as a whole and landed nothing; every other file was unaffected)"

echo; echo "== 3. re-run the same input: ingest is SKIPPED, publish is a no-op (idempotent)"
$PY ingest --db $DB --path data/sample/batches/orders_2026-01-01.csv | grep '"status"'

echo; echo "== 4. ingest the remaining batches, then crash each strategy mid-publish"
$PY ingest --db $DB --path data/sample/batches > /dev/null
$PY publish --db $DB --strategy full_refresh      --fail-after 1 | grep -E '"status"|"error"'
$PY publish --db $DB --strategy incremental_merge --fail-after 1 | grep -E '"status"|"error"'
$PY publish --db $DB --strategy partition_replace --fail-after 2 | grep -E '"status"|"error"'
echo "-- state after crash: full_refresh + incremental_merge are untouched (atomic); partition_replace is"
echo "   partially applied (2 of 3+ partitions already rewritten, watermark not advanced) and reconcile flags it:"
$PY reconcile --db $DB --out output/failure_demo 2>/dev/null | grep -E 'Overall|equals independent oracle|equivalence: full_refresh == (incr|part)' | head -12

echo; echo "== 5. retry (restart = run the same command again)"
$PY publish --db $DB --strategy all | grep -E '"strategy"|"status"'
$PY reconcile --db $DB --labels data/sample/labels/expected_rejects.csv --out output/failure_demo | grep -E 'Overall'

echo; echo "== 6. run history (failures are recorded with reasons)"
$PY status --db $DB --limit 40 | python3 -c "
import json,sys
txt=sys.stdin.read(); runs=json.JSONDecoder().raw_decode(txt)[0]
for r in runs:
    if r['status'] in ('FAILED','SKIPPED'):
        print(r['stage'], r['strategy'] or r['source_file'], r['status'], '|', (r['error'] or '')[:90])"
