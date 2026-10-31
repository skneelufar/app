"""Command line interface:  python -m loadlab <command> --help"""
import argparse
import json
import logging
import sys
from pathlib import Path

from .benchmark import run_benchmark
from .config import STRATEGIES, TARGET_TABLES
from .db import connect
from .generator import generate, write_edge_cases
from .ingest import ingest_path, load_customers
from .reconcile import reconcile, to_markdown, write_report
from .strategies import publish, reset_watermark


def _print(obj):
    print(json.dumps(obj, indent=2, sort_keys=True, default=str))


def cmd_generate(a):
    _print(generate(a.out, a.orders, a.customers, a.days, a.start, a.seed))
    if a.edge_cases:
        _print({"edge_case_files": write_edge_cases(a.edge_cases)})
    return 0


def cmd_load_customers(a):
    r = load_customers(connect(a.db), a.file)
    _print(r)
    return 0 if r["status"] == "SUCCESS" else 1


def cmd_ingest(a):
    results = ingest_path(connect(a.db), a.path)
    _print(results)
    return 1 if any(r["status"] == "FAILED" for r in results) else 0


def cmd_publish(a):
    conn = connect(a.db)
    strategies = STRATEGIES if a.strategy == "all" else (a.strategy,)
    parts = a.partitions.split(",") if a.partitions else None
    results = []
    for s in strategies:
        if a.reset_watermark:
            reset_watermark(conn, s)
        results.append(publish(conn, s, fail_after=a.fail_after,
                               atomic_partitions=a.atomic_partitions, partitions=parts))
    _print(results)
    return 1 if any(r["status"] == "FAILED" for r in results) else 0


def cmd_reconcile(a):
    res = reconcile(connect(a.db), labels_path=a.labels)
    write_report(res, a.out)
    print(to_markdown(res))
    return 0 if res["passed"] else 1


def cmd_status(a):
    conn = connect(a.db)
    rows = conn.execute("""SELECT run_id, stage, strategy, source_file, status, rows_in, rows_out,
        rows_rejected, rows_written, rows_deleted, partitions_touched, round(duration_ms,1) AS ms, error
        FROM pipeline_runs ORDER BY started_at DESC, rowid DESC LIMIT ?""", (a.limit,))
    _print([dict(r) for r in rows])
    _print({"watermarks": {r[0]: r[1] for r in conn.execute("SELECT strategy, last_seq FROM publish_state")}})
    return 0


def cmd_report(a):
    conn = connect(a.db)
    t = TARGET_TABLES[a.strategy]
    print(f"-- Daily orders and net revenue ({t})")
    for r in conn.execute(f"""SELECT order_date, COUNT(*) AS orders, SUM(amount_cents)/100.0 AS gross,
            SUM(net_amount_cents)/100.0 AS net FROM {t} GROUP BY order_date ORDER BY order_date"""):
        print(f"{r[0]}  orders={r[1]:>5}  gross={r[2]:>14,.2f}  net={r[3]:>14,.2f}")
    print(f"\n-- Order status mix ({t})")
    for r in conn.execute(f"SELECT status, COUNT(*) FROM {t} GROUP BY status ORDER BY 2 DESC"):
        print(f"{r[0]:<10} {r[1]}")
    print(f"\n-- Top 5 customers by net revenue, with region ({t})")
    for r in conn.execute(f"""SELECT o.customer_id, c.region, SUM(o.net_amount_cents)/100.0 AS net
            FROM {t} o JOIN dim_customers c USING (customer_id)
            GROUP BY 1, 2 ORDER BY net DESC LIMIT 5"""):
        print(f"{r[0]}  {r[1]}  net={r[2]:,.2f}")
    return 0


def cmd_benchmark(a):
    r = run_benchmark(a.orders, a.days, a.seed, a.out)
    print(Path(a.out, "benchmark_report.md").read_text())
    return 0 if r["reconciliation_passed"] else 1


def cmd_demo(a):
    """End to end: customers -> for each daily batch: ingest + publish all strategies -> reconcile."""
    db = Path(a.db)
    if a.fresh:
        for suffix in ("", "-wal", "-shm"):
            Path(str(db) + suffix).unlink(missing_ok=True)
    conn = connect(db)
    data = Path(a.data)
    load_customers(conn, data / "customers.csv")
    failed = False
    for res in ingest_each(conn, data / "batches"):
        failed |= res["status"] == "FAILED"
    res = reconcile(conn, labels_path=data / "labels" / "expected_rejects.csv")
    write_report(res, a.out)
    print(to_markdown(res))
    return 0 if (res["passed"] and not failed) else 1


def ingest_each(conn, batches_dir):
    """Daily-run simulation: ingest one file, then publish every strategy, repeat."""
    for f in sorted(Path(batches_dir).glob("*.csv")):
        r = ingest_path(conn, f)[0]
        pubs = [publish(conn, s) for s in STRATEGIES]
        yield {"status": "FAILED" if r["status"] == "FAILED" or any(p["status"] == "FAILED" for p in pubs)
               else "SUCCESS", "ingest": r, "publish": pubs}


def build_parser():
    p = argparse.ArgumentParser(prog="loadlab", description=__doc__)
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="generate synthetic customers + daily order batches + labels")
    g.add_argument("--out", default="data/sample")
    g.add_argument("--orders", type=int, default=200)
    g.add_argument("--customers", type=int, default=20)
    g.add_argument("--days", type=int, default=5)
    g.add_argument("--start", default="2026-01-01")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--edge-cases", metavar="DIR", help="also write small edge-case files to DIR")
    g.set_defaults(fn=cmd_generate)

    c = sub.add_parser("load-customers", help="load/refresh the customer dimension")
    c.add_argument("--db", default="output/loadlab.db"); c.add_argument("--file", required=True)
    c.set_defaults(fn=cmd_load_customers)

    i = sub.add_parser("ingest", help="validate + land a CSV file or every CSV in a directory")
    i.add_argument("--db", default="output/loadlab.db"); i.add_argument("--path", required=True)
    i.set_defaults(fn=cmd_ingest)

    b = sub.add_parser("publish", help="publish landed data to a target with a strategy")
    b.add_argument("--db", default="output/loadlab.db")
    b.add_argument("--strategy", choices=list(STRATEGIES) + ["all"], default="all")
    b.add_argument("--fail-after", type=int, help="inject a crash at the N-th unit of work")
    b.add_argument("--atomic-partitions", action="store_true",
                   help="partition_replace: one transaction for all partitions")
    b.add_argument("--partitions", help="partition_replace backfill: comma list of YYYY-MM-DD")
    b.add_argument("--reset-watermark", action="store_true", help="reprocess all history (backfill)")
    b.set_defaults(fn=cmd_publish)

    r = sub.add_parser("reconcile", help="run reconciliation checks and write the report")
    r.add_argument("--db", default="output/loadlab.db"); r.add_argument("--labels")
    r.add_argument("--out", default="output")
    r.set_defaults(fn=cmd_reconcile)

    s = sub.add_parser("status", help="recent runs and watermarks")
    s.add_argument("--db", default="output/loadlab.db"); s.add_argument("--limit", type=int, default=15)
    s.set_defaults(fn=cmd_status)

    rp = sub.add_parser("report", help="sample analytical queries on a target")
    rp.add_argument("--db", default="output/loadlab.db")
    rp.add_argument("--strategy", choices=STRATEGIES, default="full_refresh")
    rp.set_defaults(fn=cmd_report)

    bm = sub.add_parser("benchmark", help="timed comparison on a larger generated dataset")
    bm.add_argument("--orders", type=int, default=100_000)
    bm.add_argument("--days", type=int, default=30)
    bm.add_argument("--seed", type=int, default=42)
    bm.add_argument("--out", default="output")
    bm.set_defaults(fn=cmd_benchmark)

    d = sub.add_parser("demo", help="customers -> daily ingest+publish (all strategies) -> reconcile")
    d.add_argument("--data", default="data/sample"); d.add_argument("--db", default="output/demo.db")
    d.add_argument("--out", default="output"); d.add_argument("--fresh", action="store_true")
    d.set_defaults(fn=cmd_demo)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    return args.fn(args)
