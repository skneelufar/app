"""Performance measurement: simulate N daily batches, run all three strategies after each batch
and time ONLY the publish step. Then measure one optimization (partition index)."""
import json
import shutil
import statistics
import tempfile
import time
from pathlib import Path

from .config import STRATEGIES
from .db import connect
from .generator import generate
from .ingest import ingest_file, load_customers
from .reconcile import reconcile, to_markdown
from .strategies import latest_select_sql, publish


def _optimization_experiment(conn, repeats=5):
    """Partition rebuild = read one order_date out of raw_events. Compare with/without the
    (order_date, seq) index."""
    dates = [r[0] for r in conn.execute("SELECT DISTINCT order_date FROM raw_events ORDER BY order_date")]
    dates = dates[-5:]
    sql = f"SELECT COUNT(*) FROM ({latest_select_sql('order_date = :d AND seq <= :max')})"
    max_seq = conn.execute("SELECT MAX(seq) FROM raw_events").fetchone()[0]

    def timed():
        best = float("inf")
        for _ in range(repeats):
            t0 = time.perf_counter()
            for d in dates:
                conn.execute(sql, {"d": d, "max": max_seq, "run_id": "x"}).fetchone()
            best = min(best, time.perf_counter() - t0)
        return best * 1000.0

    with_idx = timed()
    conn.execute("DROP INDEX idx_raw_events_order_date_seq")
    without = timed()
    conn.execute("CREATE INDEX idx_raw_events_order_date_seq ON raw_events(order_date, seq)")
    return {"partitions_read": len(dates), "with_index_ms": with_idx, "without_index_ms": without,
            "speedup_x": without / with_idx if with_idx else None}


def run_benchmark(orders=100_000, days=30, seed=42, out_dir="output", workdir=None) -> dict:
    tmp = Path(workdir or tempfile.mkdtemp(prefix="loadlab_bench_"))
    data = tmp / "data"
    t0 = time.perf_counter()
    gen = generate(data, n_orders=orders, n_customers=max(100, orders // 50), days=days, seed=seed)
    gen_s = time.perf_counter() - t0
    conn = connect(tmp / "bench.db")
    load_customers(conn, data / "customers.csv")

    per_batch, ingest_ms = [], []
    for f in sorted((data / "batches").glob("*.csv")):
        r = ingest_file(conn, f)
        assert r["status"] == "SUCCESS", r
        ingest_ms.append(conn.execute("SELECT duration_ms FROM pipeline_runs WHERE run_id=?",
                                      (r["run_id"],)).fetchone()[0])
        row = {"batch": f.name, "events_in_batch": r["rows_in"], "rejected": r["rows_rejected"]}
        for s in STRATEGIES:
            p = publish(conn, s)
            assert p["status"] == "SUCCESS", p
            row[s] = {"ms": p["duration_ms"], "rows_in": p["rows_in"], "rows_written": p["rows_written"],
                      "rows_deleted": p["rows_deleted"], "target_rows": p["rows_out"],
                      "partitions_touched": p["partitions_touched"]}
        per_batch.append(row)

    strategies = {}
    for s in STRATEGIES:
        ms = [b[s]["ms"] for b in per_batch]
        strategies[s] = {
            "total_s": sum(ms) / 1000, "mean_ms": statistics.mean(ms), "median_ms": statistics.median(ms),
            "last_batch_ms": ms[-1], "max_ms": max(ms),
            "rows_read_total": sum(b[s]["rows_in"] for b in per_batch),
            "rows_written_total": sum(b[s]["rows_written"] for b in per_batch),
            "rows_deleted_total": sum(b[s]["rows_deleted"] for b in per_batch),
        }
    opt = _optimization_experiment(conn)
    rec = reconcile(conn, labels_path=data / "labels" / "expected_rejects.csv")
    result = {
        "config": {"orders": orders, "days": days, "seed": seed, "batches": len(per_batch)},
        "dataset": gen, "generate_s": gen_s, "ingest_total_s": sum(ingest_ms) / 1000,
        "target_rows_final": per_batch[-1]["full_refresh"]["target_rows"],
        "strategies": strategies, "per_batch": per_batch, "optimization": opt,
        "reconciliation_passed": rec["passed"],
    }
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "benchmark_results.json").write_text(json.dumps(result, indent=2, sort_keys=True))
    (out / "benchmark_report.md").write_text(_markdown(result))
    (out / "benchmark_reconciliation.md").write_text(to_markdown(rec))
    conn.close()
    if workdir is None:
        shutil.rmtree(tmp, ignore_errors=True)
    return result


def _markdown(r: dict) -> str:
    c, o = r["config"], r["optimization"]
    L = ["# Benchmark report", "",
         f"Dataset: {c['orders']:,} orders -> {r['dataset']['valid_events']:,} valid events "
         f"+ {r['dataset']['labelled_rejects']:,} labelled bad/duplicate rows, "
         f"{c['batches']} daily batch files (seed {c['seed']}). Final target size: "
         f"{r['target_rows_final']:,} rows. All three targets reconciled: "
         f"**{'PASS' if r['reconciliation_passed'] else 'FAIL'}**.", "",
         "Each batch is ingested once; then each strategy publishes it. Only the publish step is timed.", "",
         "| strategy | total publish (s) | median per batch (ms) | mean per batch (ms) | last (tiny) batch (ms) | rows read | rows written | rows deleted |",
         "|----------|------------------:|----------------------:|--------------------:|-----------------------:|----------:|-------------:|-------------:|"]
    for s, m in r["strategies"].items():
        L.append(f"| {s} | {m['total_s']:.2f} | {m['median_ms']:.0f} | {m['mean_ms']:.0f} | {m['last_batch_ms']:.1f} | "
                 f"{m['rows_read_total']:,} | {m['rows_written_total']:,} | {m['rows_deleted_total']:,} |")
    L += ["", f"Ingestion of all batches: {r['ingest_total_s']:.2f} s. Generation: {r['generate_s']:.2f} s.", "",
          "## Optimization: composite index on raw_events(order_date, seq)", "",
          f"Reading {o['partitions_read']} partitions out of the landed history: "
          f"{o['with_index_ms']:.1f} ms with the index vs {o['without_index_ms']:.1f} ms without "
          f"(**{o['speedup_x']:.1f}x**). Partition replacement and the late-data path depend on it.", "",
          "## Per-batch publish time (ms)", "",
          "| batch | events | full_refresh | incremental_merge | partition_replace | partitions rewritten (partition_replace) |",
          "|-------|-------:|-------------:|------------------:|------------------:|-----------------------------------------:|"]
    for b in r["per_batch"]:
        L.append(f"| {b['batch']} | {b['events_in_batch']} | {b['full_refresh']['ms']:.0f} | "
                 f"{b['incremental_merge']['ms']:.0f} | {b['partition_replace']['ms']:.0f} | "
                 f"{b['partition_replace']['partitions_touched']} |")
    return "\n".join(L) + "\n"
