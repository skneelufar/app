"""Reconciliation / validation report: proves the three strategies agree with each other and with
an independent Python oracle, and that counts and rejects add up."""
import csv
import json
from pathlib import Path

from .config import BUSINESS_COLUMNS, REVENUE_STATUSES, STRATEGIES, TARGET_TABLES
from .strategies import get_watermark


def oracle_state(conn, upto_seq):
    """Independent (pure Python) computation of the expected target from landed history."""
    best = {}
    for r in conn.execute("SELECT * FROM raw_events WHERE seq <= ? ORDER BY seq", (upto_seq,)):
        key = (r["updated_at"], r["event_id"])
        cur = best.get(r["order_id"])
        if cur is None or key > cur[0]:
            best[r["order_id"]] = (key, r)
    out = {}
    for oid, (_, r) in best.items():
        net = r["amount_cents"] if r["status"] in REVENUE_STATUSES else 0
        out[oid] = (r["order_id"], r["customer_id"], r["order_ts"], r["order_date"], r["status"],
                    r["amount_cents"], net, r["currency"], r["updated_at"], r["event_id"])
    return out


def _target_state(conn, table):
    cols = ", ".join(BUSINESS_COLUMNS)
    return {r[0]: tuple(r) for r in conn.execute(f"SELECT {cols} FROM {table}")}


def _check(name, passed, detail):
    return {"name": name, "passed": bool(passed), "detail": detail}


def reconcile(conn, labels_path=None) -> dict:
    checks = []

    # 1. ingestion arithmetic -------------------------------------------------
    ing = conn.execute("""SELECT COALESCE(SUM(rows_in),0), COALESCE(SUM(rows_out),0),
                                 COALESCE(SUM(rows_rejected),0)
                          FROM pipeline_runs WHERE stage IN ('ingest','load_customers')
                          AND status='SUCCESS'""").fetchone()
    cust_out = conn.execute("SELECT COALESCE(SUM(rows_out),0) FROM pipeline_runs "
                            "WHERE stage='load_customers' AND status='SUCCESS'").fetchone()[0]
    rows_in, rows_out, rows_rej = ing
    checks.append(_check("ingest: rows_in == accepted + rejected", rows_in == rows_out + rows_rej,
                         f"rows_in={rows_in} accepted={rows_out} rejected={rows_rej}"))
    raw_n = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    rej_n = conn.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
    checks.append(_check("ingest: raw_events rows == accepted event rows", raw_n == rows_out - cust_out,
                         f"raw_events={raw_n} accepted_events={rows_out - cust_out}"))
    checks.append(_check("ingest: rejects table rows == rejected counted in runs", rej_n == rows_rej,
                         f"rejects={rej_n} runs_rejected={rows_rej}"))
    orphan = conn.execute("""SELECT COUNT(*) FROM rejects r LEFT JOIN pipeline_runs p ON p.run_id=r.run_id
                             WHERE p.run_id IS NULL OR r.source_file='' OR r.line_no < 1""").fetchone()[0]
    orphan_raw = conn.execute("""SELECT COUNT(*) FROM raw_events r LEFT JOIN pipeline_runs p
                                 ON p.run_id=r.run_id WHERE p.run_id IS NULL""").fetchone()[0]
    checks.append(_check("traceability: rejects and raw rows map to a run, file and line",
                         orphan == 0 and orphan_raw == 0, f"untraceable rejects={orphan} raw={orphan_raw}"))
    by_reason = {r[0]: r[1] for r in conn.execute(
        "SELECT reason, COUNT(*) FROM rejects GROUP BY reason ORDER BY reason")}

    # 2. source -> target explanation ------------------------------------------
    distinct_orders = conn.execute("SELECT COUNT(DISTINCT order_id) FROM raw_events").fetchone()[0]
    superseded = raw_n - distinct_orders

    # 3. per-strategy checks ----------------------------------------------------
    summary, states, aggs = {}, {}, {}
    for s in STRATEGIES:
        t = TARGET_TABLES[s]
        wm = get_watermark(conn, s)
        states[s] = _target_state(conn, t)
        expected = oracle_state(conn, wm)
        missing = len(expected.keys() - states[s].keys())
        extra = len(states[s].keys() - expected.keys())
        diff = sum(1 for k in expected.keys() & states[s].keys() if expected[k] != states[s][k])
        checks.append(_check(f"{s}: equals independent oracle at watermark {wm}",
                             missing == extra == diff == 0,
                             f"rows={len(states[s])} expected={len(expected)} missing={missing} "
                             f"extra={extra} mismatched={diff}"))
        n, d = conn.execute(f"SELECT COUNT(*), COUNT(DISTINCT order_id) FROM {t}").fetchone()
        checks.append(_check(f"{s}: primary key unique", n == d, f"rows={n} distinct_order_id={d}"))
        ri = conn.execute(f"""SELECT COUNT(*) FROM {t} o LEFT JOIN dim_customers c
                              ON c.customer_id=o.customer_id WHERE c.customer_id IS NULL""").fetchone()[0]
        checks.append(_check(f"{s}: referential integrity (customer_id in dim_customers)", ri == 0,
                             f"orphan_rows={ri}"))
        aggs[s] = {r[0]: tuple(r[1:]) for r in conn.execute(
            f"""SELECT order_date, COUNT(*), SUM(amount_cents), SUM(net_amount_cents) FROM {t}
                GROUP BY order_date ORDER BY order_date""")}
        tot = conn.execute(f"SELECT COUNT(*), COALESCE(SUM(amount_cents),0), COALESCE(SUM(net_amount_cents),0)"
                           f" FROM {t}").fetchone()
        summary[s] = {"watermark": wm, "rows": tot[0], "sum_amount_cents": tot[1], "sum_net_amount_cents": tot[2]}
        if wm == conn.execute("SELECT COALESCE(MAX(seq),0) FROM raw_events").fetchone()[0]:
            checks.append(_check(f"{s}: source-to-target count explained", n == distinct_orders,
                                 f"raw_events={raw_n} - superseded_versions={superseded} "
                                 f"= distinct_orders={distinct_orders}; target={n}"))

    # 4. cross-strategy equivalence ----------------------------------------------
    up_to_date = {s for s in STRATEGIES if summary[s]["watermark"] ==
                  conn.execute("SELECT COALESCE(MAX(seq),0) FROM raw_events").fetchone()[0]}
    base = STRATEGIES[0]
    for s in STRATEGIES[1:]:
        if base in up_to_date and s in up_to_date:
            eq = states[base] == states[s]
            checks.append(_check(f"equivalence: {base} == {s} (all rows, all business columns)", eq,
                                 f"rows {len(states[base])} vs {len(states[s])}"))
            eqa = aggs[base] == aggs[s]
            checks.append(_check(f"equivalence: per-date count/amount/net aggregates {base} == {s}", eqa,
                                 f"{len(aggs[base])} partitions compared"))

    # 5. labelled invalid records ---------------------------------------------------
    label_info = None
    if labels_path and Path(labels_path).exists():
        # Only files the answer key covers (the generated batch files) are compared; other
        # inputs such as hand-made edge-case files have no labels.
        ingested = {r[0] for r in conn.execute("SELECT source_file FROM ingested_files")}
        with open(labels_path, newline="") as f:
            rows = list(csv.DictReader(f))
        covered = {r["source_file"] for r in rows}
        batches = Path(labels_path).parent.parent / "batches"
        if batches.is_dir():
            covered |= {p.name for p in batches.glob("*.csv")}
        covered &= ingested
        expected = {(r["source_file"], int(r["line_no"])): r["expected_reason"]
                    for r in rows if r["source_file"] in covered}
        actual = {(r[0], r[1]): r[2] for r in conn.execute(
            "SELECT source_file, line_no, reason FROM rejects") if r[0] in covered}
        missed = [k for k in expected if k not in actual]
        extra_rej = [k for k in actual if k not in expected]
        wrong = [k for k in expected if k in actual and actual[k] != expected[k]]
        label_info = {"expected": len(expected), "actual": len(actual), "missed": len(missed),
                      "unexpected": len(extra_rej), "wrong_reason": len(wrong)}
        checks.append(_check("labels: every labelled invalid record rejected with the expected reason "
                             "(and nothing else rejected)", not (missed or extra_rej or wrong),
                             json.dumps(label_info)))

    return {
        "passed": all(c["passed"] for c in checks),
        "checks": checks,
        "totals": {"raw_events": raw_n, "distinct_orders": distinct_orders,
                   "superseded_versions": superseded, "rejects": rej_n, "rejects_by_reason": by_reason},
        "strategies": summary,
        "labels": label_info,
    }


def to_markdown(result: dict) -> str:
    lines = ["# Reconciliation report", "",
             f"**Overall: {'PASS' if result['passed'] else 'FAIL'}**", "",
             "| # | Check | Result | Detail |", "|---|-------|--------|--------|"]
    for i, c in enumerate(result["checks"], 1):
        lines.append(f"| {i} | {c['name']} | {'PASS' if c['passed'] else '**FAIL**'} | {c['detail']} |")
    t = result["totals"]
    lines += ["", "## Source-to-target", "",
              f"- landed events (raw_events): {t['raw_events']}",
              f"- distinct orders: {t['distinct_orders']} (superseded versions: {t['superseded_versions']})",
              f"- rejected input rows: {t['rejects']}", "", "### Rejects by reason", "",
              "| reason | rows |", "|--------|------|"]
    lines += [f"| {k} | {v} |" for k, v in t["rejects_by_reason"].items()]
    lines += ["", "## Key aggregates per target", "",
              "| strategy | watermark | rows | sum amount (cents) | sum net amount (cents) |",
              "|----------|-----------|------|--------------------|------------------------|"]
    for s, v in result["strategies"].items():
        lines.append(f"| {s} | {v['watermark']} | {v['rows']} | {v['sum_amount_cents']} | {v['sum_net_amount_cents']} |")
    return "\n".join(lines) + "\n"


def write_report(result: dict, out_dir) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "reconciliation_report.json").write_text(json.dumps(result, indent=2, sort_keys=True))
    (out / "reconciliation_report.md").write_text(to_markdown(result))
