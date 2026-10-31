"""Reproducible synthetic data generator (fixed seed). Inputs and answer labels are separate:

  <out>/customers.csv
  <out>/batches/orders_<arrival_date>.csv      <- pipeline input (daily files, shuffled rows)
  <out>/labels/expected_rejects.csv            <- ANSWER KEY, never read by the pipeline
  <out>/labels/generation_summary.json

Data features: order lifecycles (created -> paid -> shipped/refunded or cancelled), late-arriving
events (arrive 1-3 days after they happened), out-of-order updates, exact duplicate rows (same
file and cross-file), boundary amounts (0.00 and 1,000,000.00) and deliberately invalid rows
covering every reject reason.
"""
import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .validate import fmt_ts

HEADER = "order_id,customer_id,order_ts,status,amount,currency,updated_at"
REGION_CCY = {"IN": "INR", "EU": "EUR", "US": "USD"}

INVALID_KINDS = [
    "missing_order_id", "invalid_order_id", "missing_customer_id", "unknown_customer",
    "invalid_order_ts", "invalid_updated_at", "updated_before_order_ts", "invalid_status",
    "missing_amount", "invalid_amount", "negative_amount", "amount_out_of_range",
    "invalid_currency", "malformed_row", "order_ts_changed",
]


def _money(cents: int) -> str:
    return f"{cents // 100}.{cents % 100:02d}"


def _lifecycle(rng, order_ts, amount_cents, boundary):
    events = [("created", order_ts, amount_cents)]
    t = order_ts

    def step():
        return timedelta(minutes=rng.randint(1, 2 * 24 * 60))

    if rng.random() < 0.10:
        t += step()
        events.append(("cancelled", t, amount_cents))
        return events
    t += step()
    if not boundary and rng.random() < 0.10:               # price adjustment on payment
        amount_cents = max(0, amount_cents + rng.randint(-200, 200))
    events.append(("paid", t, amount_cents))
    if rng.random() < 0.70:
        t += step()
        events.append(("shipped", t, amount_cents))
        if rng.random() < 0.10:
            t += step()
            events.append(("refunded", t, amount_cents))
    return events


def _invalid_row(kind, n, arrival, customers, pool, rng):
    ts = datetime(arrival.year, arrival.month, arrival.day, 12, 0, 0, tzinfo=timezone.utc)
    f = [f"bad_{n:06d}", rng.choice(customers)[0], fmt_ts(ts), "created", "10.00", "USD", fmt_ts(ts)]
    if kind == "missing_order_id": f[0] = ""
    elif kind == "invalid_order_id": f[0] = "bad id!"
    elif kind == "missing_customer_id": f[1] = ""
    elif kind == "unknown_customer": f[1] = "cust_99999"
    elif kind == "invalid_order_ts": f[2] = "not-a-timestamp"
    elif kind == "invalid_updated_at": f[6] = "2026-13-45T00:00:00Z"
    elif kind == "updated_before_order_ts": f[6] = fmt_ts(ts - timedelta(hours=1))
    elif kind == "invalid_status": f[3] = "teleported"
    elif kind == "missing_amount": f[4] = ""
    elif kind == "invalid_amount": f[4] = rng.choice(["12.3.4", "abc", "10.999"])
    elif kind == "negative_amount": f[4] = "-5.00"
    elif kind == "amount_out_of_range": f[4] = "1000000.01"
    elif kind == "invalid_currency": f[5] = "XXX"
    elif kind == "malformed_row": return ",".join(f[:4]), kind
    elif kind == "order_ts_changed":
        if not pool:
            return None
        oid, cust, orig_ts = rng.choice(pool)
        new_ts = orig_ts + timedelta(hours=1)
        f = [oid, cust, fmt_ts(new_ts), "paid", "10.00", "USD", fmt_ts(new_ts + timedelta(minutes=1))]
    return ",".join(f), kind


def generate(out_dir, n_orders=200, n_customers=20, days=5, start="2026-01-01", seed=42,
             invalid_rate=0.05, duplicate_rate=0.03, late_rate=0.15) -> dict:
    rng = random.Random(seed)
    out = Path(out_dir)
    (out / "batches").mkdir(parents=True, exist_ok=True)
    (out / "labels").mkdir(parents=True, exist_ok=True)
    for old in (out / "batches").glob("*.csv"):
        old.unlink()
    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)

    customers = []
    for i in range(n_customers):
        region = rng.choice(sorted(REGION_CCY))
        signup = (start_dt - timedelta(days=rng.randint(30, 700))).date().isoformat()
        customers.append((f"cust_{i:05d}", region, signup))
    with open(out / "customers.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["customer_id", "region", "signup_date"])
        w.writerows(customers)
    ccy = {c[0]: REGION_CCY[c[1]] for c in customers}

    by_arrival = {}
    n_events = 0
    for i in range(n_orders):
        oid = f"ord_{i:07d}"
        cust = rng.choice(customers)[0]
        order_ts = start_dt + timedelta(days=rng.randrange(days), seconds=rng.randrange(86400))
        boundary = i < 2
        cents = 0 if i == 0 else 100_000_000 if i == 1 else rng.randint(500, 50_000)
        for status, upd, amt in _lifecycle(rng, order_ts, cents, boundary):
            arrival = upd.date()
            if rng.random() < late_rate:
                arrival += timedelta(days=rng.randint(1, 3))
            by_arrival.setdefault(arrival, []).append(
                (oid, cust, order_ts, ",".join([oid, cust, fmt_ts(order_ts), status, _money(amt),
                                                ccy[cust], fmt_ts(upd)])))
            n_events += 1

    labels, pool, prev_valid, bad_n, kind_i = [], [], [], 0, 0
    summary_rows = {"valid": 0, "invalid": 0, "duplicate": 0}
    for arrival in sorted(by_arrival):
        fname = f"orders_{arrival.isoformat()}.csv"
        valid = by_arrival[arrival]
        rng.shuffle(valid)                                   # out-of-order inside the file
        items = [(line, None) for (_, _, _, line) in valid]
        for _ in range(max(1, round(invalid_rate * len(valid)))):
            for _try in range(len(INVALID_KINDS)):
                kind = INVALID_KINDS[kind_i % len(INVALID_KINDS)]
                kind_i += 1
                built = _invalid_row(kind, bad_n, arrival, customers, pool, rng)
                if built:
                    break
            bad_n += 1
            items.insert(rng.randint(0, len(items)), built)
        valid_lines = [line for line, lab in items if lab is None]
        for j in range(max(1, round(duplicate_rate * len(valid)))):
            src = valid_lines if (j % 2 == 0 or not prev_valid) else prev_valid
            items.append((rng.choice(src), "duplicate_event"))
        with open(out / "batches" / fname, "w", newline="") as f:
            f.write(HEADER + "\n")
            for line_no, (line, lab) in enumerate(items, start=2):
                f.write(line + "\n")
                if lab:
                    labels.append((fname, line_no, lab))
        summary_rows["valid"] += len(valid)
        summary_rows["invalid"] += sum(1 for _, l in items if l and l != "duplicate_event")
        summary_rows["duplicate"] += sum(1 for _, l in items if l == "duplicate_event")
        prev_valid = valid_lines
        pool += [(oid, cust, ts) for (oid, cust, ts, _) in valid]

    with open(out / "labels" / "expected_rejects.csv", "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["source_file", "line_no", "expected_reason"])
        w.writerows(labels)
    summary = {"seed": seed, "orders": n_orders, "customers": n_customers, "days": days,
               "batch_files": len(by_arrival), "valid_events": n_events,
               "labelled_rejects": len(labels), "rows": summary_rows}
    (out / "labels" / "generation_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def write_edge_cases(out_dir) -> list:
    """Small hand-made files for the failure demo (empty batch, schema change, malformed...)."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    good = "edge_000001,cust_00000,2026-01-02T10:00:00Z,created,19.99,USD,2026-01-02T10:00:00Z"
    files = {
        "edge_01_header_only.csv": HEADER + "\n",
        "edge_02_zero_bytes.csv": "",
        "edge_03_missing_currency_column.csv":
            "order_id,customer_id,order_ts,status,amount,updated_at\n"
            "edge_000002,cust_00000,2026-01-02T10:00:00Z,created,19.99,2026-01-02T10:00:00Z\n",
        "edge_04_extra_column.csv":
            HEADER + ",channel\n" + good + ",web\n",
        "edge_05_all_malformed.csv": HEADER + "\nonly,three,fields\n\"unterminated\n",
    }
    for name, text in files.items():
        (out / name).write_text(text)
    return sorted(files)
