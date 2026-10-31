"""Ingestion: CSV file -> validated, de-duplicated raw_events + quarantined rejects.

Each file is ingested in ONE transaction: all-or-nothing. Re-running the same file is a no-op
(sha256 registry) and, even if the bytes differ, duplicate events are dropped (unique event_id).
"""
import csv
import hashlib
import json
import logging
from collections import Counter
from pathlib import Path

from .config import REQUIRED_COLUMNS
from .errors import InjectedFailure, SchemaError
from .runlog import Run, utc_now_iso
from .validate import validate_row

log = logging.getLogger("loadlab")


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _summary(run_id, status, **kw):
    out = {"run_id": run_id, "status": status, "rows_in": 0, "rows_out": 0,
           "rows_rejected": 0, "reasons": {}, "warnings": [], "error": None}
    out.update(kw)
    return out


def ingest_file(conn, path, run_id=None, fail_after_rows=None) -> dict:
    path = Path(path)
    source_file = path.name
    run = Run(conn, "ingest", source_file=source_file, run_id=run_id)
    try:
        digest = sha256_file(path)
        if conn.execute("SELECT 1 FROM ingested_files WHERE file_sha256=?", (digest,)).fetchone():
            run.finish("SKIPPED", details={"reason": "file already ingested (same sha256)"})
            return _summary(run.run_id, "SKIPPED", warnings=["file already ingested"])
        res = _ingest(conn, path, source_file, digest, run.run_id, fail_after_rows)
    except SchemaError as e:
        run.finish("FAILED", error=f"SchemaError: {e}")
        return _summary(run.run_id, "FAILED", error=f"SchemaError: {e}")
    except Exception as e:  # includes InjectedFailure, decode errors, disk errors
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        run.finish("FAILED", error=f"{type(e).__name__}: {e}")
        return _summary(run.run_id, "FAILED", error=f"{type(e).__name__}: {e}")
    run.finish("SUCCESS", rows_in=res["rows_in"], rows_out=res["rows_out"],
               rows_rejected=res["rows_rejected"],
               details={"reasons": res["reasons"], "warnings": res["warnings"]})
    return res


def _ingest(conn, path, source_file, digest, run_id, fail_after_rows):
    known = {r[0] for r in conn.execute("SELECT customer_id FROM dim_customers")}
    warnings, rejects, reasons = [], [], Counter()
    rows_in = rows_out = 0
    order_ts_seen = {}

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:  # zero-byte file == empty batch
            warnings.append("empty_file")
            _register_file(conn, digest, source_file, run_id)
            return _summary(run_id, "SUCCESS", warnings=warnings)
        header = [h.strip().lower() for h in header]
        if len(set(header)) != len(header):
            raise SchemaError(f"duplicate column names in header: {header}")
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        if missing:
            raise SchemaError(f"missing required columns {missing}; header was {header}")
        extra = [c for c in header if c not in REQUIRED_COLUMNS]
        if extra:
            warnings.append(f"unexpected_columns_ignored:{','.join(extra)}")
        idx = {c: header.index(c) for c in REQUIRED_COLUMNS}

        conn.execute("BEGIN")
        for row in reader:
            if not row:          # blank line
                continue
            line_no = reader.line_num
            rows_in += 1

            def reject(reason, row=row, line_no=line_no):
                reasons[reason] += 1
                rejects.append((run_id, source_file, line_no, reason,
                                json.dumps(row, ensure_ascii=False), utc_now_iso()))

            if len(row) != len(header):
                reject("malformed_row")
                continue
            ev, reason = validate_row({c: row[idx[c]] for c in REQUIRED_COLUMNS}, known)
            if reason:
                reject(reason)
                continue
            oid = ev["order_id"]
            prior_ts = order_ts_seen.get(oid)
            if prior_ts is None:
                r = conn.execute("SELECT order_ts FROM raw_events WHERE order_id=? LIMIT 1",
                                 (oid,)).fetchone()
                prior_ts = r[0] if r else None
            if prior_ts is not None and prior_ts != ev["order_ts"]:
                reject("order_ts_changed")   # order_ts is immutable (partition key stability)
                continue
            cur = conn.execute(
                """INSERT OR IGNORE INTO raw_events (event_id, order_id, customer_id, order_ts,
                       order_date, status, amount_cents, currency, updated_at, run_id,
                       source_file, line_no) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (ev["event_id"], oid, ev["customer_id"], ev["order_ts"], ev["order_date"],
                 ev["status"], ev["amount_cents"], ev["currency"], ev["updated_at"],
                 run_id, source_file, line_no))
            if cur.rowcount == 0:
                reject("duplicate_event")
                continue
            order_ts_seen[oid] = ev["order_ts"]
            rows_out += 1
            if fail_after_rows is not None and rows_out >= fail_after_rows:
                raise InjectedFailure(f"injected failure after {rows_out} accepted rows")

    if rows_in == 0:
        warnings.append("empty_batch")
    conn.executemany(
        "INSERT INTO rejects (run_id, source_file, line_no, reason, raw_record, rejected_at)"
        " VALUES (?,?,?,?,?,?)", rejects)
    _register_file(conn, digest, source_file, run_id)
    conn.execute("COMMIT")
    return _summary(run_id, "SUCCESS", rows_in=rows_in, rows_out=rows_out,
                    rows_rejected=len(rejects), reasons=dict(reasons), warnings=warnings)


def _register_file(conn, digest, source_file, run_id):
    in_tx = conn.in_transaction
    if not in_tx:
        conn.execute("BEGIN")
    conn.execute("INSERT INTO ingested_files (file_sha256, source_file, run_id, ingested_at)"
                 " VALUES (?,?,?,?)", (digest, source_file, run_id, utc_now_iso()))
    if not in_tx:
        conn.execute("COMMIT")


def ingest_path(conn, path) -> list:
    """Ingest one file, or every *.csv in a directory in lexical (= chronological) order."""
    p = Path(path)
    files = sorted(p.glob("*.csv")) if p.is_dir() else [p]
    return [dict(ingest_file(conn, f), source_file=f.name) for f in files]


def load_customers(conn, path, run_id=None) -> dict:
    """Idempotent upsert of the customer dimension (customer_id, region, signup_date)."""
    path = Path(path)
    run = Run(conn, "load_customers", source_file=path.name, run_id=run_id)
    rows_in = rows_out = 0
    rejects = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            need = {"customer_id", "region"}
            if not reader.fieldnames or not need.issubset({h.strip().lower() for h in reader.fieldnames}):
                raise SchemaError(f"customers file needs columns {sorted(need)}; got {reader.fieldnames}")
            conn.execute("BEGIN")
            for rec in reader:
                rows_in += 1
                rec = {(k or "").strip().lower(): (v or "").strip() for k, v in rec.items()}
                if not rec.get("customer_id") or not rec.get("region"):
                    rejects.append((run.run_id, path.name, reader.line_num, "missing_customer_field",
                                    json.dumps(rec), utc_now_iso()))
                    continue
                conn.execute(
                    """INSERT INTO dim_customers (customer_id, region, signup_date, loaded_run_id)
                       VALUES (?,?,?,?) ON CONFLICT(customer_id) DO UPDATE SET
                       region=excluded.region, signup_date=excluded.signup_date,
                       loaded_run_id=excluded.loaded_run_id""",
                    (rec["customer_id"], rec["region"], rec.get("signup_date") or None, run.run_id))
                rows_out += 1
            conn.executemany(
                "INSERT INTO rejects (run_id, source_file, line_no, reason, raw_record, rejected_at)"
                " VALUES (?,?,?,?,?,?)", rejects)
            conn.execute("COMMIT")
    except Exception as e:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        run.finish("FAILED", error=f"{type(e).__name__}: {e}")
        return {"run_id": run.run_id, "status": "FAILED", "error": str(e)}
    run.finish("SUCCESS", rows_in=rows_in, rows_out=rows_out, rows_rejected=len(rejects))
    return {"run_id": run.run_id, "status": "SUCCESS", "rows_in": rows_in,
            "rows_out": rows_out, "rows_rejected": len(rejects)}
