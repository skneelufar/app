"""The three publish strategies. All read the same landed history (raw_events) and write to
their own target table, so results can be compared side by side.

  full_refresh       rebuild everything into a shadow table, then swap (atomic)
  incremental_merge  UPSERT only events newer than the watermark, guarded by (updated_at, event_id)
  partition_replace  delete + re-insert every order_date partition touched since the watermark

Deterministic "latest version" rule used by ALL strategies:
    highest updated_at wins; ties broken by highest event_id.   (independent of arrival order)
"""
import logging
import time

from .config import BUSINESS_COLUMNS, REVENUE_STATUSES, STRATEGIES, TARGET_TABLES
from .db import target_index_sql, target_table_sql
from .errors import FailureInjector
from .runlog import Run, utc_now_iso

log = logging.getLogger("loadlab")

COLS = ", ".join(BUSINESS_COLUMNS + ("loaded_run_id",))
_REV = ", ".join(f"'{s}'" for s in REVENUE_STATUSES)
_UPDATE_SET = ", ".join(f"{c}=excluded.{c}" for c in BUSINESS_COLUMNS[1:] + ("loaded_run_id",))


def _latest_cte(where: str) -> str:
    return f"""WITH ranked AS (
        SELECT order_id, customer_id, order_ts, order_date, status, amount_cents, currency,
               updated_at, event_id,
               ROW_NUMBER() OVER (PARTITION BY order_id
                                  ORDER BY updated_at DESC, event_id DESC) AS rn
        FROM raw_events WHERE {where})"""


_SELECT_LATEST = f"""SELECT order_id, customer_id, order_ts, order_date, status, amount_cents,
        CASE WHEN status IN ({_REV}) THEN amount_cents ELSE 0 END AS net_amount_cents,
        currency, updated_at, event_id, :run_id AS loaded_run_id
    FROM ranked WHERE rn = 1"""


def latest_select_sql(where: str) -> str:
    """Standalone SELECT (used by benchmark micro-tests)."""
    return _latest_cte(where) + " " + _SELECT_LATEST


# ---------------------------------------------------------------- watermark helpers
def get_watermark(conn, strategy) -> int:
    r = conn.execute("SELECT last_seq FROM publish_state WHERE strategy=?", (strategy,)).fetchone()
    return r[0] if r else 0


def set_watermark(conn, strategy, seq) -> None:
    conn.execute(
        """INSERT INTO publish_state (strategy, last_seq, updated_at) VALUES (?,?,?)
           ON CONFLICT(strategy) DO UPDATE SET last_seq=excluded.last_seq,
                                               updated_at=excluded.updated_at""",
        (strategy, seq, utc_now_iso()))


def reset_watermark(conn, strategy) -> None:
    """Backfill / replay: next incremental_merge or partition_replace run reprocesses all history."""
    set_watermark(conn, strategy, 0)


# ---------------------------------------------------------------- strategies
def _full_refresh(conn, run_id, last, max_seq, inj, **_):
    t = TARGET_TABLES["full_refresh"]
    nxt = f"{t}__next"
    conn.execute("BEGIN")
    conn.execute(f"DROP TABLE IF EXISTS {nxt}")
    conn.execute(target_table_sql(nxt))
    before = conn.total_changes
    conn.execute(f"{_latest_cte('seq <= :max')} INSERT INTO {nxt} ({COLS}) {_SELECT_LATEST}",
                 {"max": max_seq, "run_id": run_id})
    written = conn.total_changes - before
    rows_in = conn.execute("SELECT COUNT(*) FROM raw_events WHERE seq <= ?", (max_seq,)).fetchone()[0]
    inj.tick("before_swap")
    conn.execute(f"DROP TABLE {t}")
    conn.execute(f"ALTER TABLE {nxt} RENAME TO {t}")
    conn.execute(target_index_sql(t))
    set_watermark(conn, "full_refresh", max_seq)
    conn.execute("COMMIT")
    return {"rows_in": rows_in, "rows_written": written, "rows_deleted": 0, "partitions_touched": None}


def _incremental_merge(conn, run_id, last, max_seq, inj, **_):
    t = TARGET_TABLES["incremental_merge"]
    conn.execute("BEGIN")
    rows_in = conn.execute("SELECT COUNT(*) FROM raw_events WHERE seq > ? AND seq <= ?",
                           (last, max_seq)).fetchone()[0]
    before = conn.total_changes
    conn.execute(
        f"""{_latest_cte('seq > :last AND seq <= :max')}
            INSERT INTO {t} ({COLS}) {_SELECT_LATEST}
            ON CONFLICT(order_id) DO UPDATE SET {_UPDATE_SET}
            WHERE (excluded.updated_at, excluded.event_id) > ({t}.updated_at, {t}.event_id)""",
        {"last": last, "max": max_seq, "run_id": run_id})
    written = conn.total_changes - before
    set_watermark(conn, "incremental_merge", max_seq)  # same transaction as the data
    inj.tick("before_commit")
    conn.execute("COMMIT")
    return {"rows_in": rows_in, "rows_written": written, "rows_deleted": 0, "partitions_touched": None}


def _partition_replace(conn, run_id, last, max_seq, inj, atomic=False, partitions=None, **_):
    t = TARGET_TABLES["partition_replace"]
    if partitions:
        parts = sorted(set(partitions))
    else:
        parts = [r[0] for r in conn.execute(
            "SELECT DISTINCT order_date FROM raw_events WHERE seq > ? AND seq <= ? ORDER BY order_date",
            (last, max_seq))]
    rows_in = written = deleted = 0
    if atomic:
        conn.execute("BEGIN")
    for d in parts:
        if not atomic:
            conn.execute("BEGIN")
        b0 = conn.total_changes
        conn.execute(f"DELETE FROM {t} WHERE order_date = ?", (d,))
        b1 = conn.total_changes
        deleted += b1 - b0
        conn.execute(f"{_latest_cte('order_date = :d AND seq <= :max')} "
                     f"INSERT INTO {t} ({COLS}) {_SELECT_LATEST}",
                     {"d": d, "max": max_seq, "run_id": run_id})
        written += conn.total_changes - b1
        rows_in += conn.execute("SELECT COUNT(*) FROM raw_events WHERE order_date=? AND seq<=?",
                                (d, max_seq)).fetchone()[0]
        if not atomic:
            conn.execute("COMMIT")          # each partition is durable on its own
        inj.tick(f"partition {d}")
    if not partitions:                      # explicit-partition backfill never moves the watermark
        set_watermark(conn, "partition_replace", max_seq)
    if atomic:
        conn.execute("COMMIT")
    return {"rows_in": rows_in, "rows_written": written, "rows_deleted": deleted,
            "partitions_touched": len(parts)}


_IMPL = {"full_refresh": _full_refresh, "incremental_merge": _incremental_merge,
         "partition_replace": _partition_replace}


def publish(conn, strategy, *, run_id=None, fail_after=None, atomic_partitions=False,
            partitions=None) -> dict:
    """Run one publish. Never raises for pipeline failures: returns status FAILED + error and
    leaves the target in the state documented in docs/DESIGN_TRADEOFFS.md."""
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}; choose from {STRATEGIES}")
    run = Run(conn, "publish", strategy=strategy, run_id=run_id)
    inj = FailureInjector(fail_after)
    try:
        last = get_watermark(conn, strategy)
        max_seq = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM raw_events").fetchone()[0]
        t0 = time.perf_counter()
        m = _IMPL[strategy](conn, run.run_id, last, max_seq, inj,
                            atomic=atomic_partitions, partitions=partitions)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
    except Exception as e:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        err = f"{type(e).__name__}: {e}"
        run.finish("FAILED", error=err, details={"watermark": get_watermark(conn, strategy)})
        return {"run_id": run.run_id, "strategy": strategy, "status": "FAILED", "error": err}
    rows_out = conn.execute(f"SELECT COUNT(*) FROM {TARGET_TABLES[strategy]}").fetchone()[0]
    run.finish("SUCCESS", rows_in=m["rows_in"], rows_out=rows_out, rows_written=m["rows_written"],
               rows_deleted=m["rows_deleted"], partitions_touched=m["partitions_touched"],
               duration_ms=elapsed_ms, details={"from_seq": last, "to_seq": max_seq})
    return {"run_id": run.run_id, "strategy": strategy, "status": "SUCCESS",
            "duration_ms": elapsed_ms, "rows_out": rows_out, "from_seq": last, "to_seq": max_seq, **m}


def publish_all(conn, **kw) -> list:
    return [publish(conn, s, **kw) for s in STRATEGIES]
