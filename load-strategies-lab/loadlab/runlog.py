"""Run tracking: every stage execution gets a run_id and a pipeline_runs row."""
import json
import logging
import time
import uuid
from datetime import datetime, timezone

log = logging.getLogger("loadlab")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_run_id(stage: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{stage}-{stamp}-{uuid.uuid4().hex[:8]}"


class Run:
    """Inserts a RUNNING row immediately (committed, outside any data transaction)
    so a crash still leaves evidence; finish() records the outcome."""

    def __init__(self, conn, stage, strategy=None, source_file=None, run_id=None):
        self.conn = conn
        self.stage = stage
        self.strategy = strategy
        self.source_file = source_file
        self.run_id = run_id or new_run_id(stage)
        self.t0 = time.perf_counter()
        conn.execute(
            "INSERT INTO pipeline_runs (run_id, stage, strategy, source_file, status, started_at)"
            " VALUES (?, ?, ?, ?, 'RUNNING', ?)",
            (self.run_id, stage, strategy, source_file, utc_now_iso()),
        )

    def finish(self, status, *, rows_in=None, rows_out=None, rows_rejected=None,
               rows_written=None, rows_deleted=None, partitions_touched=None,
               error=None, details=None, duration_ms=None):
        if duration_ms is None:
            duration_ms = (time.perf_counter() - self.t0) * 1000.0
        self.conn.execute(
            """UPDATE pipeline_runs SET status=?, ended_at=?, duration_ms=?, rows_in=?, rows_out=?,
                      rows_rejected=?, rows_written=?, rows_deleted=?, partitions_touched=?,
                      error=?, details=? WHERE run_id=?""",
            (status, utc_now_iso(), duration_ms, rows_in, rows_out, rows_rejected, rows_written,
             rows_deleted, partitions_touched, error,
             json.dumps(details, sort_keys=True) if details is not None else None, self.run_id),
        )
        level = logging.INFO if status in ("SUCCESS", "SKIPPED") else logging.ERROR
        log.log(level,
                "run_id=%s stage=%s strategy=%s source_file=%s status=%s rows_in=%s rows_out=%s "
                "rejected=%s written=%s deleted=%s partitions=%s duration_ms=%.1f error=%s",
                self.run_id, self.stage, self.strategy, self.source_file, status, rows_in,
                rows_out, rows_rejected, rows_written, rows_deleted, partitions_touched,
                duration_ms, error)
        return duration_ms
