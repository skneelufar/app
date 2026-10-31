"""SQLite connection + schema. Manual transaction control (isolation_level=None)."""
import sqlite3
from pathlib import Path

from .config import TARGET_TABLES

SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id   TEXT PRIMARY KEY,
    region        TEXT NOT NULL,
    signup_date   TEXT,
    loaded_run_id TEXT NOT NULL
);

-- Append-only landed event history (valid, de-duplicated events). Never updated.
CREATE TABLE IF NOT EXISTS raw_events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,   -- ingestion order = watermark
    event_id     TEXT NOT NULL UNIQUE,                -- deterministic sha1 of normalized payload
    order_id     TEXT NOT NULL,
    customer_id  TEXT NOT NULL,
    order_ts     TEXT NOT NULL,
    order_date   TEXT NOT NULL,                       -- partition key (UTC date of order_ts)
    status       TEXT NOT NULL,
    amount_cents INTEGER NOT NULL CHECK (amount_cents >= 0),
    currency     TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    source_file  TEXT NOT NULL,
    line_no      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_events_order_id       ON raw_events(order_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_order_date_seq ON raw_events(order_date, seq);

-- Quarantine: every rejected input row, traceable to file + physical line.
CREATE TABLE IF NOT EXISTS rejects (
    reject_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL,
    source_file TEXT NOT NULL,
    line_no     INTEGER NOT NULL,
    reason      TEXT NOT NULL,
    raw_record  TEXT NOT NULL,                        -- JSON array of the original fields
    rejected_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rejects_run ON rejects(run_id);

CREATE TABLE IF NOT EXISTS ingested_files (
    file_sha256 TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS publish_state (
    strategy   TEXT PRIMARY KEY,
    last_seq   INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            TEXT PRIMARY KEY,
    stage             TEXT NOT NULL,      -- load_customers | ingest | publish
    strategy          TEXT,
    source_file       TEXT,
    status            TEXT NOT NULL,      -- RUNNING | SUCCESS | FAILED | SKIPPED
    started_at        TEXT NOT NULL,
    ended_at          TEXT,
    duration_ms       REAL,
    rows_in           INTEGER,
    rows_out          INTEGER,
    rows_rejected     INTEGER,
    rows_written      INTEGER,
    rows_deleted      INTEGER,
    partitions_touched INTEGER,
    error             TEXT,
    details           TEXT
);
"""


def target_table_sql(name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {name} (
    order_id         TEXT PRIMARY KEY,
    customer_id      TEXT NOT NULL,
    order_ts         TEXT NOT NULL,
    order_date       TEXT NOT NULL,
    status           TEXT NOT NULL,
    amount_cents     INTEGER NOT NULL CHECK (amount_cents >= 0),
    net_amount_cents INTEGER NOT NULL,
    currency         TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    event_id         TEXT NOT NULL,
    loaded_run_id    TEXT NOT NULL
)"""


def target_index_sql(name: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS idx_{name}_order_date ON {name}(order_date)"


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    for table in TARGET_TABLES.values():
        conn.execute(target_table_sql(table))
        conn.execute(target_index_sql(table))


def connect(path) -> sqlite3.Connection:
    path = str(path)
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # explicit BEGIN/COMMIT
    conn.row_factory = sqlite3.Row
    if path != ":memory:":
        conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_schema(conn)
    return conn
