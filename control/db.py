"""SQLite storage for the control service.

All state (immutable batches, per-label dispatch state, worker leases)
lives here.  Writes are serialized via a single connection guarded by a
database lock and SQLite's own busy timeout; reads use separate
connections.
"""

import os
import sqlite3
import threading
import time

DB_PATH = os.environ.get("CONTROL_DB", "/data/control.db")

LEASE_TTL = int(os.environ.get("LEASE_TTL", "30"))  # seconds

_db_lock = threading.RLock()
_write_lock = threading.Lock()
_init_lock = threading.Lock()
_initialized = False

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id           TEXT PRIMARY KEY,
    request_key  TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    label_count  INTEGER NOT NULL,
    cancelled    INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    cancelled_at TEXT,
    UNIQUE(request_key)
);

CREATE TABLE IF NOT EXISTS labels (
    id            TEXT PRIMARY KEY,
    batch_id      TEXT NOT NULL REFERENCES batches(id),
    seq           INTEGER NOT NULL,
    content       TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
        -- pending | checking | printed | not_printed
    op_key        TEXT,                  -- set atomically when dispatched
    worker_id     TEXT,
    leased_until  REAL,
    updated_at    TEXT NOT NULL,
    UNIQUE(batch_id, seq)
);

CREATE TABLE IF NOT EXISTS batch_leases (
    batch_id   TEXT PRIMARY KEY REFERENCES batches(id),
    worker_id  TEXT NOT NULL,
    last_seen  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    at         TEXT NOT NULL,
    batch_id   TEXT,
    label_id   TEXT,
    worker_id  TEXT,
    kind       TEXT NOT NULL,
    detail     TEXT
);
"""


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    global _initialized
    with _init_lock:
        if _initialized:
            return
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        conn = _connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()
        _initialized = True


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def tx():
    """Return (connection, write_lock) context for a serialized write tx."""
    return _Tx()


class _Tx:
    def __enter__(self):
        _write_lock.acquire()
        self.conn = _connect()
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.conn.execute("COMMIT")
            else:
                self.conn.execute("ROLLBACK")
        finally:
            self.conn.close()
            _write_lock.release()


def query_all(sql, args=()):
    conn = _connect()
    try:
        return conn.execute(sql, args).fetchall()
    finally:
        conn.close()


def query_one(sql, args=()):
    conn = _connect()
    try:
        return conn.execute(sql, args).fetchone()
    finally:
        conn.close()
