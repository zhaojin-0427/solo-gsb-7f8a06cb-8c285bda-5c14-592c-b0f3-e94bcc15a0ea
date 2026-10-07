"""Control service: immutable batches, lease-based dispatch, cancellation."""
from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from .common import content_hash, is_printable_ascii, op_key, sorted_labels

DB_PATH = os.environ.get("CONTROL_DB", "data/control/control.db")
LEASE_TTL = float(os.environ.get("LEASE_TTL", "12"))
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# --------------------------------------------------------------------- models


class LabelIn(BaseModel):
    seq: int = Field(ge=1, le=9999)
    text: str

    @field_validator("text")
    @classmethod
    def _check_text(cls, value: str) -> str:
        if not is_printable_ascii(value):
            raise ValueError(
                "text must be 1-80 printable ASCII characters (0x20-0x7E)"
            )
        return value


class BatchIn(BaseModel):
    request_key: str = Field(min_length=1, max_length=200)
    labels: list[LabelIn] = Field(min_length=1, max_length=8)


# ---------------------------------------------------------------- database


def _connect() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=8000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def db() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def txn() -> Iterator[sqlite3.Connection]:
    """Serialised write transaction (SQLite BEGIN IMMEDIATE)."""
    conn = _connect()
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS batches (
                id            TEXT PRIMARY KEY,
                request_key   TEXT NOT NULL UNIQUE,
                content_hash  TEXT NOT NULL,
                label_count   INTEGER NOT NULL,
                status        TEXT NOT NULL DEFAULT 'active',
                created_at    REAL NOT NULL,
                cancelled_at  REAL
            );
            CREATE TABLE IF NOT EXISTS labels (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id           TEXT NOT NULL REFERENCES batches(id),
                seq                INTEGER NOT NULL,
                text               TEXT NOT NULL,
                status             TEXT NOT NULL DEFAULT 'pending',
                leased_by          TEXT,
                lease_expires_at   REAL,
                attempts           INTEGER NOT NULL DEFAULT 0,
                dispatched_at      REAL,
                confirmed_at       REAL,
                receipt_id         TEXT,
                last_error         TEXT,
                UNIQUE(batch_id, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_labels_dispatch
                ON labels(status, batch_id, seq);
            """
        )


# ------------------------------------------------------------------ helpers


def _err(status: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status, detail={"error": code, "message": message})


def _row_to_label(row: sqlite3.Row, now: float) -> dict[str, Any]:
    data = dict(row)
    remaining: float | None = None
    if data["status"] == "leased" and data.get("lease_expires_at"):
        remaining = max(0.0, data["lease_expires_at"] - now)
    data["lease_remaining"] = round(remaining, 1) if remaining is not None else None
    data["op_key"] = op_key(data["batch_id"], data["seq"])
    return data


def _batch_payload(conn: sqlite3.Connection, batch_row: sqlite3.Row) -> dict[str, Any]:
    now = time.time()
    labels = [
        _row_to_label(row, now)
        for row in conn.execute(
            "SELECT * FROM labels WHERE batch_id = ? ORDER BY seq", (batch_row["id"],)
        )
    ]
    payload = dict(batch_row)
    payload["labels"] = labels
    return payload


# -------------------------------------------------------------------- app


app = FastAPI(title="Label Print Control")


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"service": "control", "status": "ok"}


# ---- batch submission -----------------------------------------------------


@app.post("/api/batches", status_code=201)
def submit_batch(body: BatchIn) -> JSONResponse:
    labels = sorted_labels([item.model_dump() for item in body.labels])

    # whole-order validation: unique seq
    seqs = [item["seq"] for item in labels]
    if len(set(seqs)) != len(seqs):
        raise _err(400, "duplicate_seq", "duplicate sequence numbers in batch")

    digest = content_hash(labels)
    now = time.time()

    # FastAPI guarantees request_key is a non-empty string; payload validation
    # (count/ascii/unique seq) already happened in the pydantic model.
    with txn() as conn:
        existing = conn.execute(
            "SELECT * FROM batches WHERE request_key = ?", (body.request_key,)
        ).fetchone()
        if existing is not None:
            if existing["content_hash"] == digest and existing["label_count"] == len(labels):
                # idempotent replay: same key, same content -> original batch
                return JSONResponse(
                    status_code=200,
                    content={
                        "batch": _batch_payload(conn, existing),
                        "reused": True,
                    },
                )
            # same key, different content -> conflict, nothing is written
            raise _err(
                409,
                "request_key_conflict",
                f"request_key {body.request_key!r} already used with different content",
            )

        batch_id = "B-" + uuid.uuid4().hex[:12]
        conn.execute(
            """INSERT INTO batches (id, request_key, content_hash, label_count,
                                    status, created_at)
               VALUES (?, ?, ?, ?, 'active', ?)""",
            (batch_id, body.request_key, digest, len(labels), now),
        )
        conn.executemany(
            """INSERT INTO labels (batch_id, seq, text, status)
               VALUES (?, ?, ?, 'pending')""",
            [(batch_id, item["seq"], item["text"]) for item in labels],
        )
        row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        payload = _batch_payload(conn, row)

    return JSONResponse(status_code=201, content={"batch": payload, "reused": False})


# ---- queries / cancellation ----------------------------------------------


@app.get("/api/batches")
def list_batches() -> dict[str, Any]:
    with db() as conn:
        rows = conn.execute("SELECT * FROM batches ORDER BY created_at DESC").fetchall()
        return {"batches": [_batch_payload(conn, row) for row in rows]}


@app.get("/api/batches/{batch_id}")
def get_batch(batch_id: str) -> dict[str, Any]:
    with db() as conn:
        row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if row is None:
            raise _err(404, "not_found", "batch not found")
        return {"batch": _batch_payload(conn, row)}


@app.post("/api/batches/{batch_id}/cancel")
def cancel_batch(batch_id: str) -> dict[str, Any]:
    """Cancellation commits in control-DB transaction order: once committed,
    no new label of this batch is dispatched. Labels still 'pending' have
    never been leased, so they are finalised as 'absent' (not printed) in
    the same transaction. Leased labels stay pending verification until
    reconciled against the printer ledger."""
    with txn() as conn:
        row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        if row is None:
            raise _err(404, "not_found", "batch not found")
        if row["status"] != "cancelled":
            now = time.time()
            conn.execute(
                "UPDATE batches SET status = 'cancelled', cancelled_at = ? WHERE id = ?",
                (now, batch_id),
            )
            # never dispatched (lease commit lost the race against this cancel)
            conn.execute(
                """UPDATE labels
                   SET status = 'absent', confirmed_at = ?
                   WHERE batch_id = ? AND status = 'pending'""",
                (now, batch_id),
            )
        row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
        return {"batch": _batch_payload(conn, row)}


# ---- internal worker API --------------------------------------------------


@app.post("/internal/work")
def claim_work(worker_id: str = Query(min_length=1, max_length=100)) -> JSONResponse:
    """Atomically hand out one unit of work.

    Selection order inside a single BEGIN IMMEDIATE transaction:
      1. an unexpired lease held by THIS worker (recovery after worker restart
         or after control came back),
      2. an expired lease held by anyone (dead worker / lease timeout),
      3. a fresh pending label, strictly after every earlier label of the
         same batch is already printed, and only for active batches.
    """
    now = time.time()
    with txn() as conn:
        # 1. own live lease
        row = conn.execute(
            """SELECT * FROM labels
               WHERE status = 'leased' AND leased_by = ? AND lease_expires_at > ?
               ORDER BY batch_id, seq LIMIT 1""",
            (worker_id, now),
        ).fetchone()
        kind = "recover"

        # 2. expired lease
        if row is None:
            row = conn.execute(
                """SELECT * FROM labels
                   WHERE status = 'leased' AND lease_expires_at <= ?
                   ORDER BY batch_id, seq LIMIT 1""",
                (now,),
            ).fetchone()
            kind = "adopt"

        # 3. fresh work, in fixed order, no dispatch past an unconfirmed earlier label
        if row is None:
            row = conn.execute(
                """SELECT l.* FROM labels l
                   JOIN batches b ON b.id = l.batch_id
                   WHERE l.status = 'pending' AND b.status = 'active'
                     AND NOT EXISTS (
                         SELECT 1 FROM labels e
                         WHERE e.batch_id = l.batch_id
                           AND e.seq < l.seq
                           AND e.status != 'printed'
                     )
                   ORDER BY l.batch_id, l.seq LIMIT 1"""
            ).fetchone()
            kind = "claim"

        if row is None:
            return JSONResponse(status_code=204, content=None)

        new_expiry = now + LEASE_TTL
        conn.execute(
            """UPDATE labels
               SET status = 'leased', leased_by = ?, lease_expires_at = ?,
                   attempts = attempts + 1,
                   dispatched_at = COALESCE(dispatched_at, ?),
                   last_error = NULL
               WHERE id = ?""",
            (worker_id, new_expiry, now, row["id"]),
        )
        batch = conn.execute(
            "SELECT * FROM batches WHERE id = ?", (row["batch_id"],)
        ).fetchone()

        job = {
            "op_key": op_key(row["batch_id"], row["seq"]),
            "batch_id": row["batch_id"],
            "request_key": batch["request_key"],
            "seq": row["seq"],
            "text": row["text"],
            "kind": kind,
            "batch_status": batch["status"],
            "lease_ttl": LEASE_TTL,
            "attempt": row["attempts"] + 1,
        }
        return JSONResponse(status_code=200, content={"job": job})


class ConfirmIn(BaseModel):
    receipt_id: str
    printed_at: float | None = None


@app.post("/internal/ops/{operation_key}/confirm")
def confirm_printed(operation_key: str, body: ConfirmIn) -> dict[str, Any]:
    """Ledger is truth: a reconciled receipt always marks the label printed,
    even if the batch was cancelled afterwards."""
    with txn() as conn:
        row = _load_op(conn, operation_key)
        conn.execute(
            """UPDATE labels
               SET status = 'printed', receipt_id = ?, confirmed_at = ?,
                   lease_expires_at = NULL, leased_by = NULL, last_error = NULL
               WHERE id = ?""",
            (body.receipt_id, body.printed_at or time.time(), row["id"]),
        )
        return {"status": "printed", "op_key": operation_key}


@app.post("/internal/ops/{operation_key}/absent")
def mark_absent(operation_key: str, worker_id: str = Query(min_length=1)) -> dict[str, Any]:
    """Only for cancelled batches whose lease has expired and whose ledger
    lookup proves the label never printed."""
    now = time.time()
    with txn() as conn:
        row = _load_op(conn, operation_key)
        batch = conn.execute(
            "SELECT * FROM batches WHERE id = ?", (row["batch_id"],)
        ).fetchone()
        if batch["status"] != "cancelled":
            raise _err(409, "not_cancelled", "batch is active; label must be printed")
        if row["status"] != "leased":
            raise _err(409, "not_leased", f"label status is {row['status']}")
        if row["leased_by"] == worker_id and row["lease_expires_at"] > now:
            raise _err(
                409,
                "lease_still_valid",
                "your own lease is still valid; another worker may still print it",
            )
        conn.execute(
            """UPDATE labels
               SET status = 'absent', confirmed_at = ?, lease_expires_at = NULL,
                   leased_by = NULL
               WHERE id = ?""",
            (now, row["id"]),
        )
        return {"status": "absent", "op_key": operation_key}


class ErrorIn(BaseModel):
    detail: str = Field(max_length=500)


@app.post("/internal/ops/{operation_key}/error")
def report_error(operation_key: str, body: ErrorIn) -> dict[str, Any]:
    with txn() as conn:
        row = _load_op(conn, operation_key)
        conn.execute(
            "UPDATE labels SET last_error = ? WHERE id = ?",
            (body.detail[:500], row["id"]),
        )
        return {"status": "recorded", "op_key": operation_key}


def _load_op(conn: sqlite3.Connection, operation_key: str) -> sqlite3.Row:
    if "#L" not in operation_key:
        raise _err(400, "bad_op_key", "operation key must look like <batch-id>#L<seq>")
    batch_id, _, seq_part = operation_key.rpartition("#L")
    try:
        seq = int(seq_part)
    except ValueError:
        raise _err(400, "bad_op_key", "bad sequence in operation key")
    row = conn.execute(
        "SELECT * FROM labels WHERE batch_id = ? AND seq = ?", (batch_id, seq)
    ).fetchone()
    if row is None:
        raise _err(404, "not_found", f"unknown operation key {operation_key}")
    return row


# ---------------------------------------------------------------- frontend

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index() -> FileResponse:
    return FileResponse(str(STATIC_DIR / "index.html"))
