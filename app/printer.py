"""Printer simulator: atomic ledger entries, idempotent receipts, fault switches.

Faults are armed per operation key. A fault may be armed *before* the batch
exists using the request-key alias "<request_key>#L<seq>"; the first print
resolves and binds it to the concrete operation key.

Faults (configured per key):
  drop_once : after the ledger entry is committed, the *first* response is
              dropped (a delayed 503). Retries with the same operation key find
              the committed receipt in the ledger and never print twice.
  pause     : after the ledger entry is committed, the receipt is withheld
              (GET 423 / POST waits) until the fault is explicitly released.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

from .common import is_printable_ascii

DB_PATH = os.environ.get("PRINTER_DB", "data/printer/printer.db")
DROP_DELAY = float(os.environ.get("PRINTER_DROP_DELAY", "10"))

VALID_MODES = {"drop_once", "pause"}


# ---------------------------------------------------------------- database


def _connect() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=8000")
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
            CREATE TABLE IF NOT EXISTS ledger (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                op_key      TEXT NOT NULL UNIQUE,
                batch_id    TEXT,
                seq         INTEGER,
                text        TEXT NOT NULL,
                receipt_id  TEXT NOT NULL,
                printed_at  REAL NOT NULL,
                held        INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS faults (
                op_key    TEXT PRIMARY KEY,
                mode      TEXT NOT NULL,
                held      INTEGER NOT NULL DEFAULT 0,
                consumed  INTEGER NOT NULL DEFAULT 0,
                set_at    REAL NOT NULL
            );
            """
        )


# ------------------------------------------------------- pause event registry

# For each *held* (paused, committed but unreleased) op_key one cleared event;
# print calls block on it until release. Guarded by _events_lock.
_events_lock = threading.Lock()
_events: dict[str, threading.Event] = {}


def _hold_event(op_key: str) -> threading.Event:
    """Get (creating if needed) the event that gates a *held* receipt.
    New entries start cleared: the receipt stays withheld until release."""
    with _events_lock:
        event = _events.get(op_key)
        if event is None:
            event = threading.Event()  # cleared: callers block
            _events[op_key] = event
        return event


def _release_event(op_key: str) -> None:
    with _events_lock:
        event = _events.setdefault(op_key, threading.Event())
        event.set()


def _is_blocking(op_key: str) -> bool:
    """True while at least one print call is parked on an unreleased hold."""
    with _events_lock:
        event = _events.get(op_key)
        return event is not None and not event.is_set()


# ----------------------------------------------------------------- models


class PrintIn(BaseModel):
    op_key: str = Field(min_length=1, max_length=240)
    text: str
    batch_id: str | None = None
    request_key: str | None = None
    seq: int | None = None


class FaultIn(BaseModel):
    op_key: str = Field(min_length=1, max_length=240)
    mode: str

    @field_validator("mode")
    @classmethod
    def _mode(cls, value: str) -> str:
        if value not in VALID_MODES:
            raise ValueError(f"mode must be one of {sorted(VALID_MODES)}")
        return value


# -------------------------------------------------------------------- app

app = FastAPI(title="Printer Simulator")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _startup() -> None:
    init_db()
    # reconstruct waiters for entries still held across a printer restart
    with db() as conn:
        held = conn.execute("SELECT op_key FROM faults WHERE held = 1").fetchall()
    with _events_lock:
        for row in held:
            event = threading.Event()
            _events[row["op_key"]] = event


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"service": "printer", "status": "ok"}


# ---------------------------------------------------------------- printing


def _fault_alias(body: "PrintIn") -> str | None:
    """Request-key alias '<request_key>#L<seq>', for faults armed pre-submit."""
    if body.request_key and body.seq is not None:
        return f"{body.request_key}#L{body.seq}"
    return None


def _load_fault(conn: sqlite3.Connection, body: "PrintIn") -> sqlite3.Row | None:
    fault = conn.execute(
        "SELECT * FROM faults WHERE op_key = ?", (body.op_key,)
    ).fetchone()
    if fault is None:
        alias = _fault_alias(body)
        if alias is not None:
            fault = conn.execute(
                "SELECT * FROM faults WHERE op_key = ?", (alias,)
            ).fetchone()
    return fault


@app.post("/print")
def print_label(body: PrintIn) -> dict:
    """Idempotent by op_key. Same key + same text -> same receipt (possibly
    after waiting for a pause). Same key + different text -> 409."""
    if not is_printable_ascii(body.text):
        raise HTTPException(status_code=400, detail={"error": "bad_text"})

    # Fast path: ledger entry exists -> validate content, honor any live pause.
    with db() as conn:
        existing = conn.execute(
            "SELECT * FROM ledger WHERE op_key = ?", (body.op_key,)
        ).fetchone()
        fault = _load_fault(conn, body)

    if existing is not None:
        if existing["text"] != body.text:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "content_conflict",
                    "message": "operation key already used with different text",
                    "stored_receipt_id": existing["receipt_id"],
                },
            )
        if existing["held"]:
            _hold_event(body.op_key).wait()
        return {
            "receipt_id": existing["receipt_id"],
            "op_key": existing["op_key"],
            "printed_at": existing["printed_at"],
            "reused": True,
        }

    # New label: commit ledger + fault bookkeeping atomically *before* any
    # fault behavior, so a dropped response can always be recovered.
    receipt_id = "R-" + uuid.uuid4().hex[:12]
    now = time.time()
    with txn() as conn:
        race = conn.execute(
            "SELECT * FROM ledger WHERE op_key = ?", (body.op_key,)
        ).fetchone()
        if race is not None:
            # another concurrent print won the race; its entry is authoritative
            if race["text"] != body.text:
                raise HTTPException(status_code=409, detail={"error": "content_conflict"})
            row = race
        else:
            mode = fault["mode"] if fault is not None else None
            held = 1 if mode == "pause" else 0
            cur = conn.execute(
                """INSERT INTO ledger
                       (op_key, batch_id, seq, text, receipt_id, printed_at, held)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (body.op_key, body.batch_id, body.seq, body.text,
                 receipt_id, now, held),
            )
            if fault is not None and fault["op_key"] != body.op_key:
                # bind the pre-submit alias fault onto the concrete op_key
                conn.execute(
                    "UPDATE faults SET op_key = ? WHERE op_key = ?",
                    (body.op_key, fault["op_key"]),
                )
            if mode == "pause":
                conn.execute(
                    "UPDATE faults SET held = 1, consumed = 1 WHERE op_key = ?",
                    (body.op_key,),
                )
            elif mode == "drop_once":
                conn.execute(
                    "UPDATE faults SET consumed = 1 WHERE op_key = ?",
                    (body.op_key,),
                )
            row = conn.execute(
                "SELECT * FROM ledger WHERE id = ?", (cur.lastrowid,)
            ).fetchone()

    if row["held"]:
        # receipt exists in the ledger and is recoverable by key, but the
        # response itself is withheld until the operator releases the fault
        event = _hold_event(body.op_key)
        event.wait()
        with db() as conn:
            refreshed = conn.execute(
                "SELECT * FROM ledger WHERE op_key = ?", (body.op_key,)
            ).fetchone()
        return {
            "receipt_id": refreshed["receipt_id"],
            "op_key": refreshed["op_key"],
            "printed_at": refreshed["printed_at"],
            "reused": True,
        }

    if fault is not None and fault["mode"] == "drop_once":
        # first response after ledger commit is "lost on the network"
        time.sleep(DROP_DELAY)
        raise HTTPException(
            status_code=503,
            detail={
                "error": "response_lost",
                "message": "ledger committed but response dropped (drop_once)",
            },
        )

    return {
        "receipt_id": receipt_id,
        "op_key": body.op_key,
        "printed_at": now,
        "reused": False,
    }


@app.get("/receipts/{op_key:path}")
def get_receipt(op_key: str) -> dict:
    """Recovery endpoint: look up a receipt by the original operation key.
    423 while a pause is holding the receipt."""
    with db() as conn:
        row = conn.execute("SELECT * FROM ledger WHERE op_key = ?", (op_key,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "unknown_op_key"})
    if row["held"]:
        raise HTTPException(
            status_code=423,
            detail={
                "error": "receipt_held",
                "message": "pause fault still active",
                # ledger is truth even while the response is withheld: a
                # reconciliation may treat the label as physically printed
                "receipt_id": row["receipt_id"],
                "printed_at": row["printed_at"],
            },
        )
    return {
        "receipt_id": row["receipt_id"],
        "op_key": row["op_key"],
        "printed_at": row["printed_at"],
    }


@app.get("/ledger")
def get_ledger() -> dict:
    with db() as conn:
        rows = conn.execute("SELECT * FROM ledger ORDER BY id").fetchall()
        return {"entries": [dict(r) | {"held": bool(r["held"])} for r in rows]}


# ------------------------------------------------------------------ faults


@app.get("/faults")
def list_faults() -> dict:
    with db() as conn:
        rows = conn.execute("SELECT * FROM faults ORDER BY set_at DESC").fetchall()
        return {
            "faults": [
                dict(r) | {"held": bool(r["held"]), "consumed": bool(r["consumed"])}
                for r in rows
            ]
        }


@app.put("/faults")
def set_fault(body: FaultIn) -> dict:
    now = time.time()
    with txn() as conn:
        ledger_row = conn.execute(
            "SELECT * FROM ledger WHERE op_key = ?", (body.op_key,)
        ).fetchone()
        existing = conn.execute(
            "SELECT * FROM faults WHERE op_key = ?", (body.op_key,)
        ).fetchone()

        if ledger_row is not None:
            if ledger_row["held"]:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "fault_held", "message": "receipt currently held"},
                )
            if existing is None or (existing["consumed"] and not ledger_row["held"]):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "already_printed",
                        "message": "label already printed; fault cannot be armed",
                    },
                )

        conn.execute(
            """INSERT INTO faults (op_key, mode, held, consumed, set_at)
               VALUES (?, ?, 0, 0, ?)
               ON CONFLICT(op_key) DO UPDATE SET mode = excluded.mode,
                                                 held = 0, consumed = 0""",
            (body.op_key, body.mode, now),
        )
        return {"op_key": body.op_key, "mode": body.mode, "armed": True}


@app.post("/faults/{op_key:path}/release")
def release_fault(op_key: str) -> dict:
    with txn() as conn:
        fault = conn.execute("SELECT * FROM faults WHERE op_key = ?", (op_key,)).fetchone()
        if fault is None or fault["mode"] != "pause":
            raise HTTPException(
                status_code=404, detail={"error": "no_pause_fault"}
            )
        conn.execute(
            "UPDATE faults SET held = 0, consumed = 1 WHERE op_key = ?", (op_key,)
        )
        conn.execute("UPDATE ledger SET held = 0 WHERE op_key = ?", (op_key,))
    _release_event(op_key)
    return {"op_key": op_key, "released": True}


@app.delete("/faults/{op_key:path}")
def disarm_fault(op_key: str) -> dict:
    with txn() as conn:
        fault = conn.execute("SELECT * FROM faults WHERE op_key = ?", (op_key,)).fetchone()
        if fault is None:
            raise HTTPException(status_code=404, detail={"error": "not_found"})
        if fault["held"] or fault["consumed"] or _is_blocking(op_key):
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "fault_triggered",
                    "message": "release it instead (a print call is parked on it)",
                },
            )
        conn.execute("DELETE FROM faults WHERE op_key = ?", (op_key,))
    return {"op_key": op_key, "disarmed": True}
