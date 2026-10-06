"""Local printer simulator.

Contract with workers:
  * A label's paper-out record is appended to an append-only SQLite
    ledger atomically keyed by op_key ("<batch_id>-<seq>") together with
    the content. Same op_key + same content -> replay the old receipt.
    Same op_key + different content -> 409 conflict. Nothing is ever
    printed twice.
  * Fault switches (operator-operated from the UI):
      lose_once: the FIRST response after the ledger insert for the
                 specified label is dropped (worker times out); the
                 receipt is then recoverable by op_key.
      pause:     after the ledger insert, the receipt (and later GET
                 lookups) is held until the switch is released, then
                 delivered once.
  * No real printer is attached.
"""

import os
import sqlite3
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("PRINTER_DB", "/data/printer.db")
HOLD_TTL = 600  # pause switches expire after 10 min to avoid dead UI

_write = threading.Lock()
# (batch_id, seq) -> threading.Event, set when pause is released
_holds: dict[tuple, threading.Event] = {}
_holds_guard = threading.Lock()


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def connect():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    conn = connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ledger (
                op_key      TEXT PRIMARY KEY,
                batch_id    TEXT NOT NULL,
                seq         INTEGER NOT NULL,
                content     TEXT NOT NULL,
                receipt_id  TEXT NOT NULL,
                printed_at  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS faults (
                batch_id  TEXT NOT NULL,
                seq       INTEGER NOT NULL,
                mode      TEXT NOT NULL,   -- lose_once | pause
                created_at TEXT NOT NULL,
                expires_at REAL,
                fired     INTEGER NOT NULL DEFAULT 0,
                released  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(batch_id, seq, mode)
            );
            CREATE TABLE IF NOT EXISTS fault_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL, batch_id TEXT, seq INTEGER,
                kind TEXT NOT NULL, detail TEXT
            );
            """
        )
    finally:
        conn.close()


init_db()
app = FastAPI(title="printer-simulator")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# --------------------------------------------------------------- fault utils


def _hold_event(batch_id, seq):
    key = (batch_id, seq)
    with _holds_guard:
        ev = _holds.get(key)
        if ev is None:
            ev = threading.Event()
            _holds[key] = ev
    return ev


def _drop_hold(batch_id, seq):
    with _holds_guard:
        _holds.pop((batch_id, seq), None)


def _log(conn, kind, batch_id, seq, detail=None):
    conn.execute(
        "INSERT INTO fault_events(at, batch_id, seq, kind, detail) "
        "VALUES(?,?,?,?,?)",
        (now_iso(), batch_id, seq, kind, detail),
    )


def _active_pause(conn, batch_id, seq, now=None):
    now = now if now is not None else time.time()
    return conn.execute(
        "SELECT * FROM faults WHERE batch_id=? AND seq=? AND mode='pause' "
        "AND released=0 AND (expires_at IS NULL OR expires_at > ?)",
        (batch_id, seq, now),
    ).fetchone()


def _cleanup_expired(conn, now):
    rows = conn.execute(
        "SELECT batch_id, seq, mode FROM faults WHERE expires_at IS NOT NULL "
        "AND expires_at < ?",
        (now,),
    ).fetchall()
    for r in rows:
        conn.execute(
            "DELETE FROM faults WHERE batch_id=? AND seq=? AND mode=?",
            (r["batch_id"], r["seq"], r["mode"]),
        )
        if r["mode"] == "pause":
            _drop_hold(r["batch_id"], r["seq"])
        _log(conn, "fault_expired", r["batch_id"], r["seq"], r["mode"])


# ------------------------------------------------------------------- models


class PrintIn(BaseModel):
    op_key: str
    batch_id: str
    seq: int
    content: str


class FaultIn(BaseModel):
    batch_id: str
    seq: int
    mode: str = Field(..., pattern="^(lose_once|pause)$")


class ReleaseIn(BaseModel):
    batch_id: str
    seq: int


# -------------------------------------------------------------------- routes


@app.post("/print")
def print_label(body: PrintIn):
    """Atomic check-and-insert into the ledger, then apply fault behavior."""
    expected_op = f"{body.batch_id}-{body.seq}"
    if body.op_key != expected_op:
        raise HTTPException(400, "op_key 必须为 <batch_id>-<seq>")

    with _write:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _cleanup_expired(conn, time.time())

            row = conn.execute(
                "SELECT * FROM ledger WHERE op_key=?", (body.op_key,)
            ).fetchone()
            if row is not None:
                if row["content"] != body.content:
                    conn.execute("ROLLBACK")
                    raise HTTPException(
                        409,
                        f"操作键 {body.op_key} 已记账不同正文（打印机账本冲突）",
                    )
                receipt = {
                    "receipt_id": row["receipt_id"],
                    "op_key": row["op_key"],
                    "batch_id": row["batch_id"],
                    "seq": row["seq"],
                    "printed_at": row["printed_at"],
                    "replayed": True,
                    "message": "同键同内容：返回旧回执（不重复出纸）",
                }
                mode = "replay"
                lose = False
                pause = False
            else:
                receipt_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO ledger(op_key, batch_id, seq, content, "
                    "receipt_id, printed_at) VALUES(?,?,?,?,?,?)",
                    (
                        body.op_key,
                        body.batch_id,
                        body.seq,
                        body.content,
                        receipt_id,
                        now_iso(),
                    ),
                )
                # consume one-shot lose switch at insert time
                fr = conn.execute(
                    "SELECT * FROM faults WHERE batch_id=? AND seq=? AND mode='lose_once'",
                    (body.batch_id, body.seq),
                ).fetchone()
                lose = fr is not None
                if lose:
                    conn.execute(
                        "UPDATE faults SET fired=1 WHERE batch_id=? AND seq=? "
                        "AND mode='lose_once'",
                        (body.batch_id, body.seq),
                    )
                    conn.execute(
                        "DELETE FROM faults WHERE batch_id=? AND seq=? AND mode='lose_once'",
                        (body.batch_id, body.seq),
                    )
                fp = conn.execute(
                    "SELECT * FROM faults WHERE batch_id=? AND seq=? AND mode='pause'",
                    (body.batch_id, body.seq),
                ).fetchone()
                pause = fp is not None
                _log(
                    conn,
                    "ledger_insert",
                    body.batch_id,
                    body.seq,
                    f"op_key={body.op_key}"
                    + (" [lose_once armed]" if lose else "")
                    + (" [pause armed]" if pause else ""),
                )
                receipt = {
                    "receipt_id": receipt_id,
                    "op_key": body.op_key,
                    "batch_id": body.batch_id,
                    "seq": body.seq,
                    "printed_at": now_iso(),
                    "replayed": False,
                }
                mode = "new"
            conn.execute("COMMIT")
        finally:
            conn.close()

    # Fault effects happen AFTER the atomic commit, outside the write lock.
    # A pause switch withholds the receipt regardless of whether this call
    # inserted the row or is replaying an existing one: the paper is out
    # (ledger row exists) but the operator has not released the receipt.
    replay_paused = False
    if mode == "replay":
        rc = connect()
        try:
            replay_paused = _active_pause(rc, body.batch_id, body.seq) is not None
        finally:
            rc.close()
    if pause or replay_paused:
        ev = _hold_event(body.batch_id, body.seq)
        ev.wait(timeout=HOLD_TTL)
        receipt["delivered_after_pause"] = True

    if lose:
        # First response after bookkeeping is simply dropped.
        time.sleep(float(os.environ.get("FAULT_LOSE_DELAY", "3")))
        raise HTTPException(503, "SIMULATED: 记账后首次响应丢失（lose_once）")

    receipt["mode"] = mode
    return receipt


@app.get("/receipt/{op_key:path}")
def get_receipt(op_key: str, wait: bool = True):
    """Look up a previously written receipt by op_key (recovery path).

    Under an active pause switch for this label the lookup is held until
    release, matching the requirement that the receipt is delivered once
    the switch is released.
    """
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM ledger WHERE op_key=?", (op_key,)).fetchone()
        hold = None
        if wait:
            parts = op_key.rsplit("-", 1)
            if len(parts) == 2:
                bid, seq_s = parts
                try:
                    seq = int(seq_s)
                except ValueError:
                    seq = None
                if seq is not None and _active_pause(conn, bid, seq):
                    hold = (bid, seq)
    finally:
        conn.close()

    if hold is not None:
        # wait=true: receipt is withheld until the pause switch is released.
        ev = _hold_event(*hold)
        ev.wait(timeout=HOLD_TTL)
        conn = connect()
        try:
            row = conn.execute(
                "SELECT * FROM ledger WHERE op_key=?", (op_key,)
            ).fetchone()
        finally:
            conn.close()
    elif row is not None:
        # wait=false while a pause switch is armed: the receipt exists but
        # is NOT delivered; report the withheld state without leaking it.
        parts = op_key.rsplit("-", 1)
        if len(parts) == 2:
            try:
                seq = int(parts[1])
            except ValueError:
                seq = None
            if seq is not None:
                conn = connect()
                try:
                    paused_now = _active_pause(conn, parts[0], seq)
                finally:
                    conn.close()
                if paused_now is not None:
                    return {
                        "paused": True,
                        "withheld": True,
                        "op_key": op_key,
                        "batch_id": parts[0],
                        "seq": seq,
                        "ledgered": True,
                    }

    if row is None:
        raise HTTPException(404, "账本中无此操作键记录（尚未出纸）")
    return {
        "receipt_id": row["receipt_id"],
        "op_key": row["op_key"],
        "batch_id": row["batch_id"],
        "seq": row["seq"],
        "printed_at": row["printed_at"],
        "replayed": True,
        "mode": "recovery",
    }


@app.get("/ledger")
def get_ledger(batch_id: str | None = None, seq: int | None = None):
    sql = "SELECT * FROM ledger"
    where, args = [], []
    if batch_id:
        where.append("batch_id=?")
        args.append(batch_id)
    if seq is not None:
        where.append("seq=?")
        args.append(seq)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY rowid"
    conn = connect()
    try:
        rows = conn.execute(sql, args).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/fault-events")
def get_fault_events(limit: int = 200):
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM fault_events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.post("/faults")
def set_fault(body: FaultIn):
    with _write:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            _cleanup_expired(conn, time.time())
            existing = conn.execute(
                "SELECT * FROM ledger WHERE batch_id=? AND seq=?",
                (body.batch_id, body.seq),
            ).fetchone()
            if existing and body.mode == "lose_once":
                conn.execute("ROLLBACK")
                raise HTTPException(
                    409, "该标签已记账，lose_once 只能在出纸前打开（请在派发前设置）"
                )
            expires = time.time() + HOLD_TTL if body.mode == "pause" else None
            conn.execute(
                "INSERT INTO faults(batch_id, seq, mode, created_at, expires_at, "
                "fired, released) VALUES(?,?,?,?,?,0,0) "
                "ON CONFLICT(batch_id, seq, mode) DO UPDATE SET "
                "created_at=excluded.created_at, expires_at=excluded.expires_at, "
                "released=0, fired=fired",
                (body.batch_id, body.seq, body.mode, now_iso(), expires),
            )
            _log(conn, "fault_set", body.batch_id, body.seq, body.mode)
            if body.mode == "pause":
                _hold_event(body.batch_id, body.seq).clear()
            conn.execute("COMMIT")
        finally:
            conn.close()
    return {"ok": True, "batch_id": body.batch_id, "seq": body.seq, "mode": body.mode}


@app.post("/faults/release")
def release_fault(body: ReleaseIn):
    with _write:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "UPDATE faults SET released=1 WHERE batch_id=? AND seq=? "
                "AND mode='pause' AND released=0",
                (body.batch_id, body.seq),
            )
            if cur.rowcount:
                conn.execute(
                    "DELETE FROM faults WHERE batch_id=? AND seq=? AND mode='pause'",
                    (body.batch_id, body.seq),
                )
                _log(conn, "fault_released", body.batch_id, body.seq, "pause")
            conn.execute("COMMIT")
        finally:
            conn.close()
    _hold_event(body.batch_id, body.seq).set()
    _drop_hold(body.batch_id, body.seq)
    return {"ok": True, "released": bool(cur.rowcount)}


@app.get("/faults")
def list_faults():
    conn = connect()
    try:
        rows = conn.execute(
            "SELECT * FROM faults ORDER BY rowid"
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["active"] = (
                (r["expires_at"] is None or r["expires_at"] > time.time())
                and not r["released"]
            )
            out.append(d)
        return out
    finally:
        conn.close()


@app.get("/health")
def health():
    return {"ok": True}
