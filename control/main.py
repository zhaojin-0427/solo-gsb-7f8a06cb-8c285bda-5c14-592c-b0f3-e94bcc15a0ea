"""Control service API.

Responsibilities:
  * save immutable batches (idempotent per request_key, 409 on conflict)
  * hand out labels to workers one at a time, in-batch strictly in seq
    order, with short leases (claim-batch / prepare-next / heartbeat)
  * record reconciliation outcomes against the printer ledger
    (printed / not_printed)
  * serialize cancel vs. dispatch: the first COMMIT wins
  * proxy the printer simulator (receipt lookup, ledger, fault switches)
  * serve the operator web page
"""

import os
import time
import uuid

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from db import LEASE_TTL, init_db, now_iso, query_all, query_one, tx
from models import BatchIn, content_hash, validate_labels

PRINTER_URL = os.environ.get("PRINTER_URL", "http://printer:8001")
WEB_DIR = os.environ.get("WEB_DIR", os.path.join(os.path.dirname(__file__), "..", "web"))

init_db()
app = FastAPI(title="label-print-control")

# ---------------------------------------------------------------- small I/O


def log_event(conn, kind, batch_id=None, label_id=None, worker_id=None, detail=None):
    conn.execute(
        "INSERT INTO events(at, batch_id, label_id, worker_id, kind, detail) "
        "VALUES(?,?,?,?,?,?)",
        (now_iso(), batch_id, label_id, worker_id, kind, detail),
    )


def row_to_label(r):
    return {
        "id": r["id"],
        "seq": r["seq"],
        "content": r["content"],
        "status": r["status"],
        "op_key": r["op_key"],
        "worker_id": r["worker_id"],
        "leased_until": r["leased_until"],
        "updated_at": r["updated_at"],
    }


def batch_status(b):
    """Derive a coarse batch status for the UI."""
    labels = query_all(
        "SELECT status FROM labels WHERE batch_id=? ORDER BY seq", (b["id"],)
    )
    statuses = [r["status"] for r in labels]
    if b["cancelled"]:
        if any(s == "printed" for s in statuses):
            return "partially_printed_cancelled"
        return "cancelled"
    if all(s == "printed" for s in statuses):
        return "printed"
    if any(s == "printed" for s in statuses):
        return "printing_partial"
    if any(s in ("checking",) for s in statuses):
        return "printing"
    if any(s == "not_printed" for s in statuses):
        # cannot happen without cancel, keep defensive
        return "partial"
    return "pending"


def serialize_batch(b, include_labels=False):
    out = dict(b)
    out["cancelled"] = bool(b["cancelled"])
    out["status"] = batch_status(b)
    now = time.time()
    lease_row = query_one(
        "SELECT worker_id, MAX(leased_until) AS until FROM labels "
        "WHERE batch_id=? AND status='checking'",
        (b["id"],),
    )
    out["active_worker"] = lease_row["worker_id"] if lease_row else None
    out["lease_expires_in"] = (
        round(lease_row["until"] - now, 1)
        if lease_row and lease_row["until"]
        else None
    )
    if include_labels:
        out["labels"] = [
            row_to_label(r)
            for r in query_all(
                "SELECT * FROM labels WHERE batch_id=? ORDER BY seq", (b["id"],)
            )
        ]
    else:
        counts = {s: 0 for s in ("pending", "checking", "printed", "not_printed")}
        for r in query_all(
            "SELECT status, COUNT(*) c FROM labels WHERE batch_id=? GROUP BY status",
            (b["id"],),
        ):
            counts[r["status"]] = r["c"]
        out["counts"] = counts
    return out


# ------------------------------------------------------------- operator API


@app.post("/api/batches", status_code=201)
def create_batch(body: BatchIn):
    key = body.request_key.strip()
    if not key:
        raise HTTPException(400, "request_key 不能为空")
    try:
        seq_content = validate_labels(body.labels)
    except ValueError as e:
        raise HTTPException(400, str(e))

    h = content_hash(seq_content)
    with tx() as conn:
        existing = conn.execute(
            "SELECT * FROM batches WHERE request_key=?", (key,)
        ).fetchone()
        if existing:
            if existing["content_hash"] == h:
                return JSONResponse(
                    status_code=200,
                    content={
                        "batch_id": existing["id"],
                        "reused": True,
                        "message": "同键同内容，返回原批次",
                    },
                )
            raise HTTPException(
                409, f"请求键 {key!r} 已用于不同内容的批次 {existing['id']}（冲突）"
            )

        batch_id = uuid.uuid4().hex[:12]
        conn.execute(
            "INSERT INTO batches(id, request_key, content_hash, label_count, "
            "cancelled, created_at) VALUES(?,?,?,?,0,?)",
            (batch_id, key, h, len(seq_content), now_iso()),
        )
        for seq, content in seq_content:
            conn.execute(
                "INSERT INTO labels(id, batch_id, seq, content, status, updated_at) "
                "VALUES(?,?,?,?,'pending',?)",
                (uuid.uuid4().hex, batch_id, seq, content, now_iso()),
            )
        log_event(conn, "batch_created", batch_id=batch_id, detail=key)

    b = query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    return JSONResponse(
        status_code=201,
        content={"batch_id": batch_id, "reused": False, "batch": serialize_batch(b)},
    )


@app.get("/api/batches")
def list_batches():
    rows = query_all("SELECT * FROM batches ORDER BY rowid DESC")
    return [serialize_batch(r) for r in rows]


@app.get("/api/batches/{batch_id}")
def get_batch(batch_id: str):
    b = query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not b:
        raise HTTPException(404, "批次不存在")
    return serialize_batch(b, include_labels=True)


class CancelIn(BaseModel):
    request_key: str | None = None


@app.post("/api/batches/{batch_id}/cancel")
def cancel_batch(batch_id: str, body: CancelIn | None = None):
    """Cancel commit vs dispatch commit: whoever COMMITs first wins.

    Labels already in 'checking' are LEFT as checking (待核对); the printer
    ledger decides later.  Labels still pending become not_printed.  If
    nothing was ever printed, the batch ends cancelled.
    """
    with tx() as conn:
        b = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not b:
            raise HTTPException(404, "批次不存在")
        if b["cancelled"]:
            return {"batch_id": batch_id, "already_cancelled": True}

        conn.execute(
            "UPDATE batches SET cancelled=1, cancelled_at=? WHERE id=?",
            (now_iso(), batch_id),
        )
        cur = conn.execute(
            "UPDATE labels SET status='not_printed', updated_at=? "
            "WHERE batch_id=? AND status='pending'",
            (now_iso(), batch_id),
        )
        log_event(
            conn,
            "batch_cancelled",
            batch_id=batch_id,
            detail=f"pending->not_printed:{cur.rowcount}",
        )
        # checking labels deliberately untouched: they stay 待核对
    return {"batch_id": batch_id, "cancelled": True}


@app.get("/api/events")
def get_events(limit: int = 200):
    limit = max(1, min(limit, 1000))
    return [
        dict(r)
        for r in query_all(
            "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
        )
    ]


# ---------------------------------------------------------------- worker API


class WorkerIn(BaseModel):
    worker_id: str = Field(..., min_length=1, max_length=100)


class PrepareIn(BaseModel):
    worker_id: str
    op_key: str | None = None  # pre-known op_key when resuming an old lease


class ConfirmIn(BaseModel):
    worker_id: str
    op_key: str


def _drop_finished_lease(conn, batch_id, worker_id):
    """Release the batch lease once nothing is left for this worker."""
    remaining = conn.execute(
        "SELECT 1 FROM labels WHERE batch_id=? AND status IN ('pending','checking') "
        "LIMIT 1",
        (batch_id,),
    ).fetchone()
    if not remaining:
        conn.execute(
            "DELETE FROM batch_leases WHERE batch_id=? AND worker_id=?",
            (batch_id, worker_id),
        )


@app.post("/api/worker/claim-batch", response_model_exclude_none=True)
def worker_claim_batch(body: WorkerIn):
    """Lease a whole batch (persisted in batch_leases) and atomically
    dispatch its first label. Survives control/worker restarts. The
    lease is kept alive via /heartbeat; a stale lease can be stolen."""
    now = time.time()
    with tx() as conn:
        # 1) I already have a LIVE in-flight label: resume that batch.
        mine = conn.execute(
            "SELECT batch_id FROM labels WHERE worker_id=? AND status='checking' "
            "AND leased_until > ? ORDER BY leased_until DESC LIMIT 1",
            (body.worker_id, now),
        ).fetchone()
        if mine:
            return {"batch_id": mine["batch_id"], "resumed": "in_flight"}

        # 2) expired in-flight label anywhere: take it over BEFORE any new
        #    claim so an out-of-order label can never be dispatched past it.
        expired = conn.execute(
            """
            SELECT l.* FROM labels l
            WHERE l.status='checking' AND l.leased_until <= ?
              AND NOT EXISTS (
                SELECT 1 FROM batch_leases bl
                WHERE bl.batch_id=l.batch_id AND bl.last_seen > ?
                  AND bl.worker_id <> ?)
            ORDER BY l.leased_until LIMIT 1
            """,
            (now, now, body.worker_id),
        ).fetchone()
        if expired:
            conn.execute(
                "UPDATE labels SET worker_id=?, leased_until=?, updated_at=? "
                "WHERE id=? AND status='checking'",
                (body.worker_id, now + LEASE_TTL, now_iso(), expired["id"]),
            )
            conn.execute(
                "INSERT INTO batch_leases(batch_id, worker_id, last_seen) "
                "VALUES(?,?,?) ON CONFLICT(batch_id) DO UPDATE SET "
                "worker_id=excluded.worker_id, last_seen=excluded.last_seen",
                (expired["batch_id"], body.worker_id, now),
            )
            log_event(
                conn, "lease_taken_over", batch_id=expired["batch_id"],
                label_id=expired["id"], worker_id=body.worker_id,
                detail=f"op_key={expired['op_key']} (via claim)",
            )
            r = conn.execute("SELECT * FROM labels WHERE id=?", (expired["id"],)).fetchone()
            return {
                "batch_id": expired["batch_id"],
                "label": row_to_label(r),
                "lease_ttl": LEASE_TTL,
                "taken_over": True,
            }

        # 2) I already hold a live lease with pending work.
        held = conn.execute(
            "SELECT bl.batch_id FROM batch_leases bl JOIN batches b ON b.id=bl.batch_id "
            "WHERE bl.worker_id=? AND bl.last_seen > ? AND b.cancelled=0 "
            "AND EXISTS (SELECT 1 FROM labels l WHERE l.batch_id=bl.batch_id "
            "AND l.status='pending') "
            "AND NOT EXISTS (SELECT 1 FROM labels l2 WHERE l2.batch_id=bl.batch_id "
            "AND l2.status='checking') "
            "ORDER BY b.rowid LIMIT 1",
            (body.worker_id, now),
        ).fetchone()
        if held:
            bid = held["batch_id"]
        else:
            # 3) steal an unleased / stale-leased eligible FIFO batch.
            #    ANY checking label — even an expired one — blocks new
            #    claims: the next worker must take over that label first.
            b = conn.execute(
                """
                SELECT b.* FROM batches b
                WHERE b.cancelled=0
                  AND EXISTS (SELECT 1 FROM labels l
                              WHERE l.batch_id=b.id AND l.status='pending')
                  AND NOT EXISTS (SELECT 1 FROM labels l2
                                  WHERE l2.batch_id=b.id
                                  AND l2.status='checking')
                  AND NOT EXISTS (SELECT 1 FROM batch_leases bl
                                  WHERE bl.batch_id=b.id AND bl.last_seen > ?)
                ORDER BY b.rowid LIMIT 1
                """,
                (now,),
            ).fetchone()
            if not b:
                return {"batch_id": None, "message": "暂无可领取批次"}
            bid = b["id"]
            conn.execute(
                "INSERT INTO batch_leases(batch_id, worker_id, last_seen) "
                "VALUES(?,?,?) ON CONFLICT(batch_id) DO UPDATE SET "
                "worker_id=excluded.worker_id, last_seen=excluded.last_seen",
                (bid, body.worker_id, now),
            )
            log_event(
                conn, "batch_leased", batch_id=bid, worker_id=body.worker_id
            )

        first = conn.execute(
            "SELECT * FROM labels WHERE batch_id=? AND status='pending' "
            "ORDER BY seq LIMIT 1",
            (bid,),
        ).fetchone()
        if not first:
            _drop_finished_lease(conn, bid, body.worker_id)
            return {"batch_id": None, "message": "暂无可领取批次"}
        op_key = f"{bid}-{first['seq']}"
        conn.execute(
            "UPDATE labels SET status='checking', op_key=?, worker_id=?, "
            "leased_until=?, updated_at=? WHERE id=? AND status='pending'",
            (op_key, body.worker_id, now + LEASE_TTL, now_iso(), first["id"]),
        )
        conn.execute(
            "UPDATE batch_leases SET last_seen=? WHERE batch_id=?", (now, bid)
        )
        log_event(
            conn,
            "label_dispatched",
            batch_id=bid,
            label_id=first["id"],
            worker_id=body.worker_id,
            detail=f"seq={first['seq']} op_key={op_key}",
        )
        return {
            "batch_id": bid,
            "label": row_to_label(
                conn.execute("SELECT * FROM labels WHERE id=?", (first["id"],)).fetchone()
            ),
            "lease_ttl": LEASE_TTL,
        }


@app.post("/api/worker/prepare-next")
def worker_prepare_next(body: PrepareIn):
    """Return the current in-flight label, take over an expired one, or
    dispatch the NEXT pending label — but only inside a batch this worker
    holds. Strict in-batch order is enforced by the NOT EXISTS checks."""
    now = time.time()
    with tx() as conn:
        # 1) my current in-flight (checking) label — check lease ownership
        checking = conn.execute(
            "SELECT l.*, b.cancelled AS b_cancelled FROM labels l "
            "JOIN batches b ON b.id=l.batch_id "
            "WHERE l.worker_id=? AND l.status='checking' "
            "ORDER BY l.leased_until DESC LIMIT 1",
            (body.worker_id,),
        ).fetchone()
        if checking:
            if checking["leased_until"] <= now:
                raise HTTPException(409, "租约已过期，标签可被他人接管")
            if body.op_key and body.op_key != checking["op_key"]:
                raise HTTPException(409, "op_key 与当前在途标签不符")
            return {
                "action": "in_flight",
                "batch_id": checking["batch_id"],
                "label": row_to_label(checking),
                "lease_expires_in": round(checking["leased_until"] - now, 1),
            }

        # 2) an expired (label lease <= now) checking label whose batch is
        #    not held by a live worker: take over label + batch lease
        expired = conn.execute(
            """
            SELECT l.* FROM labels l
            WHERE l.status='checking' AND l.leased_until <= ?
              AND NOT EXISTS (
                SELECT 1 FROM batch_leases bl
                WHERE bl.batch_id=l.batch_id AND bl.last_seen > ?
                  AND bl.worker_id <> ?)
            ORDER BY l.leased_until LIMIT 1
            """,
            (now, now, body.worker_id),
        ).fetchone()
        if expired:
            conn.execute(
                "UPDATE labels SET worker_id=?, leased_until=?, updated_at=? "
                "WHERE id=? AND status='checking'",
                (body.worker_id, now + LEASE_TTL, now_iso(), expired["id"]),
            )
            conn.execute(
                "INSERT INTO batch_leases(batch_id, worker_id, last_seen) "
                "VALUES(?,?,?) ON CONFLICT(batch_id) DO UPDATE SET "
                "worker_id=excluded.worker_id, last_seen=excluded.last_seen",
                (expired["batch_id"], body.worker_id, now),
            )
            log_event(
                conn,
                "lease_taken_over",
                batch_id=expired["batch_id"],
                label_id=expired["id"],
                worker_id=body.worker_id,
                detail=f"op_key={expired['op_key']}",
            )
            r = conn.execute("SELECT * FROM labels WHERE id=?", (expired["id"],)).fetchone()
            return {
                "action": "taken_over",
                "batch_id": expired["batch_id"],
                "label": row_to_label(r),
                "lease_expires_in": LEASE_TTL,
            }

        # 3) next pending label inside a batch this worker live-leases,
        #    only if no checking label remains in that batch
        pend = conn.execute(
            """
            SELECT l.* FROM labels l
            JOIN batch_leases bl ON bl.batch_id=l.batch_id
            JOIN batches b ON b.id=l.batch_id
            WHERE bl.worker_id=? AND bl.last_seen > ?
              AND l.status='pending' AND b.cancelled=0
              AND NOT EXISTS (
                SELECT 1 FROM labels l2
                WHERE l2.batch_id=l.batch_id AND l2.status='checking'
              )
            ORDER BY l.rowid, l.seq LIMIT 1
            """,
            (body.worker_id, now),
        ).fetchone()
        if not pend:
            # No dispatchable work held by me. Release any finished leases.
            for bl in conn.execute(
                "SELECT batch_id FROM batch_leases WHERE worker_id=?",
                (body.worker_id,),
            ).fetchall():
                _drop_finished_lease(conn, bl["batch_id"], body.worker_id)
            return {"action": "idle"}
        op_key = f"{pend['batch_id']}-{pend['seq']}"
        conn.execute(
            "UPDATE labels SET status='checking', op_key=?, worker_id=?, "
            "leased_until=?, updated_at=? WHERE id=? AND status='pending'",
            (op_key, body.worker_id, now + LEASE_TTL, now_iso(), pend["id"]),
        )
        conn.execute(
            "UPDATE batch_leases SET last_seen=? WHERE batch_id=? AND worker_id=?",
            (now, pend["batch_id"], body.worker_id),
        )
        log_event(
            conn,
            "label_dispatched",
            batch_id=pend["batch_id"],
            label_id=pend["id"],
            worker_id=body.worker_id,
            detail=f"seq={pend['seq']} op_key={op_key}",
        )
        r = conn.execute("SELECT * FROM labels WHERE id=?", (pend["id"],)).fetchone()
        return {
            "action": "dispatched",
            "batch_id": pend["batch_id"],
            "label": row_to_label(r),
            "lease_expires_in": LEASE_TTL,
        }


@app.post("/api/worker/heartbeat")
def worker_heartbeat(body: WorkerIn):
    now = time.time()
    with tx() as conn:
        rows = conn.execute(
            "SELECT * FROM labels WHERE worker_id=? AND status='checking'",
            (body.worker_id,),
        ).fetchall()
        lost = [r for r in rows if r["leased_until"] <= now]
        if lost:
            raise HTTPException(409, "租约已过期，停止该批处理并重新认领")
        for r in rows:
            conn.execute(
                "UPDATE labels SET leased_until=? WHERE id=?",
                (now + LEASE_TTL, r["id"]),
            )
        conn.execute(
            "UPDATE batch_leases SET last_seen=? WHERE worker_id=?",
            (now, body.worker_id),
        )
        held = conn.execute(
            "SELECT COUNT(*) c FROM batch_leases WHERE worker_id=?",
            (body.worker_id,),
        ).fetchone()["c"]
    return {"ok": True, "lease_ttl": LEASE_TTL, "held_labels": len(rows),
            "held_batches": held}


@app.post("/api/worker/confirm-printed")
def worker_confirm_printed(body: ConfirmIn):
    """Ledger says the label was printed: checking -> printed (terminal)."""
    with tx() as conn:
        r = conn.execute("SELECT * FROM labels WHERE op_key=?", (body.op_key,)).fetchone()
        if not r:
            raise HTTPException(404, "未知 op_key")
        if r["status"] in ("printed", "not_printed"):
            return {"op_key": body.op_key, "status": r["status"], "already": True}
        conn.execute(
            "UPDATE labels SET status='printed', updated_at=? WHERE id=?",
            (now_iso(), r["id"]),
        )
        log_event(
            conn,
            "label_printed",
            batch_id=r["batch_id"],
            label_id=r["id"],
            worker_id=body.worker_id,
            detail=f"op_key={body.op_key}",
        )
        _drop_finished_lease(conn, r["batch_id"], body.worker_id)
    return {"op_key": body.op_key, "status": "printed"}


@app.post("/api/worker/confirm-not-printed")
def worker_confirm_not_printed(body: ConfirmIn):
    """Ledger says no record exists. Only legal when the batch was
    cancelled (dispatch-vs-cancel ordering) or the label was never
    actually dispatched; otherwise the worker must retry printing."""
    with tx() as conn:
        r = conn.execute(
            "SELECT l.*, b.cancelled FROM labels l JOIN batches b ON b.id=l.batch_id "
            "WHERE l.op_key=?",
            (body.op_key,),
        ).fetchone()
        if not r:
            raise HTTPException(404, "未知 op_key")
        if r["status"] in ("printed", "not_printed"):
            return {"op_key": body.op_key, "status": r["status"], "already": True}
        if not r["cancelled"]:
            raise HTTPException(
                409, "批次未取消且账本无记录：应重试打印，不能判未出纸"
            )
        conn.execute(
            "UPDATE labels SET status='not_printed', updated_at=? WHERE id=?",
            (now_iso(), r["id"]),
        )
        log_event(
            conn,
            "label_not_printed",
            batch_id=r["batch_id"],
            label_id=r["id"],
            worker_id=body.worker_id,
            detail=f"op_key={body.op_key} (cancelled, ledger empty)",
        )
        _drop_finished_lease(conn, r["batch_id"], body.worker_id)
    return {"op_key": body.op_key, "status": "not_printed"}


# ----------------------------------------------------- printer proxy + fault


printer_client = httpx.Client(base_url=PRINTER_URL, timeout=httpx.Timeout(65.0))


def _proxy(method, path, json_body=None, timeout=None):
    try:
        resp = printer_client.request(method, path, json=json_body, timeout=timeout)
    except httpx.RequestError as e:
        raise HTTPException(502, f"打印机模拟器不可达: {e}")
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    return JSONResponse(status_code=resp.status_code, content=data)


@app.get("/api/printer/ledger")
def proxy_ledger(seq: str | None = None, batch_id: str | None = None):
    qs = []
    if seq:
        qs.append(f"seq={seq}")
    if batch_id:
        qs.append(f"batch_id={batch_id}")
    suffix = ("?" + "&".join(qs)) if qs else ""
    return _proxy("GET", "/ledger" + suffix)


@app.get("/api/printer/receipt/{op_key:path}")
def proxy_receipt(op_key: str, wait: bool = True):
    # Long poll so the UI can sit through a 暂停回执 fault until release.
    return _proxy(
        "GET", f"/receipt/{op_key}?wait={'true' if wait else 'false'}", timeout=65
    )


class FaultIn(BaseModel):
    batch_id: str
    seq: int
    mode: str = Field(..., pattern="^(lose_once|pause)$")


@app.post("/api/printer/faults")
def proxy_set_fault(body: FaultIn):
    return _proxy("POST", "/faults", json_body=body.model_dump())


class ReleaseIn(BaseModel):
    batch_id: str
    seq: int


@app.post("/api/printer/faults/release")
def proxy_release_fault(body: ReleaseIn):
    return _proxy("POST", "/faults/release", json_body=body.model_dump())


@app.get("/api/printer/faults")
def proxy_list_faults():
    return _proxy("GET", "/faults")


# ------------------------------------------------------------------ web UI

app.mount(
    "/static",
    StaticFiles(directory=os.path.abspath(WEB_DIR)),
    name="static",
)


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.abspath(WEB_DIR), "index.html"))
