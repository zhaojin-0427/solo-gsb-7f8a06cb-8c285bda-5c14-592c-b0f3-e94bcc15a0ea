"""Independent scheduler worker.

Persistence/recovery guarantees:
  * every dispatch is journaled in the worker's own SQLite DB, so a worker
    restart resumes any label it had leased;
  * the printer is idempotent by (op_key, text): before every (re)print the
    worker looks up the receipt by the original operation key, so a dropped
    response, an expired lease taken over by the other worker, or a service
    restart can never produce a second physical label;
  * control outages are retried forever; leases expiring while a response is
    paused simply get adopted (possibly by the other worker) and reconciled.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx
from urllib.parse import quote

WORKER_ID = os.environ.get("WORKER_ID", "worker")
CONTROL_URL = os.environ.get("CONTROL_URL", "http://localhost:8000").rstrip("/")
PRINTER_URL = os.environ.get("PRINTER_URL", "http://localhost:8001").rstrip("/")
WORKER_DB = os.environ.get("WORKER_DB", f"data/{WORKER_ID}/worker.db")
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "1.0"))
# grace window before the FIRST dispatch of a brand-new batch so operators
# can arm a pre-submit fault via the request-key alias; 0 disables
FAULT_GRACE = float(os.environ.get("FAULT_GRACE", "0"))
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "5.0"))
MAX_HTTP_ATTEMPTS = int(os.environ.get("MAX_HTTP_ATTEMPTS", "60"))


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {WORKER_ID}: {message}", flush=True)


# ---------------------------------------------------------------- journal


@contextmanager
def _conn() -> sqlite3.Connection:
    Path(WORKER_DB).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(WORKER_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_journal() -> None:
    with _conn() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS events (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   op_key TEXT,
                   event TEXT NOT NULL,
                   detail TEXT
               )"""
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS seen_ops (op_key TEXT PRIMARY KEY, ts REAL NOT NULL)"
        )


def journal(event: str, op_key: str | None = None, detail: str = "") -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO events (ts, op_key, event, detail) VALUES (?, ?, ?, ?)",
            (time.time(), op_key, event, detail[:500]),
        )
    log(f"{event} {op_key or ''} {detail}".rstrip())


# ---------------------------------------------------------------- runtime


async def wait_for_services(client: httpx.AsyncClient) -> None:
    for name, url in (("control", CONTROL_URL), ("printer", PRINTER_URL)):
        while True:
            try:
                resp = await client.get(f"{url}/healthz", timeout=3)
                if resp.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            log(f"waiting for {name} at {url} ...")
            await asyncio.sleep(1)


async def claim_work(client: httpx.AsyncClient) -> dict[str, Any] | None:
    try:
        resp = await client.post(
            f"{CONTROL_URL}/internal/work",
            params={"worker_id": WORKER_ID},
            timeout=HTTP_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        log(f"control unreachable while claiming: {exc}")
        return None
    if resp.status_code == 204:
        return None
    if resp.status_code != 200:
        log(f"claim failed HTTP {resp.status_code}: {resp.text[:200]}")
        return None
    return resp.json()["job"]


async def lookup_receipt(
    client: httpx.AsyncClient, op_key: str
) -> tuple[str, float, bool] | None:
    """Return (receipt_id, printed_at, held) or None if the printer ledger has
    no entry for this operation key. held=True means the pause fault is still
    withholding the response, yet the atomic ledger entry already exists."""
    try:
        resp = await client.get(
            f"{PRINTER_URL}/receipts/{quote(op_key, safe='')}", timeout=HTTP_TIMEOUT
        )
    except httpx.HTTPError:
        return None
    if resp.status_code == 200:
        data = resp.json()
        return data["receipt_id"], data["printed_at"], False
    if resp.status_code == 423:
        data = resp.json().get("detail", {})
        return data.get("receipt_id", ""), float(data.get("printed_at") or 0.0), True
    return None


async def ensure_printed(
    client: httpx.AsyncClient, job: dict[str, Any]
) -> tuple[str, float, bool] | None:
    """Idempotent print: ledger lookup first, then POST.

    Returns (receipt_id, printed_at, held); held=True means the receipt exists
    in the ledger but a pause fault is withholding it. Returns None while the
    outcome is genuinely unknown (lost response / outage) and must be retried.
    """
    op_key, text = job["op_key"], job["text"]

    found = await lookup_receipt(client, op_key)
    if found is not None:
        journal("receipt_recovered" if not found[2] else "receipt_held",
                op_key, found[0])
        return found

    try:
        resp = await client.post(
            f"{PRINTER_URL}/print",
            json={
                "op_key": op_key,
                "text": text,
                "batch_id": job["batch_id"],
                "request_key": job.get("request_key"),
                "seq": job["seq"],
            },
            timeout=HTTP_TIMEOUT,
        )
    except httpx.TimeoutException:
        # response may be lost or simply paused; the next loop recovers via
        # lookup_receipt using the same operation key — never reprints blindly
        journal("print_timeout", op_key, "will recover by op_key")
        return None
    except httpx.HTTPError as exc:
        journal("print_error", op_key, str(exc)[:200])
        return None

    if resp.status_code == 200:
        data = resp.json()
        journal(
            "receipt_recovered" if data.get("reused") else "printed",
            op_key,
            data["receipt_id"],
        )
        return data["receipt_id"], data["printed_at"], False

    if resp.status_code == 409:
        # content conflict is a hard programming-level error: park the label
        # in verification state for an operator rather than retry forever
        await _post_control(
            client,
            f"/internal/ops/{quote(op_key, safe='')}/error",
            None,
            {"detail": f"printer 409: {resp.text[:300]}"},
        )
        journal("print_conflict", op_key, resp.text[:200])
        return "CONFLICT", 0.0, False
    if resp.status_code == 503 and "response_lost" in resp.text:
        # drop_once: ledger is committed; recover the receipt by op_key
        journal("response_lost", op_key, "ledger committed, recovering")
        return None

    journal("print_http_error", op_key, f"HTTP {resp.status_code} {resp.text[:150]}")
    return None


async def _post_control(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, str] | None,
    json_body: dict[str, Any] | None,
) -> int | None:
    """POST to control, retrying through control outages. Returns the final
    HTTP status code, or None if control stayed unreachable."""
    for attempt in range(MAX_HTTP_ATTEMPTS):
        try:
            resp = await client.post(
                f"{CONTROL_URL}{path}",
                params=params,
                json=json_body,
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code < 500:
                return resp.status_code
        except httpx.HTTPError as exc:
            log(f"control post {path} unreachable: {exc}")
        await asyncio.sleep(1)
    return None


async def settle_cancelled(client: httpx.AsyncClient, job: dict[str, Any]) -> None:
    """Outcome for a label whose batch was cancelled. The printer ledger is
    the only truth: an entry exists (even while its receipt is held) ->
    printed, it must never be called cancelled; no entry -> absent once the
    lease has expired so the lease holder cannot still be printing."""
    op_key = job["op_key"]
    found = await lookup_receipt(client, op_key)
    if found is not None:
        status = await _post_control(
            client,
            f"/internal/ops/{quote(op_key, safe='')}/confirm",
            None,
            {"receipt_id": found[0], "printed_at": found[1] or time.time()},
        )
        if status == 200:
            journal("reconciled_printed", op_key,
                    f"{found[0]}{' (was held)' if found[2] else ''}")
        return

    status = await _post_control(
        client,
        f"/internal/ops/{quote(op_key, safe='')}/absent",
        {"worker_id": WORKER_ID},
        None,
    )
    if status == 200:
        journal("reconciled_absent", op_key, "ledger empty, never printed")
    else:
        # 409 lease_still_valid: the current holder may still be printing;
        # after lease expiry the label is adopted and retried
        log(f"{op_key}: absent pending lease expiry (status={status})")


async def handle_job(client: httpx.AsyncClient, job: dict[str, Any]) -> None:
    op_key = job["op_key"]
    if job["kind"] in ("recover", "adopt"):
        journal(f"lease_{job['kind']}", op_key, f"attempt={job['attempt']}")
    else:
        journal("dispatch", op_key, f"seq={job['seq']} batch={job['batch_id']}")
        if FAULT_GRACE and job["seq"] == 1:
            # one-time arming window for the batch's first label, so operators
            # can set a fault right after submitting (alias -> concrete key)
            with _conn() as conn:
                seen = conn.execute(
                    "SELECT 1 FROM seen_ops WHERE op_key = ?", (op_key,)
                ).fetchone()
                if seen is None:
                    conn.execute(
                        "INSERT INTO seen_ops (op_key, ts) VALUES (?, ?)",
                        (op_key, time.time()),
                    )
                    await asyncio.sleep(FAULT_GRACE)

    outcome = await ensure_printed(client, job)

    if outcome is None:
        # genuinely unknown (lost response / outage): keep this lease and
        # recover the same label by operation key on the next poll
        return

    receipt_id, printed_at, held = outcome
    if receipt_id == "CONFLICT":
        return

    if held:
        if job["batch_status"] == "cancelled":
            # ledger already proves physical printing despite the withheld
            # response: reconcile immediately as printed
            await settle_cancelled(client, job)
        # active batch: remain 'leased' (pending verification) and wait for
        # the operator to release; the next label stays blocked
        return

    status = await _post_control(
        client,
        f"/internal/ops/{quote(op_key, safe='')}/confirm",
        None,
        {"receipt_id": receipt_id, "printed_at": printed_at},
    )
    if status == 200:
        journal("confirmed", op_key, receipt_id)


async def run_worker() -> None:
    init_journal()
    journal("worker_start", detail=f"worker={WORKER_ID}")
    async with httpx.AsyncClient() as client:
        await wait_for_services(client)
        journal("services_ready")
        while True:
            job = await claim_work(client)
            if job is None:
                await asyncio.sleep(POLL_SECONDS)
                continue
            try:
                await handle_job(client, job)
            except Exception as exc:  # never die: always resume by op_key
                journal("unexpected_error", job.get("op_key"), repr(exc)[:300])
            await asyncio.sleep(0.2)
