"""Independent print worker.

Loop:
  1. claim a batch from control (or resume its own in-flight batch)
  2. prepare-next gives one label at a time; in-batch order is enforced by
     control (next seq is never dispatched while the previous receipt is
     unresolved)
  3. keep the lease alive with heartbeats while talking to the printer
  4. POST /print; on timeout/lost response do NOT print again blindly —
     GET the receipt by op_key (idempotent recovery)
  5. reconcile against the ledger: printed -> confirm-printed; if the
     batch got cancelled first and the ledger has no row ->
     confirm-not-printed; otherwise retry
  6. lease lost / control restart -> drop current state, re-claim, recover
     receipts by the original op_key. The printer ledger guarantees no
     duplicate paper output.

Local SQLite journal persists across worker restarts (audit/recovery).
"""

import json
import os
import signal
import sqlite3
import threading
import time

import httpx

CONTROL = os.environ.get("CONTROL_URL", "http://control:8000")
PRINTER = os.environ.get("PRINTER_URL", "http://printer:8001")
WORKER_ID = os.environ.get("WORKER_ID", "worker")
DB_PATH = os.environ.get("WORKER_DB", "/data/worker.db")

POLL = float(os.environ.get("WORKER_POLL", "2"))
PRINT_TIMEOUT = float(os.environ.get("PRINT_TIMEOUT", "12"))
HEARTBEAT_EVERY = float(os.environ.get("HEARTBEAT_EVERY", "8"))

_stop = threading.Event()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}][{WORKER_ID}] {msg}", flush=True)


# ---------------------------------------------------------------- local db


def db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS journal (
            op_key     TEXT PRIMARY KEY,
            batch_id   TEXT,
            seq        INTEGER,
            receipt_id TEXT,
            state      TEXT,
            at         TEXT
        )
        """
    )
    return c


def journal(conn, op_key, state, batch_id=None, seq=None, receipt_id=None):
    conn.execute(
        "INSERT INTO journal(op_key,batch_id,seq,receipt_id,state,at) "
        "VALUES(?,?,?,?,?,strftime('%Y-%m-%dT%H:%M:%SZ','now')) "
        "ON CONFLICT(op_key) DO UPDATE SET state=excluded.state, "
        "receipt_id=COALESCE(excluded.receipt_id, journal.receipt_id), "
        "at=excluded.at",
        (op_key, batch_id, seq, receipt_id, state),
    )


# ------------------------------------------------------------- HTTP helpers

control = httpx.Client(base_url=CONTROL, timeout=10.0)
printer = httpx.Client(base_url=PRINTER, timeout=10.0)


def call(client, method, path, json_body=None, timeout=None):
    """Returns (status, data). Network errors -> (599, {...})."""
    try:
        r = client.request(method, path, json=json_body, timeout=timeout)
        try:
            data = r.json()
        except Exception:
            data = {"detail": r.text}
        return r.status_code, data
    except httpx.RequestError as e:
        return 599, {"detail": f"network: {e}"}


# ------------------------------------------------------------------ worker


def heartbeat_thread(stop_evt):
    while not stop_evt.wait(HEARTBEAT_EVERY):
        st, data = call(control, "POST", "/api/worker/heartbeat",
                        {"worker_id": WORKER_ID})
        if st == 409:
            log(f"heartbeat: {data.get('detail')} — 放弃当前批次，重新认领")
            stop_evt.set()
        elif st >= 500:
            log(f"heartbeat 控制端暂时不可达: {data.get('detail')}")
        # 200 -> lease renewed


def reconcile_receipt(op_key, wait=True):
    """Fetch the receipt by op_key. A non-waiting probe returns immediately
    (used BEFORE the first print attempt); after an unknown response we
    long-poll. The long poll is itself a sequence of short requests so a
    SIGTERM is honoured promptly even while the printer withholds the
    receipt behind a 'pause' fault switch.
    Returns (found: bool, receipt|None, status)."""
    if not wait:
        st, data = call(
            printer, "GET", f"/receipt/{op_key}?wait=false", timeout=10.0
        )
        if st == 200:
            if data.get("withheld"):
                # pause switch active: ledger row exists but receipt is not
                # delivered — fall through to the waiting loop below
                pass
            else:
                return True, data, st
        else:
            return False, None, st

    while not _stop.is_set():
        st, data = call(
            printer, "GET", f"/receipt/{op_key}?wait=true", timeout=12.0
        )
        if st == 200:
            if data.get("withheld"):
                if _stop.wait(1):
                    break
                continue
            return True, data, st
        if st == 404:
            return False, None, st
        # timeout / 5xx: keep waiting (printer may be holding under pause),
        # but the short request window lets shutdown/lease checks run.
        if _stop.wait(1):
            break
    return False, None, 599


def process_label(label, batch_id, conn):
    op_key = label["op_key"] or f"{batch_id}-{label['seq']}"
    content = label["content"]
    seq = label["seq"]
    log(f"处理 {op_key} (seq={seq})")
    journal(conn, op_key, "processing", batch_id, seq)

    # 1) Non-blocking ledger probe: an earlier attempt may already have
    #    inserted the row (lost response / worker restart / control
    #    restart). wait=false so an armed "pause" switch cannot hold us
    #    BEFORE the ledger insert — the paper-out must be recorded first.
    found, receipt, _ = reconcile_receipt(op_key, wait=False)
    if found:
        log(f"账本已有回执 {receipt['receipt_id'][:8]}，直接确认（不重复出纸）")
        return finish("printed", op_key, receipt["receipt_id"], conn)

    # 2) Ask the printer to print (atomic check-and-insert on its side).
    st, data = call(
        printer,
        "POST",
        "/print",
        {"op_key": op_key, "batch_id": batch_id, "seq": seq, "content": content},
        timeout=PRINT_TIMEOUT,
    )

    if st == 200:
        rid = data.get("receipt_id")
        replay = data.get("replayed")
        log(f"打印机回执 {rid[:8]}{'（旧回执）' if replay else '（已记账出纸）'}")
        return finish("printed", op_key, rid, conn)

    if st == 409:
        # Same op_key, different content must never happen: op_key is
        # derived from immutable batch id + seq. Stop and surface it.
        log(f"!! 打印机内容冲突: {data.get('detail')}；跳过，等待人工核查")
        return "conflict"

    if 400 <= st < 500:
        # A 4xx other than 409 is a protocol/state bug, not a transient
        # outage. Back off and surface it; never busy-loop.
        log(f"!! 打印机拒绝请求 ({st}): {data.get('detail')}；等待 5s 后重试")
        _stop.wait(5)
        return "retry"

    # timeout(599) / simulated 503 lose_once / 5xx / pause that outlasted
    # our timeout -> response status unknown: recover by op_key.
    log(f"打印响应未明 ({st} {data.get('detail','')})，以操作键找回回执…")
    found, receipt, rstatus = reconcile_receipt(op_key)
    if found:
        log(f"找回回执 {receipt['receipt_id'][:8]}，确认已出纸")
        return finish("printed", op_key, receipt["receipt_id"], conn)

    if rstatus != 404:
        log("找回回执时遇到临时错误，稍后重试")
        return "retry"

    # Ledger says no row. Two cases: the row genuinely never got inserted,
    # or cancel committed before dispatch. Ask control to decide.
    log("账本无记录，与控制库核对取消/派发顺序…")
    st2, d2 = call(
        control, "POST", "/api/worker/confirm-not-printed",
        {"worker_id": WORKER_ID, "op_key": op_key},
    )
    if st2 == 200:
        log(f"{op_key} 判定未出纸（取消先于派发/记账）")
        journal(conn, op_key, "not_printed", batch_id, seq)
        return "not_printed"
    if st2 == 409:
        # batch still live: we must retry printing (no row was inserted)
        log("批次未取消，重试打印")
        return "retry"
    log(f"核对未出纸失败({st2})，稍后重试: {d2.get('detail')}")
    return "retry"


def finish(outcome, op_key, receipt_id, conn):
    endpoint = "/api/worker/confirm-printed"
    st, data = call(
        control, "POST", endpoint,
        {"worker_id": WORKER_ID, "op_key": op_key},
    )
    if st == 200:
        journal(conn, op_key, outcome, receipt_id=receipt_id)
        return outcome
    log(f"确认{outcome}被拒({st}): {data.get('detail')}，稍后重试")
    return "retry"


def run_batch(batch_id, first_label, conn):
    hb_stop = threading.Event()
    hb = threading.Thread(target=heartbeat_thread, args=(hb_stop,), daemon=True)
    hb.start()
    try:
        label = first_label
        while not _stop.is_set() and not hb_stop.is_set():
            result = process_label(label, batch_id, conn)
            if result == "conflict":
                return  # do not touch anything; operator inspects ledger
            if hb_stop.is_set():
                log("租约已丢失，退出本批（其他 worker 可用同一操作键接管）")
                return
            if result == "retry":
                if _stop.wait(POLL):
                    return
                # refresh label state, then loop without dispatching next
                st, data = call(
                    control, "POST", "/api/worker/prepare-next",
                    {"worker_id": WORKER_ID, "op_key": label["op_key"]},
                )
                if st == 200 and data.get("action") == "in_flight":
                    label = data["label"]
                    continue
                return  # lease lost / cancelled etc -> outer re-claim

            # label terminal: ask for the next one (control enforces order)
            st, data = call(
                control, "POST", "/api/worker/prepare-next",
                {"worker_id": WORKER_ID},
            )
            if st != 200:
                log(f"prepare-next {st}: {data.get('detail')}，退回认领")
                return
            action = data.get("action")
            if action == "idle":
                log(f"批次 {batch_id} 处理完毕")
                return
            if action == "in_flight":
                label = data["label"]
            else:  # dispatched or taken_over
                label = data["label"]
                log(f"下一张: seq={label['seq']} op_key={label['op_key']} ({action})")
    finally:
        hb_stop.set()
        hb.join(timeout=2)


def main():
    signal.signal(signal.SIGTERM, lambda *_: _stop.set())
    signal.signal(signal.SIGINT, lambda *_: _stop.set())
    conn = db()
    log(f"启动；control={CONTROL} printer={PRINTER}")

    while not _stop.is_set():
        st, data = call(control, "POST", "/api/worker/claim-batch",
                        {"worker_id": WORKER_ID})
        if st == 200 and data.get("batch_id"):
            bid = data["batch_id"]
            if data.get("resumed"):
                log(f"重新认领在途批次 {bid}，准备以原操作键找回回执")
                st2, d2 = call(
                    control, "POST", "/api/worker/prepare-next",
                    {"worker_id": WORKER_ID},
                )
                if st2 != 200 or d2.get("action") in (None, "idle"):
                    continue
                run_batch(bid, d2["label"], conn)
            else:
                if data.get("taken_over"):
                    log(f"接管过期在途标签 {bid} seq={data['label']['seq']}，"
                        f"以原操作键 {data['label']['op_key']} 核对")
                else:
                    log(f"领取批次 {bid}，首张 seq={data['label']['seq']}")
                run_batch(bid, data["label"], conn)
        elif st >= 500 or st == 599:
            log(f"控制端不可达 ({st})，{POLL}s 后重试，恢复后凭操作键继续")
        _stop.wait(POLL)
    log("退出")


if __name__ == "__main__":
    main()
