# 本地标签打印排程（FastAPI + SQLite + 原生 JS）

一个不接真实打印机的本地标签排程演示：操作员提交不可变批次，两个独立 worker
并发领取、按序出纸，打印机模拟器记账并支持两类可操作故障；控制端失联、租约
过期、任意服务重启都凭**原操作键**找回回执，保证不重复出纸。

## 组件

| 服务 | 端口 | 数据（重启后保留） | 说明 |
|------|------|--------------------|------|
| `control`  | 8000 | `./data/control/control.db` | 批次/标签库、租约派发、取消事务、Web 页面 |
| `printer`  | 8001 | `./data/printer/printer.db` | 出纸账本（操作键唯一）、故障开关 |
| `worker-1` | — | `./data/worker-1/worker.db` | 独立 worker，各自 SQLite 事件日志 |
| `worker-2` | — | `./data/worker-2/worker.db` | 与 worker-1 完全对等，可并发领取 |

## 启动（单条命令）

```bash
docker compose up --build
```

打开页面：

- 操作页面：http://localhost:8000
- 打印机账本 JSON：http://localhost:8001/ledger
- 故障列表 JSON：http://localhost:8001/faults

数据保存在仓库下的 `./data/*/`，`docker compose down` 后再 `up` 仍然保留；
如需清空演示数据，删除 `./data` 目录（容器以 root 创建的文件可
`sudo rm -rf ./data`）。

## 规则速览

- 每批 1～8 张标签，顺序按序号固定；正文为 1～80 个可打印 ASCII（`0x20–0x7E`）。
- 无效字符、重复序号、空批次：**整单拒绝**（不写入任何数据）。
- 批次先整体落库（不可变）再返回 ID；相同请求键 + 相同内容返回**原批次**，
  相同请求键 + 不同内容返回 **409 冲突**。
- 操作键 = `批次ID#L序号`（如 `B-abc123#L2`）。打印机以 `(操作键, 正文)` 原子
  记入出纸账本后才给回执；同键同内容只返回旧回执，同键异内容 409。
- 两个 worker 可并发领不同批次；**同一批次严格按序**：前一张未确认 `printed`
  不派发下一张。
- 已派发而回执未明的标签为「待核对」（leased）。取消批次时：取消事务先提交则
  不再派发后续；待核对标签保留，随后按打印机账本核对——账本有记录即「已出纸」
  （绝不把已出纸说成已取消），无记录且租约过期才记「未出纸」。
- 租约 TTL 默认 12 秒（`LEASE_TTL`）。租约过期或 worker 重启后，标签可被另一个
  worker 接管，接管方先用原操作键查询打印机账本，绝不会重复打印。

## 故障演练步骤

页面每个标签行都有「丢响应 / 暂停」快捷按钮，也可在第 2 区手填操作键。

**布防时机**：故障必须在该标签出纸记账前设置。为方便演练，worker 对每个新批次
的第 1 张标签预留 2 秒布防窗口（`FAULT_GRACE`）。更稳妥的方式是用请求键别名
**在提交批次之前**预设故障：操作键填 `请求键#L序号`（如 `demo-2#L1`），首次出纸
时自动绑定到真实操作键。

### 演练 0：正常流程

1. 请求键填 `demo-1`，文本框留 3 行（`1|...`、`2|...`、`3|...`），提交。
2. 观察三张标签依次变为 **已出纸**（同批次严格按序）。
3. 「查询账本」可见 3 条原子记账记录；用相同请求键和内容再次提交，返回原批次。

### 演练 1：记账后首次丢失响应（drop_once）

1. 先在第 2 区操作键填 `demo-2#L1`，点「丢响应」（提交前布防，别名在首次
   出纸时绑定）。
2. 再提交请求键为 `demo-2` 的批次（2 张标签）。
3. 标签记账后首次响应丢失：账本立即出现该操作键，但标签停在「待核对」。
4. worker 用同一操作键找回回执，标签随后变为 **已出纸**，账本只有 1 条
   （没有重复出纸），后续标签继续按序派发。

### 演练 2：记账后暂停回执直到释放（pause）

1. 操作键填 `demo-3#L1` 点「暂停」，再提交请求键为 `demo-3` 的批次。
2. 标签出现在账本（`暂停中=是`），但回执被扣留，worker 保持「待核对」，
   **同批次下一张不会派发**。
3. 在第 2 区故障行点「释放回执」→ worker 凭原操作键拿到回执 → 已出纸，
   下一张才继续。

### 演练 3：取消与派发的事务顺序

1. 对一张标签设置「暂停」后提交批次，使其停在待核对。
2. 点「取消批次」：取消先在控制库提交 → 后续标签保持「待派发/未出纸」，
   不再派发。
3. 释放暂停回执 → 核对发现账本已有记录 → 该标签仍判 **已出纸**
   （不会被说成已取消）。
4. 对比：若标签尚未出纸（账本无记录）且租约已过期，则核对后记为 **未出纸**。

### 演练 4：控制端失联

1. `docker compose stop control`，观察 worker 日志持续重试等待。
2. `docker compose start control`：worker 凭原操作键继续，无重复出纸；
   批次与状态数据完好。

### 演练 5：worker 租约过期 / worker 重启

- 暂停一张标签的回执后重启持有它的 worker：
  `docker compose restart worker-1`。worker 用自己的 SQLite 日志恢复租约；
  若租约已过期，则由另一个 worker 接管，先查账本再决定，绝不重复出纸。

### 演练 6：打印机重启

1. `docker compose restart printer`：账本与「暂停中」状态从
   `./data/printer/printer.db` 恢复；被暂停的回执仍需点「释放」。

## 本地不用 Docker 的开发方式

```bash
pip install -r requirements.txt
CONTROL_DB=/tmp/ctl.db  LEASE_TTL=12 uvicorn app.control:app  --port 8000 &
PRINTER_DB=/tmp/prn.db  uvicorn app.printer:app --port 8001 &
WORKER_ID=worker-1 WORKER_DB=/tmp/w1.db python -m app.runner worker &
WORKER_ID=worker-2 WORKER_DB=/tmp/w2.db python -m app.runner worker &
```

## 关键 API

控制端（8000）

- `POST /api/batches` `{request_key, labels:[{seq,text}]}`
- `GET  /api/batches` / `GET /api/batches/{id}`
- `POST /api/batches/{id}/cancel`
- `POST /internal/work?worker_id=...`（worker 领取/恢复/接管，租约事务原子）
- `POST /internal/ops/{op_key}/confirm|absent|error`

打印机（8001）

- `POST /print` `{op_key,text,batch_id,seq}`（幂等：账本先提交）
- `GET  /receipts/{op_key}`（423=回执暂停中；找回回执专用）
- `GET  /ledger`
- `PUT  /faults` `{op_key,mode:"drop_once"|"pause"}`
- `POST /faults/{op_key}/release`、`DELETE /faults/{op_key}`
