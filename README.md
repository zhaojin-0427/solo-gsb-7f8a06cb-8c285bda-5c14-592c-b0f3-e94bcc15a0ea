# 本地标签打印排程系统

Python(FastAPI) + SQLite + 原生 JavaScript 的本地标签打印排程演示。
**不接真实打印机**：打印机是一个带“出纸账本”和故障开关的模拟器。

## 架构

| 组件 | 说明 | 持久化 |
| --- | --- | --- |
| `control`（控制端，FastAPI，端口 **8000**） | 保存不可变批次、派发标签、worker 租约、取消/核对事务、代理打印机、托管操作员网页 | 命名卷 `control-data`（SQLite） |
| `worker-a` / `worker-b`（两个独立 worker） | 并发领取批次；同批次严格按序号出纸；心跳续租；失联后凭原操作键找回回执 | 各自独立命名卷 `worker-a-data` / `worker-b-data` |
| `printer`（本地打印机模拟器） | 出纸账本（只追加 SQLite）、回执、`首次丢响应`/`暂停回执` 故障开关 | 命名卷 `printer-data` |

所有数据在 `docker compose down`（不加 `-v`）后保留；`docker compose down -v` 清空全部数据。

### 关键一致性规则

- **不可变批次 + 请求键去重**：控制端先整单落库再返回批次 ID；同键同内容返回原批次（`reused=true`），同键异内容 `409`。无效字符（非 0x20–0x7E 可打印 ASCII）、正文长度越界、重复序号、空批次 → 整单拒绝。
- **操作键**：`<批次ID>-<序号>`。打印机把“操作键 + 正文 + 回执”**原子**记入账本（SQLite `BEGIN IMMEDIATE` 下 check-and-insert，主键即操作键）：同键同内容只返回旧回执，不重复出纸；同键异内容 `409`。
- **按序出纸**：前一张回执未确认（状态不是已出纸/未出纸）前，控制端绝不派发同批次下一张；两个 worker 可以同时处理不同批次。
- **派发/取消按事务提交顺序裁决**：取消先提交 → 未派发的标签直接“未出纸”，不再派发；已派发到“待核对”的标签**保留待核对**，之后以打印机账本核对——账本有记录就是“已出纸”，绝不把已出纸说成已取消；无记录且取消在先才记“未出纸”。
- **失联/租约过期/重启恢复**：worker 打印响应未明（超时、模拟丢响应、暂停、控制端重启）时只按操作键 `GET /receipt/<op_key>` 找回回执；租约 30 秒，心跳每 8 秒，过期后另一个 worker 可凭同一操作键接管核对。账本是唯一事实来源，任何路径都不会重复出纸。

> 暂停回执开关有 10 分钟自动失效保护，避免忘记释放导致永久挂起；正常演练请在页面上点“释放”。
> 暂停期间回执**不会交付**（含按操作键查询/重放打印请求），标签保持“待核对”；释放后才送达，worker 随即确认。

标签状态：**待派发 / 待核对 / 已出纸 / 未出纸**。

## 启动（单条命令）

```bash
docker compose up --build
```

打开浏览器访问：

- 操作员页面： <http://localhost:8000/>
- 控制端 API 文档： <http://localhost:8000/docs>
- 打印机模拟器 API 文档： <http://localhost:8001/docs>（容器网络内；页面已代理，通常不用直连）

停止并保留数据：`docker compose down`；连数据一起清空：`docker compose down -v`。

## 页面用法

1. **① 提交批次**：填请求键 + 1～8 张标签（序号、1~80 字符的 ASCII 正文），提交后立刻得到 12 位批次 ID。
2. **② 批次与标签状态**：自动 2 秒刷新；展开“详情/核对”可见每张标签状态、操作键、持有 worker 和租约剩余；可手动“取消”或“查回执”。
3. **③ 打印机故障开关**：输入批次 ID（选中批次后自动带入）与序号，选择故障模式；暂停故障可在此点“释放”。
4. **④ 打印机出纸账本**：查看所有已记账出纸记录（操作键、正文、回执 ID、记账时间）及控制端事件流。

## 故障演练步骤

先用页面①提交一个请求键 `DRILL`、3～8 张标签的批次，复制批次 ID（记为 `BID`）。观察状态自动推进；每张标签都会在账本里出现一行。

### 演练 A：记账后首次丢失响应（lose_once）

1. 再提交一个新批次 `DRILL-LOSE`（标签数 ≥ 2），拿到 `BID2`；**在它开始出纸前**（worker 每 2 秒轮询，动作要快；也可以先停 worker：`docker compose stop worker-a worker-b`）。
2. 在页面③对 `BID2` 序号 `1` 点 **“打开：首次丢响应”**，然后 `docker compose start worker-a worker-b`。
3. 现象：打印机已把序号 1 原子记入账本，但第一次响应以 503/超时丢失；worker 日志显示“打印响应未明…以操作键找回回执”；标签短暂停留在**待核对**，随后凭 `BID2-1` 找回旧回执并变**已出纸**。
4. 在账本中确认 `BID2-1` **只有一行**（没有重复出纸），后续序号正常依次出纸。
   日志：`docker compose logs -f worker-a worker-b`。

### 演练 B：暂停回执直到释放（pause），并演示租约过期接管

1. 提交新批次 `DRILL-PAUSE`（≥ 2 张），拿到 `BID3`。
2. 为避免 worker 心跳一直续租导致看不到接管，可先停一个 worker：`docker compose stop worker-b`；对 `BID3` 序号 `1` 点 **“打开：暂停回执”**。
3. 现象：账本立刻出现 `BID3-1`（已记账），但回执被扣留，标签停在**待核对**；worker A 持续心跳等待（GET 回执长轮询）。
4. 等待约 **30 秒租约过期**后 `docker compose start worker-b`：worker B 接管该标签（事件流出现 `lease_taken_over`），同样以 `BID3-1` 查询回执——不会重复记账。
5. 在页面③点该故障的 **“释放”**：回执送达，拿到回执的 worker 把标签确认为**已出纸**，序号 2 才被派发。账本中 `BID3-1` 始终只有一行。

### 演练 C：取消与派发的提交顺序

1. 暂停 worker：`docker compose stop worker-a worker-b`。提交批次 `DRILL-CANCEL`（3 张）。
2. 直接点该批次的 **“取消”**（取消先提交）；再启动 worker：所有标签为**未出纸**，账本无该批次任何记录，不会被派发。
3. 再来一次：先启动 worker，再对进行中的批次点“取消”。
   - 取消提交时仍“待派发”的标签 → **未出纸**；
   - 已派发、回执未明（如正好叠加暂停故障）的标签保持**待核对**；释放/找回回执后，账本有记录的判为**已出纸**（不会把已出纸说成已取消），无记录且取消在先的判为**未出纸**；
   - 取消后同批次绝不再派发后续标签。

### 演练 D：控制端 / worker 重启恢复

1. 出纸进行中（可叠加演练 B 的暂停）执行 `docker compose restart control` 或 `docker compose restart worker-a`。
2. worker 重连后重新认领，在途标签仍以原操作键（如 `BID3-2`）找回回执继续；账本无重复行。
3. `docker compose down && docker compose up -d`（不带 `-v`）后，批次、租约状态、账本、worker 日志全部保留。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/batches` | 提交/幂等去重批次 |
| GET | `/api/batches` / `/api/batches/{id}` | 批次列表/明细（含每张标签状态） |
| POST | `/api/batches/{id}/cancel` | 取消（与派发按提交顺序裁决） |
| POST | `/api/worker/claim-batch` | worker 认领批次（首个标签原子进入待核对） |
| POST | `/api/worker/prepare-next` | 取在途标签/接管过期租约/派发下一张（强制按序） |
| POST | `/api/worker/heartbeat` | 租约续期；丢失返回 409 |
| POST | `/api/worker/confirm-printed` / `confirm-not-printed` | 以账本核对结果终结标签 |
| GET | `/api/printer/ledger` / `/api/printer/receipt/{op_key}` | 账本查询 / 按操作键找回回执 |
| POST | `/api/printer/faults` / `/faults/release`，GET `/api/printer/faults` | 故障开关 |
