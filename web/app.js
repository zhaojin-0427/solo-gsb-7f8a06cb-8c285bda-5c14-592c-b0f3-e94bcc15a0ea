/* 操作员控制台 —— 原生 JavaScript，无任何框架/构建步骤 */

const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];

const BATCH_STATUS_TEXT = {
  pending: "待派发",
  printing: "出纸中（有待核对）",
  printing_partial: "出纸中（部分已出纸）",
  printed: "全部已出纸",
  cancelled: "已取消（均未出纸）",
  partially_printed_cancelled: "已取消（部分已出纸）",
  partial: "部分未出纸",
};
const LABEL_STATUS_TEXT = {
  pending: "待派发",
  checking: "待核对",
  printed: "已出纸",
  not_printed: "未出纸",
};

const api = async (path, opts = {}) => {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw Object.assign(new Error(data.detail || `HTTP ${resp.status}`), { data });
  return data;
};

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const fmtTime = (s) => (s ? s.replace("T", " ").replace("Z", " UTC") : "");

// ------------------------------------------------------------ ① batch form

const rowsBox = $("#label-rows");

function addRow(seq) {
  const n = rowsBox.children.length;
  const seqVal = seq ?? n + 1;
  const div = document.createElement("div");
  div.className = "label-row";
  div.innerHTML = `
    <input type="number" class="lr-seq" min="1" value="${seqVal}" placeholder="序号">
    <input type="text" class="lr-content" maxlength="80" placeholder="标签正文（1~80 个可打印 ASCII 字符）">
    <span class="len">0/80</span>`;
  const content = $(".lr-content", div);
  const len = $(".len", div);
  content.addEventListener("input", () => {
    len.textContent = `${content.value.length}/80`;
    len.classList.toggle("over", content.value.length > 80);
  });
  rowsBox.appendChild(div);
}
addRow(1);

$("#add-row").addEventListener("click", () => {
  if (rowsBox.children.length >= 8) return;
  addRow();
});
$("#del-row").addEventListener("click", () => {
  if (rowsBox.children.length > 1) rowsBox.lastChild.remove();
});

$("#batch-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const out = $("#submit-result");
  out.className = "result";
  out.textContent = "提交中…";
  const labels = $$(".label-row", rowsBox).map((row) => ({
    seq: Number($(".lr-seq", row).value),
    content: $(".lr-content", row).value,
  }));
  try {
    const r = await api("/api/batches", {
      method: "POST",
      body: { request_key: $("#request-key").value, labels },
    });
    out.className = "result ok";
    out.innerHTML = r.reused
      ? `同键同内容，复用原批次 <code>${r.batch_id}</code>`
      : `已保存不可变批次 <code>${r.batch_id}</code>`;
    selectedBatch = r.batch_id;
    await refreshBatches();
  } catch (err) {
    out.className = "result err";
    out.textContent = "整单拒绝：" + err.message;
  }
});

// ------------------------------------------------------------ ② batch list

let selectedBatch = null;

async function refreshBatches() {
  let batches;
  try {
    batches = await api("/api/batches");
  } catch (e) {
    return;
  }
  const tbody = $("#batch-table tbody");
  tbody.innerHTML = batches.map((b) => {
    const c = b.counts || {};
    const lease = b.active_worker
      ? `${esc(b.active_worker)}（${b.lease_expires_in ?? "?"}s）`
      : "—";
    return `
    <tr data-id="${b.id}" class="${b.id === selectedBatch ? "selected" : ""}">
      <td><code>${b.id}</code>${b.cancelled ? ' <span class="badge badge-cancelled">取消</span>' : ""}</td>
      <td>${esc(b.request_key)}</td>
      <td><span class="badge batch-${b.status}">${BATCH_STATUS_TEXT[b.status] || b.status}</span></td>
      <td>${c.pending || 0}</td>
      <td>${c.checking || 0}</td>
      <td>${c.printed || 0}</td>
      <td>${c.not_printed || 0}</td>
      <td>${lease}</td>
      <td>${fmtTime(b.created_at)}</td>
      <td>
        <button class="tiny" data-act="detail">详情/核对</button>
        <button class="tiny danger" data-act="cancel">取消</button>
      </td>
    </tr>`;
  }).join("");

  $$("button[data-act]", tbody).forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      const id = e.target.closest("tr").dataset.id;
      selectedBatch = id;
      if (btn.dataset.act === "detail") {
        await openDetail(id);
      } else {
        if (confirm(`取消批次 ${id}？\n取消先提交则不再派发；已派发而回执未明的标签保留“待核对”，核对后以打印机账本为准。`)) {
          try {
            await api(`/api/batches/${id}/cancel`, { method: "POST", body: {} });
          } catch (err) {
            alert(err.message);
          }
          await refreshBatches();
          if ($("#batch-detail").dataset.id === id) await openDetail(id);
        }
      }
    });
  });

  if (selectedBatch) await openDetail(selectedBatch, true);
}

$("#refresh-btn").addEventListener("click", refreshBatches);

async function openDetail(id, silent) {
  let b;
  try {
    b = await api(`/api/batches/${id}`);
  } catch (e) {
    if (!silent) alert(e.message);
    selectedBatch = null;
    $("#batch-detail").classList.add("hidden");
    return;
  }
  $("#batch-detail").classList.remove("hidden");
  $("#batch-detail").dataset.id = id;
  $("#fault-batch").value = id;
  $("#batch-detail").innerHTML = `
    <h3>批次 <code>${b.id}</code> 的标签明细
      <span class="badge batch-${b.status}">${BATCH_STATUS_TEXT[b.status]}</span></h3>
    <table>
      <thead><tr><th>序号</th><th>正文</th><th>状态</th><th>操作键</th>
      <th>worker</th><th>租约剩余</th><th>更新时间</th><th>核对回执</th></tr></thead>
      <tbody>${b.labels.map((l) => {
        const mine = l.op_key === `${b.id}-${l.seq}`;
        return `<tr>
          <td>${l.seq}</td>
          <td>${esc(l.content)}</td>
          <td><span class="badge ${l.status}">${LABEL_STATUS_TEXT[l.status]}</span></td>
          <td><code>${esc(l.op_key || "—")}</code>${mine ? "" : ""}</td>
          <td>${esc(l.worker_id || "—")}</td>
          <td>${l.leased_until ? Math.max(0, Math.round(l.leased_until - Date.now() / 1000)) + "s" : "—"}</td>
          <td>${fmtTime(l.updated_at)}</td>
          <td>${l.op_key ? `<button class="tiny" data-receipt="${esc(l.op_key)}">查回执</button>` : "—"}</td>
        </tr>`;
      }).join("")}</tbody>
    </table>`;
  $$("button[data-receipt]", $("#batch-detail")).forEach((btn) =>
    btn.addEventListener("click", async () => {
      try {
        const r = await api(`/api/printer/receipt/${encodeURIComponent(btn.dataset.receipt)}?wait=false`);
        alert(`回执已存在：\nreceipt_id: ${r.receipt_id}\n记账时间: ${r.printed_at}`);
      } catch (e) {
        alert("账本无记录（尚未出纸）：" + e.message);
      }
    })
  );
}

// ------------------------------------------------------------ ③ faults

$("#fault-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const mode = e.submitter.value;
  try {
    await api("/api/printer/faults", {
      method: "POST",
      body: {
        batch_id: $("#fault-batch").value.trim(),
        seq: Number($("#fault-seq").value),
        mode,
      },
    });
  } catch (err) {
    alert(err.message);
  }
  refreshFaults();
});

async function refreshFaults() {
  let faults = [];
  try {
    faults = await api("/api/printer/faults");
  } catch (e) { /* printer down */ }
  $("#fault-list").innerHTML = faults.length
    ? faults.map((f) => `
      <div class="fault-item">
        <span>
          <b>${f.mode === "lose_once" ? "首次丢响应（一次性）" : "暂停回执"}</b>
          批次 <code>${esc(f.batch_id)}</code> 序号 ${f.seq}
          ${f.fired ? "· 已触发" : "· 待触发"}
          ${f.active === false ? "· 已失效" : ""}
        </span>
        ${f.mode === "pause" ? `<button class="tiny" data-release-b="${esc(f.batch_id)}" data-release-s="${f.seq}">释放</button>` : ""}
      </div>`).join("")
    : `<span class="hint">当前没有故障开关。</span>`;
  $$("button[data-release-b]", $("#fault-list")).forEach((btn) =>
    btn.addEventListener("click", async () => {
      await api("/api/printer/faults/release", {
        method: "POST",
        body: { batch_id: btn.dataset.releaseB, seq: Number(btn.dataset.releaseS) },
      });
      refreshFaults();
    })
  );
}

// ------------------------------------------------------------ ④ ledger

async function refreshLedger() {
  const rows = await api("/api/printer/ledger");
  $("#ledger-table tbody").innerHTML = rows.length
    ? rows.map((r) => `<tr>
        <td><code>${esc(r.op_key)}</code></td>
        <td>${esc(r.batch_id)}</td>
        <td>${r.seq}</td>
        <td>${esc(r.content)}</td>
        <td><code>${esc(r.receipt_id.slice(0, 12))}</code></td>
        <td>${fmtTime(r.printed_at)}</td>
      </tr>`).join("")
    : `<tr><td colspan="6" class="hint">账本为空。</td></tr>`;

  const evs = await api("/api/events").catch(() => []);
  $("#fault-events").innerHTML = evs
    .slice(0, 60)
    .map((e) =>
      `<div>${fmtTime(e.at)} [${esc(e.worker_id || "-")}] ${esc(e.kind)} ` +
      `${e.batch_id ? esc(e.batch_id) : ""}${e.detail ? " " + esc(e.detail) : ""}</div>`)
    .join("");
}
$("#ledger-refresh").addEventListener("click", refreshLedger);

// ------------------------------------------------------------ auto refresh

setInterval(() => {
  if ($("#auto-refresh").checked) {
    refreshBatches();
    refreshFaults();
    refreshLedger();
  }
}, 2000);

refreshBatches();
refreshFaults();
refreshLedger();
