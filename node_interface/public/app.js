// Navegação por abas moderna com transições suaves
function initTabs() {
  try {
    const tabs = document.querySelectorAll('#mainTabs .tab');
    const contents = document.querySelectorAll('.tab-content');
    
    if (tabs.length === 0) {
      setTimeout(initTabs, 100);
      return;
    }
    
    tabs.forEach(tab => {
      tab.addEventListener('click', (e) => {
        e.preventDefault();
        const targetTab = tab.dataset.tab;
        
        // Transição suave entre abas
        tabs.forEach(t => t.classList.remove('active'));
        contents.forEach(c => {
          c.classList.remove('active');
          // Pequeno delay para transição suave
          setTimeout(() => {
            if (!c.classList.contains('active')) {
              c.style.display = 'none';
            }
          }, 100);
        });
        
        // Ativar nova aba
        tab.classList.add('active');
        const targetContent = document.getElementById(`tab-${targetTab}`);
        if (targetContent) {
          targetContent.style.display = 'block';
          setTimeout(() => {
            targetContent.classList.add('active');
          }, 10);
        }
      });
    });
    
    // Ativar primeira aba por padrão
    if (tabs.length > 0 && !document.querySelector('#mainTabs .tab.active')) {
      tabs[0].click();
    }
    
  } catch (error) {
    console.error('Error initializing tabs:', error);
  }
}

// Inicializar abas quando DOM estiver pronto
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initTabs);
} else {
  initTabs();
}

const fmtDate = (s) => {
  if (!s) return "";
  try {
    const d = new Date(s);
    return d.toLocaleString("pt-BR", { hour12: false });
  } catch {
    return String(s);
  }
};

const fmtProtocolDate = (s) => {
  const v = String(s || "");
  if (!v) return "";
  try {
    const m1 = v.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (m1) return `${m1[1]}/${m1[2]}/${m1[3]}`;
    const m2 = v.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
    if (m2) return `${m2[1]}/${m2[2]}/${m2[3]}`;
    const m3 = v.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (m3) return `${m3[3]}/${m3[2]}/${m3[1]}`;
    return v;
  } catch { return v; }
};

const getProtocolDate = (r) => {
  try {
    const v1 = String(r.data_protocolo || "").trim();
    const norm = fmtProtocolDate(v1);
    if (norm) {
      const mm = parseInt(norm.slice(3,5), 10);
      const dd = parseInt(norm.slice(0,2), 10);
      if (mm >= 1 && mm <= 12 && dd >= 1 && dd <= 31) return norm;
    }
    const cand = `${r.peticao_encontrada || ''} ${r.nome_arquivo_original || ''} ${r.detalhes || ''}`;
    const m = cand.match(/(\d{2}[./]\d{2}[./]\d{4})/);
    if (m) {
      const c = fmtProtocolDate(m[1]);
      const mm = parseInt(c.slice(3,5), 10);
      const dd = parseInt(c.slice(0,2), 10);
      if (mm >= 1 && mm <= 12 && dd >= 1 && dd <= 31) return c;
    }
  } catch {}
  return "";
};

const el = (sel) => document.querySelector(sel);

const renderSummary = async (data) => {
  const box = el("#summary");
  box.innerHTML = "";
  try {
    const j = data || (await (await fetch("/status")).json());
    const items = [
      { k: "Total verificações", v: j.total },
      { k: "Execuções ativas", v: j.ativas },
      { k: "Itens pendentes", v: j.pendentes },
      { k: "Hoje", v: j.hoje },
    ];
    items.forEach((m) => {
      const div = document.createElement("div");
      div.className = "metric";
      div.innerHTML = `<div style="font-size:12px;color:var(--muted)">${m.k}</div><div style="font-size:18px">${m.v}</div>`;
      box.appendChild(div);
    });
  } catch (e) {
    box.textContent = String(e);
  }
};

let jobsMap = new Map();
let selectedWorkerId = 'ALL';
let recentRowsCache = [];
let recentFilter = 'all';
let openBatches = new Set();
let failureSelection = new Set();
let manualStreamLock = false;
const renderJobs = async (rowsParam) => {
  const tbody = el("#jobsTable tbody");
  try {
    const rows = rowsParam || (await (await fetch("/jobs")).json());
    const seen = new Set();
    let hasRunning = false;
    rows.forEach((row) => {
      const id = String(row.batch_id);
      seen.add(id);
      let tr = tbody.querySelector(`tr[data-batch='${id}']`);
      const pct = Number(row.pct || 0);
      const prog = Number(row.progress || 0);
      const total = Number(row.total_arquivos || 0);
      const processos = Number(row.processos_enviados || 0);
      const analisados = Number(row.analisados || 0);
      const aProt = Number(row.analisados_protocolizadas || 0);
      const aNao = Number(row.analisados_nao_encontradas || 0);
      const itPend = Number(row.itens_pendentes || 0);
      const itRun = Number(row.itens_execucao || 0);
      const itDone = Number(row.itens_concluidos || 0);
      const itFail = Number(row.itens_falha || 0);
      const progBar = `<div class="progress"><div style="width:${pct}%"></div></div><div style="font-size:12px;color:var(--muted)">${prog}/${total} (${pct}%)</div>`;
  const statusMap = { queued: "Em fila", starting: "Iniciando", running: "Em execução", done: "Finalizado", error: "Erro" };
      const st = statusMap[(row.status || "").toLowerCase()] || (row.status || "");
      if ((row.status || "").toLowerCase() === 'running') hasRunning = true;
      if (!tr) {
        tr = document.createElement("tr");
        tr.setAttribute("data-batch", id);
        const actions = document.createElement("td");
        const btnVer = document.createElement("button");
        btnVer.textContent = "Ver itens";
        btnVer.onclick = () => {
          el("#batchSelect").value = row.batch_id;
          loadBatch();
        };
        const btnFin = document.createElement("button");
        btnFin.textContent = "Finalizar";
        btnFin.style.marginLeft = "6px";
        btnFin.onclick = async () => {
          await fetch(`/jobs/${row.batch_id}/finalize`, { method: "POST" });
          renderJobs();
          renderSummary();
        };
        actions.appendChild(btnVer);
        actions.appendChild(btnFin);
        tr.innerHTML = `
          <td class="c-batch"></td>
          <td class="c-inicio"></td>
          <td class="c-status"></td>
          <td class="c-proc-env"></td>
          <td class="c-analis"></td>
          <td class="c-it-pend"></td>
          <td class="c-it-run"></td>
          <td class="c-it-done"></td>
          <td class="c-it-fail"></td>
          <td class="c-prog"></td>
          <td class="c-usr"></td>
          <td class="c-nav"></td>
          <td class="c-host"></td>
          <td class="c-prot"></td>
          <td class="c-nao"></td>
        `;
        tr.appendChild(actions);
        tbody.appendChild(tr);
      }
      tr.style.cursor = "pointer";
      tr.querySelector(".c-batch").textContent = row.batch_id;
      tr.querySelector(".c-inicio").textContent = fmtDate(row.iniciado_em);
      tr.querySelector(".c-status").textContent = st;
      tr.querySelector(".c-proc-env").textContent = String(processos);
      tr.querySelector(".c-analis").textContent = String(analisados);
      tr.querySelector(".c-it-pend").textContent = String(itPend);
      tr.querySelector(".c-it-run").textContent = String(itRun);
      tr.querySelector(".c-it-done").textContent = String(itDone);
      tr.querySelector(".c-it-fail").textContent = String(itFail);
      tr.querySelector(".c-prog").innerHTML = progBar;
      tr.querySelector(".c-usr").textContent = row.usuario_projudi || "";
      tr.querySelector(".c-nav").textContent = row.navegador_modo || "";
      tr.querySelector(".c-host").textContent = row.host_execucao || "";
      tr.querySelector(".c-prot").innerHTML = `<span class="badge good">${aProt}</span>`;
      tr.querySelector(".c-nao").innerHTML = `<span class="badge bad">${aNao}</span>`;
      jobsMap.set(id, row);
      let exp = tbody.querySelector(`tr.exp-batch[data-batch='${id}']`);
      if (!exp) {
        exp = document.createElement("tr");
        exp.className = "exp-batch";
        exp.setAttribute("data-batch", id);
        exp.style.display = "none";
        const td = document.createElement("td");
        td.colSpan = 16;
        exp.appendChild(td);
        tbody.insertBefore(exp, tr.nextSibling);
      } else {
        if (exp.previousElementSibling !== tr) {
          tbody.removeChild(exp);
          tbody.insertBefore(exp, tr.nextSibling);
        }
      }
      const expTd = exp.querySelector("td");
      tr.onclick = (ev) => {
        if (ev && ev.target && String(ev.target.tagName).toUpperCase() === 'BUTTON') return;
        const show = exp.style.display === "none";
        exp.style.display = show ? "table-row" : "none";
        if (show) {
          openBatches.add(id);
          loadJobDetails(id, expTd);
        } else {
          openBatches.delete(id);
        }
      };
      if (openBatches.has(id)) {
        exp.style.display = "table-row";
        loadJobDetails(id, expTd);
      } else {
        exp.style.display = "none";
      }
    });
    [...tbody.querySelectorAll("tr[data-batch]")].forEach(tr => {
      const id = tr.getAttribute("data-batch");
      if (!seen.has(id)) tr.remove();
    });
    [...tbody.querySelectorAll("tr.exp-batch")].forEach(tr => {
      const id = tr.getAttribute("data-batch");
      if (!seen.has(id)) tr.remove();
    });
    if (!rows.length && !tbody.querySelector("tr")) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 15;
      td.textContent = "Sem execuções";
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    const btn = el('#backupResetBtn');
    if (btn) btn.disabled = !!hasRunning;
    const hasStarting = !!rows.find(r => String(r.status || '').toLowerCase() === 'starting');
    const sw = el('#startWorker');
    if (sw) sw.disabled = hasRunning || hasStarting;
    const activeRunning = rows.find(r => ['running','starting'].includes(String(r.status || '').toLowerCase()));
    const activeQueued = rows.find(r => String(r.status || '').toLowerCase() === 'queued');
    const nextBatch = activeRunning ? String(activeRunning.batch_id) : (activeQueued ? String(activeQueued.batch_id) : "");
    if (!manualStreamLock) {
      if (nextBatch && nextBatch !== currentBatch) {
        openStream(nextBatch);
      } else if (!nextBatch && currentBatch) {
        openStream("");
      }
    }
  } catch {}
};

const renderItems = (rows) => {
  const tbody = el("#itemsTable tbody");
  tbody.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.id}</td>
      <td>${r.nome_arquivo}</td>
      <td>${r.numero_processo}</td>
      <td>${r.identificador || ""}</td>
      <td>${r.status || ""}</td>
      <td>${r.mensagem || ""}</td>
    `;
    tbody.appendChild(tr);
  });
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 6;
    td.textContent = "Sem itens";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
};

const renderLogs = (rows) => {
  const tabsBox = el('#logsTabs');
  const tbody = el("#logsTable tbody");
  const list = Array.isArray(rows) ? rows : [];
  const ids = [...new Set(list.map(r => r.worker_id).filter(Boolean))];
  if (tabsBox) {
    tabsBox.innerHTML = '';
    const mkTab = (id, label) => {
      const t = document.createElement('div');
      t.className = 'tab' + (selectedWorkerId === id ? ' active' : '');
      t.textContent = label;
      t.onclick = () => { selectedWorkerId = id; renderLogs(list); };
      return t;
    };
    tabsBox.appendChild(mkTab('ALL', 'Todos'));
    ids.forEach(id => { tabsBox.appendChild(mkTab(id, String(id))); });
  }
  const rows2 = selectedWorkerId === 'ALL' ? list : list.filter(r => String(r.worker_id || '') === String(selectedWorkerId));
  tbody.innerHTML = "";
  rows2.forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtDate(r.timestamp)}</td>
      <td>${r.nivel}</td>
      <td>${r.mensagem}</td>
    `;
    tbody.appendChild(tr);
  });
  if (!rows2.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 3;
    td.textContent = ids.length ? "Sem logs para este robô" : "Sem logs";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
};

const renderResults = (rows) => {
  const tbody = el("#resultsTable tbody");
  tbody.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${fmtDate(r.data_verificacao)}</td>
      <td>${r.numero_processo}</td>
      <td>${r.identificador_peticao || ""}</td>
      <td>${r.nome_arquivo_original || ""}</td>
      <td>${r.status_verificacao || ""}</td>
      <td>${r.peticao_encontrada || ""}</td>
      <td>${getProtocolDate(r)}</td>
      <td>${r.detalhes || ""}</td>
    `;
    tbody.appendChild(tr);
  });
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 8;
    td.textContent = "Sem resultados";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
};

const renderRecentResults = (rows) => {
  const tbody = el("#recentResultsTable tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  recentRowsCache = Array.isArray(rows) ? rows : [];
  const filtered = recentRowsCache.filter(r => {
    try {
      const base = String(r.data_protocolo || "");
      if (!base) return recentFilter === 'all';
      let d = null;
      if (/^\d{2}\/\d{2}\/\d{4}$/.test(base)) {
        const [dd,mm,yy] = base.split('/');
        d = new Date(`${yy}-${mm}-${dd}T00:00:00`);
      } else if (/^\d{2}\.\d{2}\.\d{4}$/.test(base)) {
        const [dd,mm,yy] = base.split('.');
        d = new Date(`${yy}-${mm}-${dd}T00:00:00`);
      } else if (/^\d{4}-\d{2}-\d{2}/.test(base)) {
        d = new Date(base);
      }
      if (!d) return recentFilter === 'all';
      const now = new Date();
      const startToday = new Date(now.getFullYear(), now.getMonth(), now.getDate());
      if (recentFilter === 'today') return d >= startToday;
      if (recentFilter === '7d') return (now - d) <= 7 * 86400000;
      if (recentFilter === '30d') return (now - d) <= 30 * 86400000;
      return true;
    } catch { return true; }
  });
  filtered.forEach((r) => {
    const tr = document.createElement("tr");
    tr.style.cursor = "pointer";
    tr.innerHTML = `
      <td>${fmtDate(r.data_verificacao)}</td>
      <td>${r.numero_processo}</td>
      <td>${r.identificador_peticao || ""}</td>
      <td>${r.nome_arquivo_original || ""}</td>
      <td>${r.status_verificacao || ""}</td>
      <td>${r.peticao_encontrada || ""}</td>
      <td>${getProtocolDate(r)}</td>
      <td>${r.detalhes || ""}</td>
    `;
    const exp = document.createElement("tr");
    exp.style.display = "none";
    exp.className = 'exp-recent';
    const td = document.createElement("td");
    td.colSpan = 8;
    const protDate = (function(){
      try {
        const txt = String(r.detalhes || "");
        const m = txt.match(/(\d{2}\/\d{2}\/\d{4})/);
        return m ? m[1] : "";
      } catch { return ""; }
    })();
    const btn = document.createElement("button");
    btn.textContent = "Ver execução";
    btn.onclick = () => {
      const sel = el("#batchSelect");
      if (sel) { sel.value = String(r.batch_id || ""); }
      loadBatch();
    };
    const box = document.createElement("div");
    box.innerHTML = `
      <div style="font-size:12px;color:var(--muted)">Batch: ${r.batch_id || ""}</div>
      <div>Usuário: ${r.usuario_projudi || ""}</div>
      <div>Navegador: ${r.navegador_modo || ""}</div>
      <div>Host: ${r.host_execucao || ""}</div>
      <div>Protocolo: ${protDate || ""}</div>
      <div>Detalhes: ${r.detalhes || ""}</div>
    `;
    td.appendChild(box);
    td.appendChild(btn);
    exp.appendChild(td);
    tr.addEventListener("click", () => {
      exp.style.display = exp.style.display === "none" ? "table-row" : "none";
    });
    tbody.appendChild(tr);
    tbody.appendChild(exp);
  });
  if (!filtered.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 8;
    td.textContent = "Sem resultados";
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
};

const renderFailures = (rows) => {
  const tbody = el('#failureTable tbody');
  const status = el('#failuresStatus');
  if (!tbody) return;
  tbody.innerHTML = '';
  const all = Array.isArray(rows) ? rows : [];
  const list = all.filter(r => String(r.status_verificacao || '') !== 'Protocolizada');
  if (status) status.textContent = `${list.length} insucesso(s)`;
  list.forEach(r => {
    const tr = document.createElement('tr');
    const tdSel = document.createElement('td');
    const file = String(r.nome_arquivo_original || '').trim();
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'failure-select';
    cb.setAttribute('data-file', file);
    cb.disabled = !file;
    cb.checked = file && failureSelection.has(file);
    cb.addEventListener('change', () => {
      if (cb.checked) {
        if (file) failureSelection.add(file);
      } else {
        failureSelection.delete(file);
      }
      const st = el('#failuresStatus');
      if (st) st.textContent = `${list.length} insucesso(s) • ${failureSelection.size} selecionado(s)`;
    });
    tdSel.appendChild(cb);
    tr.appendChild(tdSel);
    tr.innerHTML += `
      <td>${fmtDate(r.data_verificacao)}</td>
      <td>${r.numero_processo || ''}</td>
      <td>${r.identificador_peticao || ''}</td>
      <td>${r.nome_arquivo_original || ''}</td>
      <td>${r.status_verificacao || ''}</td>
      <td>${r.detalhes || ''}</td>
    `;
    tbody.appendChild(tr);
  });
  if (!list.length) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 7;
    td.textContent = 'Sem insucessos';
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
};

async function loadFailures() {
  try {
    const r = await fetch('/results?limit=2000');
    const j = await r.json();
    renderFailures(j);
  } catch {}
}

const enqueueSelectedFailures = async () => {
  const status = el('#failuresStatus');
  const files = Array.from(failureSelection);
  if (!files.length) { if (status) status.textContent = 'Selecione itens'; return; }
  try {
    const modeSel = el('#headlessSelect');
    const mode = modeSel ? String(modeSel.value || 'headless') : 'headless';
    const usuarioSel = el('#usuarioSelect');
    const usuario = usuarioSel ? String(usuarioSel.value || '') : '';
    const r = await fetch('/enqueue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ files, mode, robots: 1, usuario })
    });
    const j = await r.json();
    if (!j.ok) { if (status) status.textContent = j.error || 'Falha'; return; }
    failureSelection.clear();
    if (status) status.textContent = `Batch ${j.batch_id} com ${j.count} itens`;
    const sel = el('#batchSelect');
    if (sel) sel.value = j.batch_id;
    renderJobs();
    loadBatch();
    loadFailures();
  } catch (e) {
    if (status) status.textContent = String(e);
  }
};

const loadBatch = async () => {
  const batch = el("#batchSelect").value.trim();
  if (!batch) return;
  try {
    const [itemsRes, logsRes, resultsRes] = await Promise.all([
      fetch(`/jobs/${batch}/items`),
      fetch(`/jobs/${batch}/logs`),
      fetch(`/jobs/${batch}/results`),
    ]);
    renderItems(await itemsRes.json());
    renderLogs(await logsRes.json());
    renderResults(await resultsRes.json());
  } catch {}
};

const loadJobDetails = async (batch, td) => {
  try {
    td.innerHTML = "";
    const info = jobsMap.get(String(batch)) || {};
    const [itemsRes, resultsRes] = await Promise.all([
      fetch(`/jobs/${batch}/items`),
      fetch(`/jobs/${batch}/results`),
    ]);
    const items = await itemsRes.json();
    const results = await resultsRes.json();
    const box = document.createElement("div");
    const head = document.createElement("div");
    head.style.marginBottom = "8px";
    head.innerHTML = `
      <div style="font-size:12px;color:var(--muted)">Batch: ${batch}</div>
      <div>Usuário: ${info.usuario_projudi || ""}</div>
      <div>Navegador: ${info.navegador_modo || ""}</div>
      <div>Host: ${info.host_execucao || ""}</div>
      <div>Progresso: ${(info.pct || 0)}%</div>
      <div>Protocolizadas: <span class="badge good">${info.analisados_protocolizadas || 0}</span> Não protocolizadas: <span class="badge bad">${info.analisados_nao_encontradas || 0}</span></div>
    `;
    box.appendChild(head);
    const btn = document.createElement("button");
    btn.textContent = "Ver execução";
    btn.style.marginBottom = "8px";
    btn.onclick = () => { el("#batchSelect").value = String(batch); loadBatch(); };
    box.appendChild(btn);
    const table = document.createElement("table");
    table.style.width = "100%";
    table.innerHTML = `
      <thead>
        <tr>
          <th>ID</th>
          <th>Arquivo</th>
          <th>Processo</th>
          <th>Identificador</th>
          <th>Status</th>
          <th>Mensagem</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;
    const tbody2 = table.querySelector("tbody");
    const list = Array.isArray(items) ? items.slice(0, 20) : [];
    list.forEach(r => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${r.id}</td>
        <td>${r.nome_arquivo}</td>
        <td>${r.numero_processo}</td>
        <td>${r.identificador || ""}</td>
        <td>${r.status || ""}</td>
        <td>${r.mensagem || ""}</td>
      `;
      tbody2.appendChild(tr);
    });
    if (!list.length) {
      const tr = document.createElement("tr");
      const td2 = document.createElement("td");
      td2.colSpan = 6;
      td2.textContent = "Sem itens";
      tr.appendChild(td2);
      tbody2.appendChild(tr);
    }
    const resBox = document.createElement("div");
    resBox.style.marginTop = "8px";
    resBox.style.fontSize = "12px";
    resBox.style.color = "var(--muted)";
    resBox.textContent = `Resultados: ${Array.isArray(results) ? results.length : 0}`;
    box.appendChild(table);
    box.appendChild(resBox);
    td.appendChild(box);
  } catch {}
};

const enqueue = async () => {
  const ta = el("#filesInput");
  const status = el("#enqueueStatus");
  const lines = ta.value.split(/\r?\n/).map((s) => s.trim()).filter((s) => s);
  if (!lines.length) {
    status.textContent = "Informe arquivos";
    return;
  }
  try {
    const modeSel = el("#headlessSelect");
    const mode = modeSel ? String(modeSel.value || "headless") : "headless";
    const usuarioSel = el("#usuarioSelect");
    const usuario = usuarioSel ? String(usuarioSel.value || "") : "";
    const robots = 1;
    const r = await fetch("/enqueue", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ files: lines, mode, robots, usuario }),
    });
    const j = await r.json();
    if (!j.ok) {
      status.textContent = j.error || "Falha";
      return;
    }
    status.textContent = `Batch ${j.batch_id} com ${j.count} itens`;
    el("#batchSelect").value = j.batch_id;
    renderJobs();
    loadBatch();
  } catch (e) {
    status.textContent = String(e);
  }
};

const backupReset = async () => {
  const btn = el('#backupResetBtn');
  const status = el('#backupStatus');
  if (btn) btn.disabled = true;
  status.textContent = 'Executando...';
  try {
    const r = await fetch('/backup-reset', { method: 'POST' });
    const j = await r.json();
    if (!j.ok) {
      status.textContent = j.error || 'Falha';
    } else {
      status.textContent = `Backup em ${j.backup_path}`;
      renderSummary();
      renderJobs();
    }
  } catch (e) {
    status.textContent = String(e);
  } finally {
    // estado do botão será reavaliado por renderJobs()
  }
};

const startWorker = async () => {
  try {
    const btn = el('#startWorker');
    if (btn) btn.disabled = true;
    await fetch("/start-worker", { method: "POST" });
    setTimeout(() => { try { const rowsBox = el('#jobsTable'); if (btn && rowsBox) btn.disabled = false; } catch {} }, 4000);
  } catch {}
};

let stream = null;
let currentBatch = "";
let queue = { summary: null, jobs: null, items: null, logs: null, results: null, recent_results: null, snapshots: null };
let scheduled = false;
let snapshotsOpen = false;
const schedule = (type, data) => {
  queue[type] = data;
  if (scheduled) return;
  scheduled = true;
  setTimeout(() => {
    if (queue.summary) { renderSummary(queue.summary); }
    if (queue.jobs) { renderJobs(queue.jobs); }
    if (queue.items) { renderItems(queue.items); }
    if (queue.results) { renderResults(queue.results); }
    if (queue.logs) { renderLogs(queue.logs); }
    if (queue.recent_results) { renderRecentResults(queue.recent_results); }
    if (queue.snapshots && snapshotsOpen) { renderSnapshots(queue.snapshots); }
    queue = { summary: null, jobs: null, items: null, logs: null, results: null, recent_results: null, snapshots: null };
    scheduled = false;
  }, 250);
};

const openStream = (batch = "") => {
  if (stream) {
    try { stream.close(); } catch {}
    stream = null;
  }
  currentBatch = batch;
  const url = batch ? `/events?batch=${encodeURIComponent(batch)}` : "/events";
  stream = new EventSource(url);
  stream.addEventListener("summary", (ev) => {
    try { schedule("summary", JSON.parse(ev.data)); } catch {}
  });
  stream.addEventListener("jobs", (ev) => {
    try { schedule("jobs", JSON.parse(ev.data)); } catch {}
  });
  stream.addEventListener("items", (ev) => {
    try { schedule("items", JSON.parse(ev.data)); } catch {}
  });
  stream.addEventListener("logs", (ev) => {
    try { schedule("logs", JSON.parse(ev.data)); } catch {}
  });
  stream.addEventListener("results", (ev) => {
    try { schedule("results", JSON.parse(ev.data)); } catch {}
  });
  stream.addEventListener("recent_results", (ev) => {
    try { schedule("recent_results", JSON.parse(ev.data)); } catch {}
  });
  stream.addEventListener("snapshots", (ev) => {
    try { schedule("snapshots", JSON.parse(ev.data)); } catch {}
  });
  stream.onerror = () => {
    setTimeout(() => openStream(currentBatch), 1500);
  };
};

const init = () => {
  el("#enqueueBtn").addEventListener("click", enqueue);
  el("#startWorker").addEventListener("click", startWorker);
  const sc = el('#saveCredsBtn');
  if (sc) sc.addEventListener('click', saveCreds);
  const br = el('#backupResetBtn');
  if (br) br.addEventListener('click', backupReset);
  const sp = el('#snapshotsPanel');
  if (sp) sp.addEventListener('toggle', () => {
    snapshotsOpen = sp.open;
    if (!snapshotsOpen) {
      const box = el('#snapshots');
      if (box) box.innerHTML = '';
    } else if (queue.snapshots) {
      renderSnapshots(queue.snapshots);
    }
  });
  const rr = el('#robotsRefresh');
  if (rr) rr.addEventListener('click', loadRobots);
  const rk = el('#robotsKillAgg');
  if (rk) rk.addEventListener('click', killRobotsAggressive);
  const rkw = el('#robotsKillWorkers');
  if (rkw) rkw.addEventListener('click', killRobotsWorkers);
  const ra = el('#robotsAuto');
  if (ra) {
    ra.addEventListener('change', setupRobotsAuto);
    setupRobotsAuto();
  }
  const rp = el('#recentPanel');
  if (rp) rp.addEventListener('toggle', () => {
    const tbody = el('#recentResultsTable tbody');
    if (!tbody) return;
    [...tbody.querySelectorAll('tr.exp-recent')].forEach(tr => {
      tr.style.display = rp.open ? 'table-row' : 'none';
    });
  });
  const rf = el('#recentFilter');
  if (rf) rf.addEventListener('change', () => {
    recentFilter = rf.value || 'all';
    renderRecentResults(recentRowsCache);
  });
  const jt = el('#jobTabs');
  if (jt) {
    const tabs = jt.querySelectorAll('.tab');
    tabs.forEach(t => {
      t.addEventListener('click', () => {
        tabs.forEach(x => x.classList.remove('active'));
        t.classList.add('active');
        const tv = String(t.dataset.tab || '');
        const jr = el('#job-results');
        const jl = el('#job-logs');
        if (jr) jr.style.display = tv === 'resultados' ? 'block' : 'none';
        if (jl) jl.style.display = tv === 'logs' ? 'block' : 'none';
      });
    });
  }
  el("#refreshAll").addEventListener("click", () => {
    openStream(currentBatch);
  });
  el("#loadBatch").addEventListener("click", () => {
    const b = el("#batchSelect").value.trim();
    openStream(b);
  });
  const sr = el('#successRefresh');
  if (sr) sr.addEventListener('click', loadSuccesses);
  const se = el('#successExport');
  if (se) se.addEventListener('click', () => { window.location.href = '/export/successes.csv'; });
  const seX = el('#successExportXlsx');
  if (seX) seX.addEventListener('click', () => { window.location.href = '/export/successes.xlsx'; });
  const fr = el('#failuresRefresh');
  if (fr) fr.addEventListener('click', loadFailures);
  const fe = el('#failuresEnqueue');
  if (fe) fe.addEventListener('click', enqueueSelectedFailures);
  const fsa = el('#failureSelectAll');
  if (fsa) fsa.addEventListener('change', () => {
    const tbody = el('#failureTable tbody');
    if (!tbody) return;
    const checked = !!fsa.checked;
    [...tbody.querySelectorAll('input.failure-select')].forEach(ch => {
      ch.checked = checked;
      const f = String(ch.getAttribute('data-file') || '');
      if (checked && f) failureSelection.add(f); else failureSelection.delete(f);
    });
    const st = el('#failuresStatus');
    if (st) st.textContent = `${failureSelection.size} selecionado(s)`;
  });
  const rtc = el('#rtConnect');
  if (rtc) rtc.addEventListener('click', () => {
    const b = el('#rtBatchSelect').value.trim();
    if (!b) { const st = el('#rtStatus'); if (st) st.textContent = 'Informe batch_id'; return; }
    manualStreamLock = true;
    openStream(b);
    const st = el('#rtStatus');
    if (st) st.textContent = `Conectado ao batch ${b}`;
  });
  // Fallback HTTP inicial, além do stream
  renderSummary();
  renderJobs();
  loadUsuarios();
  fetch("/results?limit=200").then(r => r.json()).then(renderRecentResults).catch(() => {});
  loadSuccesses();
  loadFailures();
  openStream("");
};

document.addEventListener("DOMContentLoaded", init);

const renderSnapshots = (rows) => {
  const box = el('#snapshots');
  if (!box) return;
  box.innerHTML = '';
  const max = 12;
  const list = Array.isArray(rows) ? rows.slice(0, max) : [];
  if (!list.length) {
    const p = document.createElement('p');
    p.style.color = 'var(--muted)';
    p.style.fontSize = '12px';
    p.textContent = 'Sem prints';
    box.appendChild(p);
    return;
  }
  list.forEach(r => {
    const d = document.createElement('div');
    d.style.display = 'inline-block';
    d.style.margin = '4px';
    d.style.width = '180px';
    d.style.border = '1px solid var(--border)';
    d.style.borderRadius = '4px';
    const img = document.createElement('img');
    img.src = r.url;
    img.style.width = '100%';
    img.style.display = 'block';
    const cap = document.createElement('div');
    cap.style.fontSize = '11px';
    cap.style.color = 'var(--muted)';
    cap.style.padding = '4px';
    cap.textContent = r.name;
    d.appendChild(img);
    d.appendChild(cap);
    box.appendChild(d);
  });
};

const renderRobots = (rows) => {
  const t = el('#robotsTable tbody');
  const status = el('#robotsStatus');
  if (!t) return;
  t.innerHTML = '';
  const list = Array.isArray(rows) ? rows : [];
  status.textContent = `${list.length} processo(s)`;
  list.forEach(r => {
    const tr = document.createElement('tr');
    const td1 = document.createElement('td'); td1.textContent = String(r.pid || '');
    const td2 = document.createElement('td'); td2.textContent = String(r.name || '');
    const td3 = document.createElement('td'); td3.textContent = String(r.cmd || '').slice(0, 200);
    tr.appendChild(td1); tr.appendChild(td2); tr.appendChild(td3);
    t.appendChild(tr);
  });
};

const loadRobots = async () => {
  try {
    const r = await fetch('/robots');
    const j = await r.json();
    if (j && j.ok) renderRobots(j.rows);
  } catch {}
};

const killRobotsAggressive = async () => {
  try {
    const status = el('#robotsStatus');
    status.textContent = 'Finalizando...';
    const r = await fetch('/robots/kill', { method: 'POST' });
    const j = await r.json();
    status.textContent = j && j.ok ? `Finalizados: ${j.killed}` : 'Falha ao finalizar';
    await loadRobots();
  } catch {}
};

let robotsTimer = null;
const setupRobotsAuto = () => {
  const ra = el('#robotsAuto');
  const enabled = !!(ra && ra.checked);
  if (robotsTimer) { clearInterval(robotsTimer); robotsTimer = null; }
  if (enabled) {
    robotsTimer = setInterval(loadRobots, 10000);
    loadRobots();
  }
};

const killRobotsWorkers = async () => {
  try {
    const status = el('#robotsStatus');
    status.textContent = 'Finalizando workers...';
    const r = await fetch('/robots/kill-workers', { method: 'POST' });
    const j = await r.json();
    status.textContent = j && j.ok ? `Workers finalizados: ${j.killed}` : 'Falha ao finalizar workers';
    await loadRobots();
  } catch {}
};
const saveCreds = async () => {
  const u = el('#cfgUser').value.trim();
  const p = el('#cfgPass').value.trim();
  const status = el('#saveCredsStatus');
  if (!u || !p) { status.textContent = 'Informe usuário e senha'; return; }
  status.textContent = 'Salvando...';
  try {
    const r = await fetch('/config/credenciais', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ usuario: u, senha: p })
    });
    const j = await r.json();
    status.textContent = j && j.ok ? 'OK' : (j.error || 'Falha');
    renderSummary();
  } catch (e) {
    status.textContent = String(e);
  }
};
const loadUsuarios = async () => {
  try {
    const sel = el('#usuarioSelect');
    if (!sel) return;
    const r = await fetch('/config/usuarios');
    const j = await r.json();
    if (!j || !j.ok) return;
    const list = Array.isArray(j.usuarios) ? j.usuarios : [];
    sel.innerHTML = '';
    list.forEach(u => {
      const opt = document.createElement('option');
      opt.value = String(u);
      opt.textContent = String(u);
      sel.appendChild(opt);
    });
  } catch {}
};

const renderSuccesses = (rows) => {
  const tbody = el('#successTable tbody');
  const status = el('#successStatus');
  if (!tbody) return;
  tbody.innerHTML = '';
  const list = Array.isArray(rows) ? rows : [];
  if (status) status.textContent = `${list.length} sucesso(s)`;
  list.forEach(r => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${fmtDate(r.data_verificacao)}</td>
      <td>${r.numero_processo || ''}</td>
      <td>${r.codproc || ''}</td>
      <td>${r.codpet || ''}</td>
      <td>${r.identificador_peticao || ''}</td>
      <td>${r.nome_arquivo_original || ''}</td>
      <td>${fmtProtocolDate(r.data_protocolo || '')}</td>
    `;
    tbody.appendChild(tr);
  });
  if (!list.length) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 7;
    td.textContent = 'Sem sucessos';
    tr.appendChild(td);
    tbody.appendChild(tr);
  }
};

async function loadSuccesses() {
  try {
    const r = await fetch('/successes?limit=1000');
    const j = await r.json();
    renderSuccesses(j);
  } catch {}
}
