import express from "express";
import cors from "cors";
import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";
import fs from "fs";

const app = express();
app.use(cors());
app.use(express.json());

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dbPath = path.resolve(__dirname, "..", "verificador_peticoes", "data", "verificacoes.db");
const db = new Database(dbPath, {});

function initDB() {
  try {
    db.exec("PRAGMA journal_mode=WAL;");
    db.exec("PRAGMA synchronous=NORMAL;");
    db.exec(`CREATE TABLE IF NOT EXISTS verificacoes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      numero_processo TEXT NOT NULL,
      identificador_peticao TEXT NOT NULL,
      nome_arquivo_original TEXT NOT NULL,
      status_verificacao TEXT NOT NULL,
      peticao_encontrada TEXT,
      data_verificacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      detalhes TEXT,
      UNIQUE(numero_processo, identificador_peticao)
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS logs_verificacao (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      nivel TEXT NOT NULL,
      mensagem TEXT NOT NULL,
      detalhes TEXT,
      batch_id TEXT
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS execucoes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id TEXT UNIQUE,
      iniciado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      finalizado_em TIMESTAMP,
      usuario_projudi TEXT,
      navegador_modo TEXT,
      host_execucao TEXT,
      total_arquivos INTEGER,
      total_protocolizadas INTEGER,
      total_nao_encontradas INTEGER,
      status TEXT DEFAULT 'pending',
      progress INTEGER DEFAULT 0
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS job_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      batch_id TEXT NOT NULL,
      nome_arquivo TEXT NOT NULL,
      numero_processo TEXT NOT NULL,
      identificador TEXT,
      status TEXT DEFAULT 'pending',
      mensagem TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP
    )`);
    db.exec(`CREATE TABLE IF NOT EXISTS config (
      chave TEXT PRIMARY KEY,
      valor TEXT NOT NULL
    )`);

    const vcols = db.prepare("PRAGMA table_info(verificacoes)").all().map(r => r.name);
    if (!vcols.includes("usuario_projudi")) db.exec("ALTER TABLE verificacoes ADD COLUMN usuario_projudi TEXT DEFAULT ''");
    if (!vcols.includes("navegador_modo")) db.exec("ALTER TABLE verificacoes ADD COLUMN navegador_modo TEXT DEFAULT ''");
    if (!vcols.includes("host_execucao")) db.exec("ALTER TABLE verificacoes ADD COLUMN host_execucao TEXT DEFAULT ''");
    if (!vcols.includes("batch_id")) db.exec("ALTER TABLE verificacoes ADD COLUMN batch_id TEXT DEFAULT ''");

    const lcols = db.prepare("PRAGMA table_info(logs_verificacao)").all().map(r => r.name);
    if (!lcols.includes("batch_id")) db.exec("ALTER TABLE logs_verificacao ADD COLUMN batch_id TEXT");

    const ecols = db.prepare("PRAGMA table_info(execucoes)").all().map(r => r.name);
    if (!ecols.includes("status")) db.exec("ALTER TABLE execucoes ADD COLUMN status TEXT DEFAULT 'pending'");
    if (!ecols.includes("progress")) db.exec("ALTER TABLE execucoes ADD COLUMN progress INTEGER DEFAULT 0");
    if (!ecols.includes("heartbeat_at")) db.exec("ALTER TABLE execucoes ADD COLUMN heartbeat_at TIMESTAMP");
    db.exec("UPDATE execucoes SET status='queued' WHERE status='pending' AND finalizado_em IS NULL");
  } catch (e) {
    console.error("Falha ao inicializar DB:", e);
  }
}

initDB();

const clients = new Set();
let sseTimer = null;

function readSummary() {
  const total = db.prepare("SELECT COUNT(*) as c FROM verificacoes").get().c;
  const ativas = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE finalizado_em IS NULL").get().c;
  const pendentes = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE status='pending'").get().c;
  const hoje = db.prepare("SELECT COUNT(*) as c FROM verificacoes WHERE DATE(data_verificacao)=DATE('now')").get().c;
  return { ok: true, total, ativas, pendentes, hoje };
}

function readJobs() {
  const rows = db.prepare(`
    SELECT 
      e.*, 
      MIN(100.0, ROUND(100.0 * COALESCE(e.progress,0) / NULLIF(e.total_arquivos,0), 1)) AS pct,
      (SELECT COUNT(DISTINCT ji.numero_processo) FROM job_items ji WHERE ji.batch_id=e.batch_id) AS processos_enviados,
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id) AS itens_total,
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id AND ji.status!='pending') AS itens_analisados,
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id AND ji.status='pending') AS itens_pendentes,
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id AND ji.status='running') AS itens_execucao,
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id AND ji.status='done') AS itens_concluidos,
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id AND ji.status='failed') AS itens_falha,
      (SELECT COUNT(*) FROM verificacoes v WHERE v.batch_id=e.batch_id) AS analisados,
      (SELECT COUNT(*) FROM verificacoes v WHERE v.batch_id=e.batch_id AND v.status_verificacao='Protocolizada') AS analisados_protocolizadas,
      (SELECT COUNT(*) FROM verificacoes v WHERE v.batch_id=e.batch_id AND v.status_verificacao='Não encontrada') AS analisados_nao_encontradas
    FROM execucoes e
    ORDER BY e.iniciado_em DESC
    LIMIT 500
  `).all();
  return rows;
}

function readBatch(batch) {
  if (!batch) return { items: [], logs: [], results: [] };
  const items = db.prepare("SELECT * FROM job_items WHERE batch_id=? ORDER BY id ASC").all(batch);
  const logs = db.prepare("SELECT * FROM logs_verificacao WHERE batch_id=? ORDER BY timestamp DESC").all(batch);
  const results = db.prepare("SELECT * FROM verificacoes WHERE batch_id=? ORDER BY data_verificacao DESC").all(batch);
  return { items, logs, results };
}

function readRecentResults(limit = 200) {
  return db.prepare("SELECT * FROM verificacoes ORDER BY data_verificacao DESC LIMIT ?").all(limit);
}

function readSnapshots(limit = 50) {
  try {
    const dir = path.resolve(__dirname, "..", "verificador_peticoes", "data", "snapshots");
    try { fs.mkdirSync(dir, { recursive: true }); } catch {}
    const files = fs.readdirSync(dir).filter(f => /\.(png|jpg|jpeg)$/i.test(f));
    const rows = files.map(f => {
      const fp = path.join(dir, f);
      let ts = 0;
      try { ts = fs.statSync(fp).mtimeMs; } catch {}
      return { name: f, url: `/snapshots/${encodeURIComponent(f)}`, ts };
    }).sort((a, b) => b.ts - a.ts).slice(0, limit);
    return rows;
  } catch {
    return [];
  }
}

function sseWrite(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

app.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  const batch = String(req.query.batch || "");
  const client = { res, batch, prev: { sum: null, jobs: null, items: null, logs: null, results: null } };
  clients.add(client);
  const sum = readSummary();
  const jobs = readJobs();
  const b = readBatch(batch);
  const snaps0 = readSnapshots();
  sseWrite(res, "summary", sum);
  sseWrite(res, "jobs", jobs);
  sseWrite(res, "recent_results", readRecentResults());
  sseWrite(res, "snapshots", snaps0);
  if (batch) {
    sseWrite(res, "items", b.items);
    sseWrite(res, "logs", b.logs);
    sseWrite(res, "results", b.results);
  }
  req.on("close", () => {
    clients.delete(client);
  });
  if (!sseTimer) {
    sseTimer = setInterval(() => {
      for (const c of clients) {
        try {
          const sum2 = readSummary();
          const jobs2 = readJobs();
          const b2 = readBatch(c.batch);
          const rr2 = readRecentResults();
          const snaps = readSnapshots();
          sseWrite(c.res, "summary", sum2);
          sseWrite(c.res, "jobs", jobs2);
          sseWrite(c.res, "recent_results", rr2);
          sseWrite(c.res, "snapshots", snaps);
          if (c.batch) {
            sseWrite(c.res, "items", b2.items);
            sseWrite(c.res, "logs", b2.logs);
            sseWrite(c.res, "results", b2.results);
          }
        } catch (e) {}
      }
      if (clients.size === 0 && sseTimer) {
        clearInterval(sseTimer);
        sseTimer = null;
      }
    }, 1000);
  }
});
app.use(express.static(path.join(__dirname, "public")));
app.use("/snapshots", express.static(path.resolve(__dirname, "..", "verificador_peticoes", "data", "snapshots")));

app.get("/status", (req, res) => {
  try {
    const total = db.prepare("SELECT COUNT(*) as c FROM verificacoes").get().c;
    const ativas = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE finalizado_em IS NULL").get().c;
    const pendentes = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE status='pending'").get().c;
    const hoje = db.prepare("SELECT COUNT(*) as c FROM verificacoes WHERE DATE(data_verificacao)=DATE('now')").get().c;
    res.json({ ok: true, total, ativas, pendentes, hoje });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/jobs", (req, res) => {
  try {
    const rows = readJobs();
    res.json(rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/jobs/:batch/items", (req, res) => {
  try {
    const rows = db
      .prepare("SELECT * FROM job_items WHERE batch_id=? ORDER BY id ASC")
      .all(req.params.batch);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/jobs/:batch/logs", (req, res) => {
  try {
    const rows = db
      .prepare("SELECT * FROM logs_verificacao WHERE batch_id=? ORDER BY timestamp DESC")
      .all(req.params.batch);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/jobs/:batch/results", (req, res) => {
  try {
    const rows = db
      .prepare("SELECT * FROM verificacoes WHERE batch_id=? ORDER BY data_verificacao DESC")
      .all(req.params.batch);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/results", (req, res) => {
  try {
    const limit = Number(req.query.limit || 1000);
    const rows = db
      .prepare("SELECT * FROM verificacoes ORDER BY data_verificacao DESC LIMIT ?")
      .all(limit);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

function parseCNJAndId(name) {
  const cnjRegex = /(\d{1,7}\.\d{2}\.\d{4}\.\d{1}\.\d{2}\.\d{4})/g;
  const idRegex = /_(\d+)_(\d+)_/g;
  const cnjs = [...String(name).matchAll(cnjRegex)].map(m => m[1]).map(c => {
    const parts = String(c).split('.');
    if (parts.length === 6) parts[0] = parts[0].padStart(7, '0');
    return parts.join('.');
  });
  const ids = [...String(name).matchAll(idRegex)].map(m => `_${m[1]}_${m[2]}_`);
  const numero_processo = cnjs.length ? cnjs[cnjs.length - 1] : "";
  const identificador = ids.length ? ids[ids.length - 1] : "";
  return { numero_processo, identificador };
}

function nodeBackupReset() {
  const baseDir = path.resolve(__dirname, "..", "verificador_peticoes", "data");
  const backups = path.join(baseDir, "backups");
  try { fs.mkdirSync(backups, { recursive: true }); } catch {}
  const ts = new Date().toISOString().replace(/[-:T]/g, "").slice(0, 15);
  const backupPath = path.join(backups, `verificacoes_${ts}.db`);
  try { fs.copyFileSync(dbPath, backupPath); } catch {}
  try {
    db.exec("DROP TABLE IF EXISTS verificacoes;");
    db.exec("DROP TABLE IF EXISTS logs_verificacao;");
    db.exec("DROP TABLE IF EXISTS execucoes;");
    db.exec("DROP TABLE IF EXISTS job_items;");
    db.exec("DROP TABLE IF EXISTS config;");
  } catch {}
  initDB();
  return backupPath;
}
function spawnWorkerDetached() {
  const script = path.resolve(__dirname, "..", "verificador_peticoes", "src", "worker.py");
  const pyCwd = path.resolve(__dirname, "..", "verificador_peticoes");
  let child;
  const opts = { detached: true, stdio: "ignore", windowsHide: true, cwd: pyCwd };
  try { child = spawn("pythonw", [script], opts); } catch {}
  if (!child) { try { child = spawn("py", ["-3", script], opts); } catch {} }
  if (!child) { try { child = spawn("python", [script], opts); } catch {} }
  if (!child) { try { child = spawn("python3", [script], opts); } catch {} }
  if (!child) return false;
  child.unref();
  return true;
}

app.post("/enqueue", (req, res) => {
  try {
    const files = Array.isArray(req.body.files) ? req.body.files : [];
    if (!files.length) return res.status(400).json({ ok: false, error: "files vazio" });
    const batch = (Math.random().toString(16).slice(2, 10));
    const usuarioRow = db.prepare("SELECT valor FROM config WHERE chave='PROJUDI_USERNAME'").get();
    const usuario = usuarioRow ? usuarioRow.valor : "";
    const host = process.env.COMPUTERNAME || process.env.HOSTNAME || "";
    db.prepare("INSERT OR REPLACE INTO execucoes (batch_id, iniciado_em, usuario_projudi, navegador_modo, host_execucao, total_arquivos, status, progress) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, 'queued', 0)")
      .run(batch, usuario, 'headless', host, files.length);
    const stmt = db.prepare("INSERT INTO job_items (batch_id, nome_arquivo, numero_processo, identificador, status, mensagem) VALUES (?, ?, ?, ?, 'pending', '')");
    let inserted = 0;
    for (const f of files) {
      const s = String(f).trim();
      if (!s) continue;
      const { numero_processo, identificador } = parseCNJAndId(s);
      stmt.run(batch, s, numero_processo, identificador);
      inserted++;
    }
    const running = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status='running' AND finalizado_em IS NULL").get().c;
    if (running === 0) {
      try { spawnWorkerDetached(); } catch {}
    }
    res.json({ ok: true, batch_id: batch, count: inserted });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post("/jobs/:batch/finalize", (req, res) => {
  try {
    const batch = req.params.batch;
    const mapa = db.prepare("SELECT status_verificacao, COUNT(*) as c FROM verificacoes WHERE batch_id=? GROUP BY status_verificacao").all(batch);
    const m = Object.fromEntries(mapa.map(r => [r.status_verificacao, r.c]));
    const prot = m["Protocolizada"] || 0;
    const nao = m["Não encontrada"] || 0;
    db.prepare("UPDATE execucoes SET finalizado_em=CURRENT_TIMESTAMP, total_protocolizadas=?, total_nao_encontradas=?, status='done' WHERE batch_id=?")
      .run(prot, nao, batch);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post("/start-worker", (req, res) => {
  try {
    const ok = spawnWorkerDetached();
    if (!ok) return res.status(500).json({ ok: false, error: "Falha ao iniciar Python" });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

function markBatchTimeout(batch) {
  try {
    db.prepare("UPDATE job_items SET status='failed', mensagem='Timeout 60s' WHERE batch_id=? AND status IN ('pending','running')").run(batch);
    db.prepare("UPDATE execucoes SET status='error', finalizado_em=CURRENT_TIMESTAMP WHERE batch_id=?").run(batch);
    db.prepare("INSERT INTO logs_verificacao (nivel,mensagem,detalhes,batch_id) VALUES ('ERROR','Timeout do worker >60s','watchdog', ?)").run(batch);
    const pend = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE status='pending'").get().c;
    const running = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status='running' AND finalizado_em IS NULL").get().c;
    if (pend > 0 && running === 0) { try { spawnWorkerDetached(); } catch {} }
  } catch {}
}

function watchdog() {
  try {
    const rowsRun = db.prepare("SELECT batch_id, strftime('%s', heartbeat_at) as hb FROM execucoes WHERE status='running' AND finalizado_em IS NULL").all();
    const now = Math.floor(Date.now() / 1000);
    for (const r of rowsRun) {
      const lastLog = db.prepare("SELECT strftime('%s', MAX(timestamp)) as ts FROM logs_verificacao WHERE batch_id=?").get(r.batch_id);
      const ts = Math.max(Number(r.hb || 0), Number((lastLog && lastLog.ts) || 0));
      if (!ts || now - ts > 60) { markBatchTimeout(r.batch_id); }
    }
    const rowsQueued = db.prepare("SELECT batch_id, strftime('%s', iniciado_em) as st FROM execucoes WHERE status='queued' AND finalizado_em IS NULL").all();
    for (const r of rowsQueued) {
      const ts = Number(r.st || 0);
      if (ts && now - ts > 60) { markBatchTimeout(r.batch_id); }
    }
  } catch {}
}

setInterval(watchdog, 5000);

function ensureWorker() {
  try {
    const running = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status='running' AND finalizado_em IS NULL").get().c;
    const pend = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE status='pending'").get().c;
    const queued = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status='queued' AND finalizado_em IS NULL").get().c;
    if (running === 0 && (pend > 0 || queued > 0)) { try { spawnWorkerDetached(); } catch {} }
  } catch {}
}

setInterval(ensureWorker, 10000);

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.post("/backup-reset", (req, res) => {
  try {
    const running = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status='running' AND finalizado_em IS NULL").get().c;
    if (running > 0) return res.status(409).json({ ok: false, error: "robô ativo" });
    const p = nodeBackupReset();
    res.json({ ok: true, backup_path: p });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

const PORT = process.env.PORT || 3745;
app.listen(PORT, () => { console.log(`http://localhost:${PORT}/`) })