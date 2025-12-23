import express from "express";
import cors from "cors";
import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";
import { spawn } from "child_process";
import { exec } from "child_process";
import fs from "fs";
import ExcelJS from "exceljs";

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
      item_id INTEGER,
      numero_processo TEXT NOT NULL,
      identificador_peticao TEXT NOT NULL,
      nome_arquivo_original TEXT NOT NULL,
      status_verificacao TEXT NOT NULL,
      peticao_encontrada TEXT,
      data_verificacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      detalhes TEXT,
      data_protocolo TEXT,
      usuario_projudi TEXT DEFAULT '',
      navegador_modo TEXT DEFAULT '',
      host_execucao TEXT DEFAULT '',
      batch_id TEXT DEFAULT '',
      UNIQUE(item_id)
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
      max_robots INTEGER,
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

    db.exec(`CREATE TABLE IF NOT EXISTS credenciais (
      usuario TEXT PRIMARY KEY,
      senha TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`);

    const vcols = db.prepare("PRAGMA table_info(verificacoes)").all().map(r => r.name);
    if (!vcols.includes("usuario_projudi")) db.exec("ALTER TABLE verificacoes ADD COLUMN usuario_projudi TEXT DEFAULT ''");
    if (!vcols.includes("navegador_modo")) db.exec("ALTER TABLE verificacoes ADD COLUMN navegador_modo TEXT DEFAULT ''");
    if (!vcols.includes("host_execucao")) db.exec("ALTER TABLE verificacoes ADD COLUMN host_execucao TEXT DEFAULT ''");
    if (!vcols.includes("batch_id")) db.exec("ALTER TABLE verificacoes ADD COLUMN batch_id TEXT DEFAULT ''");
    if (!vcols.includes("data_protocolo")) db.exec("ALTER TABLE verificacoes ADD COLUMN data_protocolo TEXT");
    if (!vcols.includes("item_id")) {
      try {
        db.exec(`CREATE TABLE IF NOT EXISTS verificacoes_new (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          item_id INTEGER,
          numero_processo TEXT NOT NULL,
          identificador_peticao TEXT NOT NULL,
          nome_arquivo_original TEXT NOT NULL,
          status_verificacao TEXT NOT NULL,
          peticao_encontrada TEXT,
          data_verificacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          detalhes TEXT,
          data_protocolo TEXT,
          usuario_projudi TEXT DEFAULT '',
          navegador_modo TEXT DEFAULT '',
          host_execucao TEXT DEFAULT '',
          batch_id TEXT DEFAULT '',
          UNIQUE(item_id)
        )`);
        db.exec(`INSERT INTO verificacoes_new (id, numero_processo, identificador_peticao, nome_arquivo_original, status_verificacao, peticao_encontrada, data_verificacao, detalhes, data_protocolo, usuario_projudi, navegador_modo, host_execucao, batch_id)
                 SELECT id, numero_processo, identificador_peticao, nome_arquivo_original, status_verificacao, peticao_encontrada, data_verificacao, detalhes, data_protocolo, COALESCE(usuario_projudi,''), COALESCE(navegador_modo,''), COALESCE(host_execucao,''), COALESCE(batch_id,'') FROM verificacoes`);
        db.exec("DROP TABLE verificacoes");
        db.exec("ALTER TABLE verificacoes_new RENAME TO verificacoes");
      } catch {}
    }

    const lcols = db.prepare("PRAGMA table_info(logs_verificacao)").all().map(r => r.name);
    if (!lcols.includes("batch_id")) db.exec("ALTER TABLE logs_verificacao ADD COLUMN batch_id TEXT");
    if (!lcols.includes("worker_id")) db.exec("ALTER TABLE logs_verificacao ADD COLUMN worker_id TEXT");

    const ecols = db.prepare("PRAGMA table_info(execucoes)").all().map(r => r.name);
    if (!ecols.includes("status")) db.exec("ALTER TABLE execucoes ADD COLUMN status TEXT DEFAULT 'pending'");
    if (!ecols.includes("progress")) db.exec("ALTER TABLE execucoes ADD COLUMN progress INTEGER DEFAULT 0");
    if (!ecols.includes("heartbeat_at")) db.exec("ALTER TABLE execucoes ADD COLUMN heartbeat_at TIMESTAMP");
    if (!ecols.includes("max_robots")) db.exec("ALTER TABLE execucoes ADD COLUMN max_robots INTEGER");
    db.exec("UPDATE execucoes SET status='queued' WHERE status='pending' AND finalizado_em IS NULL");

    try {
      const urow = db.prepare("SELECT valor FROM config WHERE chave='PROJUDI_USERNAME'").get();
      const prow = db.prepare("SELECT valor FROM config WHERE chave='PROJUDI_PASSWORD'").get();
      const uenv = process.env.PROJUDI_USERNAME || "";
      const penv = process.env.PROJUDI_PASSWORD || "";
      if (uenv && !urow) db.prepare("INSERT OR REPLACE INTO config (chave,valor) VALUES ('PROJUDI_USERNAME', ?)").run(uenv);
      if (penv && !prow) db.prepare("INSERT OR REPLACE INTO config (chave,valor) VALUES ('PROJUDI_PASSWORD', ?)").run(penv);
      const haveCreds = db.prepare("SELECT COUNT(*) as c FROM credenciais").get().c;
      const cu = (urow && urow.valor) || uenv;
      const cp = (prow && prow.valor) || penv;
      if (!haveCreds && cu && cp) db.prepare("INSERT OR REPLACE INTO credenciais (usuario, senha) VALUES (?, ?)").run(cu, cp);
    } catch {}
  } catch (e) {
    console.error("Falha ao inicializar DB:", e);
  }
}

initDB();
reconcileVerificacoes();
recoverStuckItems();

const clients = new Set();
let sseTimer = null;
let robotsMonitor = null;
function reconcileVerificacoes() {
  try {
    const items = db.prepare("SELECT id, batch_id, numero_processo, identificador, nome_arquivo FROM job_items WHERE status='done'").all();
    for (const it of items) {
      const ex = db.prepare("SELECT 1 FROM verificacoes WHERE item_id=?").get(it.id);
      if (ex) continue;
      const ex2 = db.prepare("SELECT 1 FROM verificacoes WHERE batch_id=? AND nome_arquivo_original=?").get(it.batch_id, it.nome_arquivo);
      if (ex2) {
        try { db.prepare("UPDATE verificacoes SET item_id=? WHERE batch_id=? AND nome_arquivo_original=? AND item_id IS NULL").run(it.id, it.batch_id, it.nome_arquivo); } catch {}
        continue;
      }
      const base = db.prepare("SELECT * FROM verificacoes WHERE batch_id=? AND numero_processo=? AND identificador_peticao=? LIMIT 1").get(it.batch_id, it.numero_processo, it.identificador);
      if (!base) continue;
      try {
        db.prepare(`INSERT INTO verificacoes (item_id, numero_processo, identificador_peticao, nome_arquivo_original, status_verificacao, peticao_encontrada, detalhes, data_protocolo, usuario_projudi, navegador_modo, host_execucao, batch_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
          .run(it.id, base.numero_processo, base.identificador_peticao, it.nome_arquivo, base.status_verificacao, base.peticao_encontrada, base.detalhes, base.data_protocolo, base.usuario_projudi || '', base.navegador_modo || '', base.host_execucao || '', base.batch_id);
      } catch {}
    }
    const toDelete = db.prepare(`SELECT v1.id as id_null
                                 FROM verificacoes v1
                                 WHERE v1.item_id IS NULL AND EXISTS (
                                   SELECT 1 FROM verificacoes v2
                                   WHERE v2.batch_id=v1.batch_id AND v2.nome_arquivo_original=v1.nome_arquivo_original AND v2.item_id IS NOT NULL
                                 )`).all();
    for (const r of toDelete) { try { db.prepare("DELETE FROM verificacoes WHERE id=?").run(r.id_null); } catch {} }
    const toDelete2 = db.prepare(`SELECT v1.id as id_null
                                  FROM verificacoes v1
                                  WHERE v1.item_id IS NULL AND EXISTS (
                                    SELECT 1 FROM verificacoes v2
                                    WHERE v2.batch_id=v1.batch_id AND v2.numero_processo=v1.numero_processo AND v2.identificador_peticao=v1.identificador_peticao AND v2.item_id IS NOT NULL
                                  )`).all();
    for (const r of toDelete2) { try { db.prepare("DELETE FROM verificacoes WHERE id=?").run(r.id_null); } catch {} }
  } catch {}
}

function recoverStuckItems() {
  try {
    const stuck = db.prepare(`
      SELECT id, batch_id, COALESCE(updated_at, created_at) AS ts
      FROM job_items
      WHERE status='running' AND (strftime('%s','now') - strftime('%s', COALESCE(updated_at, created_at))) > 60
    `).all();
    for (const it of stuck) {
      try {
        const logs = db.prepare("SELECT mensagem FROM logs_verificacao WHERE batch_id=? AND timestamp >= DATETIME('now','-90 seconds') ORDER BY timestamp DESC LIMIT 50").all(it.batch_id).map(r => String(r.mensagem || ''));
        let heavy = false;
        for (const m of logs) {
          const mm1 = m.match(/Movimentações encontradas:\s*(\d+)/i);
          const mm2 = m.match(/Anexos coletados:\s*(\d+)/i);
          if ((mm1 && parseInt(mm1[1], 10) >= 30) || (mm2 && parseInt(mm2[1], 10) >= 60)) { heavy = true; break; }
        }
        if (heavy) continue;
        db.prepare("UPDATE job_items SET status='pending', mensagem='Watchdog: reiniciado', updated_at=CURRENT_TIMESTAMP WHERE id=?").run(it.id);
        db.prepare("UPDATE execucoes SET status='queued' WHERE batch_id=? AND status!='done'").run(it.batch_id);
      } catch {}
    }
  } catch {}
}

function reconcileExecucoesStatus() {
  try {
    const exs = db.prepare("SELECT batch_id, status FROM execucoes").all();
    for (const e of exs) {
      const pend = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE batch_id=? AND status='pending'").get(e.batch_id).c;
      const run = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE batch_id=? AND status='running'").get(e.batch_id).c;
      const done = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE batch_id=? AND status='done'").get(e.batch_id).c;
      const fail = db.prepare("SELECT COUNT(*) as c FROM job_items WHERE batch_id=? AND status='failed'").get(e.batch_id).c;
      const total = pend + run + done + fail;
      let st = e.status;
      if (run > 0) st = 'running'; else if (pend > 0) st = 'queued'; else st = 'done';
      db.prepare("UPDATE execucoes SET status=?, progress=? WHERE batch_id=?").run(st, done, e.batch_id);
    }
  } catch {}
}

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
      (SELECT COUNT(*) FROM job_items ji WHERE ji.batch_id=e.batch_id) AS processos_enviados,
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
          reconcileVerificacoes();
          recoverStuckItems();
          reconcileExecucoesStatus();
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
    const rows = db.prepare(`
      SELECT 
        ji.id AS item_id,
        COALESCE(v.numero_processo, ji.numero_processo) AS numero_processo,
        COALESCE(v.identificador_peticao, ji.identificador) AS identificador_peticao,
        COALESCE(v.nome_arquivo_original, ji.nome_arquivo) AS nome_arquivo_original,
        COALESCE(v.status_verificacao, CASE WHEN ji.status='failed' THEN 'Erro' ELSE '' END) AS status_verificacao,
        COALESCE(v.peticao_encontrada,'') AS peticao_encontrada,
        v.data_verificacao AS data_verificacao,
        COALESCE(v.detalhes,'') AS detalhes,
        COALESCE(v.data_protocolo,'') AS data_protocolo,
        COALESCE(v.usuario_projudi,'') AS usuario_projudi,
        COALESCE(v.navegador_modo,'') AS navegador_modo,
        COALESCE(v.host_execucao,'') AS host_execucao,
        ji.batch_id AS batch_id
      FROM job_items ji
      LEFT JOIN verificacoes v ON v.item_id = ji.id
      WHERE ji.batch_id=? AND ji.status!='pending'
      ORDER BY COALESCE(v.data_verificacao, ji.id) DESC
    `).all(req.params.batch);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post('/reconcile', (req, res) => {
  try {
    reconcileVerificacoes();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post('/sanitize_dates', (req, res) => {
  try {
    db.exec(`UPDATE verificacoes 
             SET data_protocolo='' 
             WHERE COALESCE(data_protocolo,'') <> '' 
               AND (
                 CAST(substr(data_protocolo,4,2) AS INTEGER) NOT BETWEEN 1 AND 12 
                 OR CAST(substr(data_protocolo,1,2) AS INTEGER) NOT BETWEEN 1 AND 31
               )`);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post('/sanitize_protocol_no_date', (req, res) => {
  try {
    db.exec("UPDATE verificacoes SET status_verificacao='Não encontrada' WHERE status_verificacao='Protocolizada' AND COALESCE(data_protocolo,'')='' ");
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post('/jobs/:batch/requeue-failed', (req, res) => {
  try {
    const batch = req.params.batch;
    db.prepare("UPDATE job_items SET status='pending', mensagem='' WHERE batch_id=? AND status='failed'").run(batch);
    db.prepare("UPDATE execucoes SET status='queued', finalizado_em=NULL WHERE batch_id=?").run(batch);
    try { db.prepare("INSERT INTO logs_verificacao (nivel,mensagem,detalhes,batch_id) VALUES ('INFO','Requeue de itens failed','admin', ?)").run(batch); } catch {}
    res.json({ ok: true });
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

function splitCods(id) {
  try {
    const m = String(id || '').match(/_(\d+)_(\d+)_/);
    if (!m) return { codproc: '', codpet: '' };
    return { codproc: m[1], codpet: m[2] };
  } catch { return { codproc: '', codpet: '' }; }
}

function fmtProtocolDate(s) {
  try {
    const v = String(s || '').trim();
    if (!v) return '';
    let dd = '', mm = '', yy = '';
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(v)) {
      const p = v.split('/'); dd = p[0]; mm = p[1]; yy = p[2];
    } else if (/^\d{2}\.\d{2}\.\d{4}$/.test(v)) {
      const p = v.split('.'); dd = p[0]; mm = p[1]; yy = p[2];
    } else if (/^\d{4}-\d{2}-\d{2}/.test(v)) {
      const p = v.slice(0,10).split('-'); yy = p[0]; mm = p[1]; dd = p[2];
    } else {
      const m = v.match(/(\d{2})[./](\d{2})[./](\d{4})/);
      if (m) { dd = m[1]; mm = m[2]; yy = m[3]; }
    }
    if (!dd || !mm || !yy) return '';
    const mdi = parseInt(mm, 10); const ddi = parseInt(dd, 10);
    if (mdi < 1 || mdi > 12 || ddi < 1 || ddi > 31) return '';
    return `${dd}/${mm}/${yy}`;
  } catch { return ''; }
}

app.get("/successes", (req, res) => {
  try {
    const limit = Number(req.query.limit || 1000);
    const rows = db.prepare(`
      SELECT * FROM verificacoes 
      WHERE status_verificacao='Protocolizada' 
      ORDER BY data_verificacao DESC 
      LIMIT ?
    `).all(limit);
    const list = rows.map(r => ({
      data_verificacao: r.data_verificacao,
      numero_processo: r.numero_processo,
      identificador_peticao: r.identificador_peticao,
      nome_arquivo_original: r.nome_arquivo_original,
      data_protocolo: r.data_protocolo,
      status_verificacao: r.status_verificacao,
      detalhes: r.detalhes,
      ...splitCods(r.identificador_peticao)
    }));
    res.json(list);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/export/successes.csv", (req, res) => {
  try {
    const limit = Number(req.query.limit || 5000);
    const rows = db.prepare(`
      SELECT * FROM verificacoes 
      WHERE status_verificacao='Protocolizada' 
      ORDER BY data_verificacao DESC 
      LIMIT ?
    `).all(limit);
    const header = ['data_verificacao','numero_processo','codproc','codpet','identificador_peticao','nome_arquivo_original','data_protocolo'];
    const lines = [header.join(',')];
    rows.forEach(r => {
      const { codproc, codpet } = splitCods(r.identificador_peticao);
      const vals = [
        r.data_verificacao || '',
        r.numero_processo || '',
        codproc || '',
        codpet || '',
        r.identificador_peticao || '',
        r.nome_arquivo_original || '',
        r.data_protocolo || ''
      ];
      const esc = vals.map(v => {
        const s = String(v).replace(/\r|\n/g,' ').replace(/"/g,'""');
        return /,|\s|"/.test(s) ? `"${s}"` : s;
      });
      lines.push(esc.join(','));
    });
    const csv = lines.join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="sucessos.csv"');
    res.send(csv);
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/export/successes.xlsx", async (req, res) => {
  try {
    const maxLimit = 25000;
    const reqLimit = Number(req.query.limit || 5000);
    const limit = Math.min(Number.isFinite(reqLimit) ? reqLimit : 5000, maxLimit);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="sucessos_codpet.xlsx"');
    const wb = new ExcelJS.stream.xlsx.WorkbookWriter({ stream: res });
    const ws = wb.addWorksheet('Sucessos');
    ws.columns = [
      { header: 'codpet', key: 'codpet', width: 16 },
      { header: 'data_protocolo', key: 'data_protocolo', width: 18 },
    ];
    const stmt = db.prepare(`
      SELECT identificador_peticao, data_protocolo
      FROM verificacoes 
      WHERE status_verificacao='Protocolizada' 
      ORDER BY data_verificacao DESC 
      LIMIT ?
    `);
    for (const r of stmt.iterate(limit)) {
      const { codpet } = splitCods(r.identificador_peticao);
      const dp = fmtProtocolDate(r.data_protocolo);
      ws.addRow({ codpet: String(codpet || ''), data_protocolo: String(dp || '') }).commit();
    }
    await wb.commit();
    res.end();
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

const isWindows = process.platform === 'win32';

function listActiveRobots() {
  return new Promise((resolve) => {
    if (!isWindows) return resolve([]);
    const ps = spawn('powershell.exe', ['-NoProfile', '-Command', "Get-CimInstance Win32_Process | Select-Object ProcessId, Name, CommandLine | ConvertTo-Json"], { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    ps.stdout.on('data', (d) => { out += d.toString(); });
    ps.on('close', () => {
      try {
        let data = [];
        const jsonStart = out.indexOf('[') >= 0 ? out.slice(out.indexOf('[')) : out;
        data = JSON.parse(jsonStart);
        const rows = (Array.isArray(data) ? data : [data]).filter(r => {
          const cl = String(r.CommandLine || '').toLowerCase();
          return cl.includes('verificador_peticoes') || cl.includes('worker.py') || cl.includes('projudi');
        }).map(r => ({ pid: r.ProcessId, name: r.Name, cmd: r.CommandLine }));
        resolve(rows);
      } catch {
        resolve([]);
      }
    });
    ps.on('error', () => resolve([]));
  });
}

function killPIDs(pids = []) {
  return new Promise((resolve) => {
    if (!isWindows || !pids.length) return resolve({ killed: 0 });
    let killed = 0;
    let pending = pids.length;
    pids.forEach((pid) => {
      const k = spawn('taskkill', ['/F', '/PID', String(pid)], { stdio: 'ignore' });
      k.on('close', () => { killed++; if (--pending === 0) resolve({ killed }); });
      k.on('error', () => { if (--pending === 0) resolve({ killed }); });
    });
  });
}

app.get('/robots', async (req, res) => {
  try {
    const rows = await listActiveRobots();
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post('/robots/kill', async (req, res) => {
  try {
    const aggressive = true; // sempre agressivo por enquanto
    const rows = await listActiveRobots();
    let targets = rows;
    if (!aggressive) targets = rows.filter(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
    const { killed } = await killPIDs(targets.map(r => r.pid));
    // Marcar execuções como finalizadas/erro
    try {
      db.exec("UPDATE job_items SET status='failed', mensagem='Finalização global' WHERE status IN ('pending','running')");
      db.exec("UPDATE execucoes SET status='error', finalizado_em=CURRENT_TIMESTAMP WHERE finalizado_em IS NULL");
      db.prepare("INSERT INTO logs_verificacao (nivel,mensagem,detalhes,batch_id) VALUES ('ERROR','Finalização agressiva','robots/kill', '')").run();
    } catch {}
    res.json({ ok: true, killed });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post('/robots/kill-workers', async (req, res) => {
  try {
    const rows = await listActiveRobots();
    const targets = rows.filter(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
    const { killed } = await killPIDs(targets.map(r => r.pid));
    res.json({ ok: true, killed });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

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
    // preservar credenciais em 'config'
  } catch {}
  initDB();
  return backupPath;
}
async function spawnWorkerDetached(batchId = "") {
  const script = path.resolve(__dirname, "..", "verificador_peticoes", "src", "worker.py");
  const pyCwd = path.resolve(__dirname, "..", "verificador_peticoes");
  let child;
  const workerId = `w-${Date.now().toString(36)}-${Math.random().toString(16).slice(2,6)}`;
  const opts = { detached: true, stdio: "ignore", windowsHide: true, cwd: pyCwd, env: { ...process.env, WORKER_ID: workerId } };
  const args = batchId ? [script, "--batch", batchId] : [script];
  // Hard guard: do not spawn if any worker process exists
  try {
    const procs = await listActiveRobots();
    const hasWorkerProc = !!procs.find(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
    if (hasWorkerProc) return false;
  } catch {}
  try { if (batchId) db.prepare("UPDATE execucoes SET status='starting' WHERE batch_id=? AND finalizado_em IS NULL").run(batchId); } catch {}
  try { child = spawn("pythonw", args, opts); } catch {}
  if (!child) { try { child = spawn("py", ["-3", ...args], opts); } catch {} }
  if (!child) { try { child = spawn("python", args, opts); } catch {} }
  if (!child) { try { child = spawn("python3", args, opts); } catch {} }
  if (!child) {
    try { if (batchId) db.prepare("UPDATE execucoes SET status='queued' WHERE batch_id=? AND finalizado_em IS NULL").run(batchId); } catch {}
    return false;
  }
  try { child.unref(); } catch {}
  return true;
}

async function spawnWorkersForBatch(batchId, robots = 1) {
  const n = 1;
  let okAny = false;
  for (let i = 0; i < n; i++) {
    const ok = await spawnWorkerDetached(batchId);
    okAny = okAny || ok;
  }
  return okAny;
}

function createBatch(files, usuarioSel, mode) {
  if (!files || !files.length) throw new Error("files vazio");
  const batch = (Math.random().toString(16).slice(2, 10));
  let usuario = usuarioSel;
  if (!usuario) {
    const usuarioRow = db.prepare("SELECT valor FROM config WHERE chave='PROJUDI_USERNAME'").get();
    usuario = usuarioRow ? usuarioRow.valor : "";
  }
  const host = process.env.COMPUTERNAME || process.env.HOSTNAME || "";
  let modo = String(mode || '').toLowerCase();
  if (modo !== 'visible') modo = 'headless';
  
  db.prepare("INSERT OR REPLACE INTO execucoes (batch_id, iniciado_em, usuario_projudi, navegador_modo, host_execucao, total_arquivos, status, progress) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, 'queued', 0)")
    .run(batch, usuario, modo, host, files.length);

  if (usuarioSel) {
    try {
      const prow = db.prepare("SELECT senha FROM credenciais WHERE usuario=?").get(usuarioSel);
      if (prow && prow.senha) {
        db.prepare("INSERT OR REPLACE INTO config (chave,valor) VALUES ('PROJUDI_USERNAME', ?)").run(usuarioSel);
        db.prepare("INSERT OR REPLACE INTO config (chave,valor) VALUES ('PROJUDI_PASSWORD', ?)").run(prow.senha);
      }
    } catch {}
  }
  try { db.prepare("UPDATE execucoes SET max_robots=1 WHERE batch_id=?").run(batch); } catch {}
  const stmt = db.prepare("INSERT INTO job_items (batch_id, nome_arquivo, numero_processo, identificador, status, mensagem) VALUES (?, ?, ?, ?, 'pending', '')");
  let inserted = 0;
  for (const f of files) {
    const s = String(f).trim();
    if (!s) continue;
    const { numero_processo, identificador } = parseCNJAndId(s);
    stmt.run(batch, s, numero_processo, identificador);
    inserted++;
  }
  return { batch_id: batch, count: inserted };
}

app.post("/enqueue", (req, res) => {
  try {
    const files = Array.isArray(req.body.files) ? req.body.files : [];
    const usuarioSel = String(req.body.usuario || '').trim();
    const mode = String(req.body.mode || '');
    const { batch_id, count } = createBatch(files, usuarioSel, mode);
    res.json({ ok: true, batch_id, count });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post("/api/v1/batch", async (req, res) => {
  try {
    const files = Array.isArray(req.body.files) ? req.body.files : [];
    const usuarioSel = String(req.body.usuario || '').trim();
    const mode = String(req.body.mode || '');
    const { batch_id, count } = createBatch(files, usuarioSel, mode);
    
    // Auto-start logic similar to /start-worker
    // First check if any worker is running (global lock)
    let workerStarted = false;
    try {
      const procs = await listActiveRobots();
      const hasWorkerProc = !!procs.find(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
      const active = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status IN ('starting','running') AND finalizado_em IS NULL").get().c;
      
      if (!hasWorkerProc && active === 0) {
         workerStarted = await spawnWorkerDetached(batch_id);
      }
    } catch (e) {
      console.error("Failed to auto-start worker:", e);
    }
    
    res.json({ 
      ok: true, 
      batch_id, 
      count, 
      worker_started: workerStarted,
      message: workerStarted ? "Batch created and worker started" : "Batch created (worker busy or queued)"
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.post("/config/credenciais", (req, res) => {
  try {
    const usuario = String(req.body.usuario || '').trim();
    const senha = String(req.body.senha || '').trim();
    if (!usuario || !senha) return res.status(400).json({ ok: false, error: 'Credenciais inválidas' });
    db.prepare("INSERT OR REPLACE INTO credenciais (usuario, senha) VALUES (?,?)").run(usuario, senha);
    db.prepare("INSERT OR REPLACE INTO config (chave,valor) VALUES ('PROJUDI_USERNAME', ?)").run(usuario);
    db.prepare("INSERT OR REPLACE INTO config (chave,valor) VALUES ('PROJUDI_PASSWORD', ?)").run(senha);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get("/config/usuarios", (req, res) => {
  try {
    const rows = db.prepare("SELECT usuario FROM credenciais ORDER BY usuario").all();
    res.json({ ok: true, usuarios: rows.map(r => r.usuario) });
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

app.post("/start-worker", async (req, res) => {
  try {
    const batchId = String(req.query.batch || (req.body && req.body.batch) || '').trim();
    const procs = await listActiveRobots();
    const hasWorkerProc = !!procs.find(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
    if (hasWorkerProc) return res.json({ ok: true });
    const active = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status IN ('starting','running') AND finalizado_em IS NULL").get().c;
    if (active > 0) return res.json({ ok: true });
    if (batchId) {
      try {
        const row = db.prepare("SELECT status FROM execucoes WHERE batch_id=?").get(batchId);
        const st = String((row && row.status) || '').toLowerCase();
        if (st === 'running' || st === 'starting') return res.json({ ok: true });
      } catch {}
      const ok = await spawnWorkerDetached(batchId);
      if (!ok) return res.status(500).json({ ok: false, error: "Falha ao iniciar Python" });
      return res.json({ ok: true });
    }
    const runAny = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status IN ('starting','running') AND finalizado_em IS NULL").get().c;
    if (runAny > 0) return res.json({ ok: true });
    const nxt = db.prepare("SELECT batch_id FROM execucoes WHERE status='queued' AND finalizado_em IS NULL ORDER BY iniciado_em ASC LIMIT 1").get();
    const bid = nxt && nxt.batch_id ? String(nxt.batch_id) : '';
    if (!bid) return res.json({ ok: true });
    const ok = await spawnWorkerDetached(bid);
    if (!ok) return res.status(500).json({ ok: false, error: "Falha ao iniciar Python" });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

function markBatchTimeout(batch) {
  try {
    db.prepare("UPDATE job_items SET status='failed', mensagem='Timeout 900s' WHERE batch_id=? AND status IN ('pending','running')").run(batch);
    db.prepare("UPDATE execucoes SET status='error', finalizado_em=CURRENT_TIMESTAMP WHERE batch_id=?").run(batch);
    db.prepare("INSERT INTO logs_verificacao (nivel,mensagem,detalhes,batch_id) VALUES ('ERROR','Timeout do worker >900s','watchdog', ?)").run(batch);
  } catch {}
}

function watchdog() {
  try {
    const rowsRun = db.prepare("SELECT batch_id, strftime('%s', heartbeat_at) as hb FROM execucoes WHERE status='running' AND finalizado_em IS NULL").all();
    const now = Math.floor(Date.now() / 1000);
    for (const r of rowsRun) {
      const lastLog = db.prepare("SELECT strftime('%s', MAX(timestamp)) as ts FROM logs_verificacao WHERE batch_id=?").get(r.batch_id);
      const ts = Math.max(Number(r.hb || 0), Number((lastLog && lastLog.ts) || 0));
      if (!ts || now - ts > 900) { markBatchTimeout(r.batch_id); }
    }
    // não marcar queued como timeout; robô paciente
  } catch {}
}

setInterval(watchdog, 5000);

 

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

// Scheduler conservador: usa estado do banco para evitar múltiplos workers simultâneos
setInterval(async () => {
  try {
    const procs = await listActiveRobots();
    const hasWorkerProc = !!procs.find(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
    if (hasWorkerProc) return;
    const runAny = db.prepare("SELECT COUNT(*) as c FROM execucoes WHERE status IN ('starting','running') AND finalizado_em IS NULL").get().c;
    if (runAny > 0) return;
    const nxt = db.prepare("SELECT batch_id, COALESCE(max_robots,1) as max_robots FROM execucoes WHERE status='queued' AND finalizado_em IS NULL ORDER BY iniciado_em ASC LIMIT 1").get();
    const bid = nxt && nxt.batch_id ? String(nxt.batch_id) : '';
    if (!bid) return;
    try { await spawnWorkerDetached(bid); } catch {}
  } catch {}
}, 8000);
async function enforceSingleWorkerProc() {
  try {
    const rows = await listActiveRobots();
    const workers = rows.filter(r => String(r.cmd || '').toLowerCase().includes('worker.py'));
    if (workers.length > 1) {
      const keep = workers[0];
      const toKill = workers.slice(1).map(r => r.pid);
      await killPIDs(toKill);
    }
  } catch {}
}

setInterval(enforceSingleWorkerProc, 5000);
