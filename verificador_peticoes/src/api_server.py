from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uuid
import os
import sys
import subprocess
import platform
from typing import List
from database import DatabaseManager
from projudi_extractor import processar_lista_arquivos

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = DatabaseManager()

@app.get("/")
def root():
    return {"status": "ok", "execucoes_ativas": db.execucoes_ativas()}

@app.get("/logs")
def obter_logs(limite: int = 100):
    return db.obter_logs_recentes(int(limite))

@app.get("/resultados")
def obter_resultados(limite: int = 1000):
    return db.obter_verificacoes_recentes(int(limite))

@app.post("/config/credenciais")
def configurar_credenciais(payload: dict):
    usuario = str(payload.get("usuario", ""))
    senha = str(payload.get("senha", ""))
    if not usuario or not senha:
        raise HTTPException(status_code=400, detail="Credenciais inválidas")
    db.set_config("PROJUDI_USERNAME", usuario)
    db.set_config("PROJUDI_PASSWORD", senha)
    return {"ok": True}

@app.post("/verificar")
def verificar(payload: dict):
    arquivos: List[str] = payload.get("arquivos", []) or []
    headless = bool(payload.get("headless", True))
    if not arquivos:
        raise HTTPException(status_code=400, detail="Nenhum arquivo informado")
    itens = processar_lista_arquivos(arquivos)
    if not itens:
        raise HTTPException(status_code=400, detail="Nenhum item válido")
    usuario = os.environ.get("PROJUDI_USERNAME", "") or db.get_config("PROJUDI_USERNAME")
    senha = os.environ.get("PROJUDI_PASSWORD", "") or db.get_config("PROJUDI_PASSWORD")
    if not usuario or not senha:
        raise HTTPException(status_code=400, detail="Credenciais ausentes")
    batch_id = f"{uuid.uuid4().hex[:8]}"
    db.iniciar_execucao(batch_id, usuario, ("headless" if headless else "gui"), platform.node(), len(itens))
    db.adicionar_itens_execucao(batch_id, itens)
    try:
        subprocess.Popen([sys.executable, "verificador_peticoes/src/worker.py"], close_fds=True)
    except Exception:
        pass
    return {"batch_id": batch_id, "total_itens": len(itens)}

@app.post("/worker/start")
def iniciar_worker():
    try:
        subprocess.Popen([sys.executable, "verificador_peticoes/src/worker.py"], close_fds=True)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))