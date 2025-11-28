import os
import time
import logging
from database import DatabaseManager
import sys
from projudi_extractor import ProjudiExtractor

class DBLogHandler(logging.Handler):
    def __init__(self, db_manager, batch_id, worker_id=""):
        super().__init__()
        self.db_manager = db_manager
        self.batch_id = batch_id
        self.worker_id = worker_id or (f"py-{os.getpid()}")
        self.setLevel(logging.INFO)
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    def emit(self, record):
        try:
            self.db_manager.registrar_log(record.levelname, record.getMessage(), None, self.batch_id, self.worker_id)
            self.db_manager.atualizar_execucao_heartbeat(self.batch_id)
        except Exception:
            pass

def run():
    db = DatabaseManager()
    logger = logging.getLogger("worker")
    logger.setLevel(logging.INFO)
    worker_id = os.environ.get("WORKER_ID", "") or f"py-{os.getpid()}"
    batch_limit = None
    try:
        argv = sys.argv[1:]
        if "--batch" in argv:
            i = argv.index("--batch")
            if i >= 0 and i + 1 < len(argv):
                batch_limit = argv[i + 1]
    except Exception:
        batch_limit = None
    while True:
        pendentes = db.obter_itens_pendentes(batch_id=batch_limit) if batch_limit else db.obter_itens_pendentes()
        if not pendentes:
            time.sleep(2)
            continue
        por_batch = {}
        for it in pendentes:
            b = it.get('batch_id','')
            if b:
                por_batch.setdefault(b, []).append(it)
        for batch_id, itens in por_batch.items():
            batch_logger = logging.getLogger(f"worker.{batch_id}")
            batch_logger.setLevel(logging.INFO)
            h = DBLogHandler(db, batch_id, worker_id)
            batch_logger.addHandler(h)
            try:
                batch_logger.info(f"Iniciando processamento do batch {batch_id} com {len(itens)} itens")
                db.atualizar_execucao_status(batch_id, 'running')
                exec_info = db.obter_execucao_por_batch(batch_id) or {}
                usuario = (exec_info.get('usuario_projudi') or "") or os.environ.get("PROJUDI_USERNAME", "") or db.get_config("PROJUDI_USERNAME")
                senha = (db.get_password(usuario) if usuario else "") or os.environ.get("PROJUDI_PASSWORD", "") or db.get_config("PROJUDI_PASSWORD")
                if not usuario or not senha:
                    for it in itens:
                        db.atualizar_item_status(it['id'], 'failed', 'Credenciais ausentes')
                    batch_logger.error("Credenciais ausentes")
                    continue
                extrator = ProjudiExtractor(logger=logging.getLogger(f"worker.{batch_id}.projudi_extractor"), batch_id=batch_id)
                modo = str(exec_info.get('navegador_modo') or 'headless').lower()
                headless = False if modo == 'visible' else True
                ok = extrator.configurar_driver(headless=headless)
                if not ok:
                    for it in itens:
                        db.atualizar_item_status(it['id'], 'failed', 'Falha ao configurar navegador')
                    batch_logger.error("Falha ao configurar navegador para o batch")
                    continue
                ok = extrator.realizar_login(usuario, senha)
                if not ok:
                    for it in itens:
                        db.atualizar_item_status(it['id'], 'failed', 'Falha no login')
                    extrator.fechar_driver()
                    batch_logger.error("Falha no login")
                    continue
                for it in itens:
                    db.atualizar_execucao_heartbeat(batch_id)
                    batch_logger.info("heartbeat")
                    if not db.tentar_iniciar_item(it['id']):
                        continue
                    numero = it.get('numero_processo','')
                    ident = it.get('identificador','')
                    r = extrator.verificar_protocolizacao(numero, ident)
                    status = "Protocolizada" if r.get('encontrado') else "Não encontrada"
                    db.registrar_verificacao(
                        numero,
                        ident or "",
                        it.get('nome_arquivo',''),
                        status,
                        r.get("nome_documento", ""),
                        r.get("mensagem", ""),
                        usuario_projudi=usuario,
                        navegador_modo=modo,
                        host_execucao=os.environ.get("COMPUTERNAME", "") or os.environ.get("HOSTNAME", ""),
                        batch_id=batch_id
                    )
                    db.atualizar_item_status(it['id'], 'done', status)
                    db.incrementar_progresso(batch_id, 1)
                extrator.fechar_driver()
                cont = db.contar_status_por_batch(batch_id)
                db.finalizar_execucao(batch_id, cont['protocolizadas'], cont['nao_encontradas'])
                batch_logger.info("Batch finalizado")
            finally:
                batch_logger.removeHandler(h)
        time.sleep(1)

if __name__ == "__main__":
    run()