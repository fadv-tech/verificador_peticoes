import os
import logging
import streamlit as st
import pandas as pd
from database import DatabaseManager, LogManager
from projudi_extractor import ProjudiExtractor, processar_lista_arquivos
import platform
import uuid
import sys
import subprocess
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Verificador de Petições - Projudi TJGO", page_icon="📄", layout="wide")

db = DatabaseManager()
log = LogManager(db)

st.title("Verificador de Petições Protocolizadas")
st.caption("Cole os nomes dos arquivos das petições e verifique se foram protocolizadas no Projudi TJGO")
aba_verificacao, aba_resultados, aba_logs, aba_prints = st.tabs(["Verificação", "Resultados", "Logs", "Prints em tempo real"]) 

with aba_verificacao:
    input_text = st.text_area("Nomes dos arquivos (um por linha)", height=200, placeholder="5188032.43.2019.8.09.0152_9565_56790_Manifestação.pdf\n176359.51.2013.8.09.0152 Certidão optante simples nacional - 2025.pdf")

    col1, col2 = st.columns([1,1])
    with col1:
        headless = st.checkbox("Executar navegador em modo headless", value=True)
    with col2:
        st.write("")

    st.subheader("Terminal em tempo real")
    _ = st_autorefresh(interval=1500, limit=None, key="global_logs_refresh")
    logs_global = db.obter_logs_recentes(500)
    if logs_global:
        linhas = []
        for r in logs_global:
            ts = str(r.get('timestamp', ''))
            lvl = str(r.get('nivel', ''))
            bid = str(r.get('batch_id', ''))
            msg = str(r.get('mensagem', ''))
            linhas.append(f"{ts} - [{lvl}] ({bid}) {msg}")
        st.code('\n'.join(linhas))
    else:
        st.info("Sem logs ainda.")

    

def normalizar(s: str) -> str:
    return ''.join(c.lower() for c in s if c.isalnum())

class DBLogHandler(logging.Handler):
    def __init__(self, db_manager, batch_id):
        super().__init__()
        self.db_manager = db_manager
        self.batch_id = batch_id
        self.setLevel(logging.INFO)
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    def emit(self, record):
        try:
            self.db_manager.registrar_log(record.levelname, record.getMessage(), None, self.batch_id)
        except Exception:
            pass

class UIBufferHandler(logging.Handler):
    def __init__(self, buffer_list, container):
        super().__init__()
        self.buffer_list = buffer_list
        self.container = container
        self.setLevel(logging.INFO)
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    def emit(self, record):
        try:
            msg = self.format(record)
            self.buffer_list.append(msg)
            if len(self.buffer_list) > 250:
                self.buffer_list[:] = self.buffer_list[-250:]
            self.container.code('\n'.join(reversed(self.buffer_list)))
        except Exception:
            pass

st.sidebar.header("Configurações")
with st.sidebar:
    cfg_usuario_env = os.environ.get("PROJUDI_USERNAME", "")
    cfg_senha_env = os.environ.get("PROJUDI_PASSWORD", "")
    cfg_usuario_db = db.get_config("PROJUDI_USERNAME")
    cfg_senha_db = db.get_config("PROJUDI_PASSWORD")
    usuario_cfg = cfg_usuario_env or cfg_usuario_db
    senha_cfg = cfg_senha_env or cfg_senha_db

    usuario_input = st.text_input("Usuário Projudi", value=usuario_cfg)
    senha_input = st.text_input("Senha Projudi", value=senha_cfg, type="password")

    if st.button("Salvar credenciais", use_container_width=True):
        if usuario_input and senha_input:
            db.set_config("PROJUDI_USERNAME", usuario_input)
            db.set_config("PROJUDI_PASSWORD", senha_input)
            st.success("Credenciais salvas no banco de dados.")
        else:
            st.error("Informe usuário e senha.")

    status_txt = "Definidas" if (usuario_cfg and senha_cfg) else "Ausentes"
    st.caption(f"Status das credenciais: {status_txt}")

with aba_verificacao:
    if st.button("Verificar petições", type="primary", use_container_width=True):
        batch_id = f"{uuid.uuid4().hex[:8]}"
        logger = logging.getLogger(f"exec.{batch_id}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        st.session_state['buffer_logs'] = []
        logger.addHandler(DBLogHandler(db, batch_id))
        logger.addHandler(UIBufferHandler(st.session_state['buffer_logs'], st.session_state.get('painel_logs_container', st.empty())))
        logger.info("Iniciando verificação de petições")
        linhas = [l for l in input_text.splitlines() if l.strip()]
        if not linhas:
            st.error("Informe ao menos um nome de arquivo.")
            for h in list(logger.handlers):
                logger.removeHandler(h)
            st.stop()

        itens = processar_lista_arquivos(linhas)
        if not itens:
            st.error("Nenhuma linha pôde ser interpretada. Verifique o formato dos nomes.")
            for h in list(logger.handlers):
                logger.removeHandler(h)
            st.stop()

        usuario = os.environ.get("PROJUDI_USERNAME", "") or db.get_config("PROJUDI_USERNAME")
        senha = os.environ.get("PROJUDI_PASSWORD", "") or db.get_config("PROJUDI_PASSWORD")
        if not usuario or not senha:
            st.error("Credenciais do Projudi ausentes. Configure-as na aba Configurações.")
            for h in list(logger.handlers):
                logger.removeHandler(h)
            st.stop()
        db.iniciar_execucao(batch_id, usuario, ("headless" if headless else "gui"), platform.node(), len(itens))
        db.adicionar_itens_execucao(batch_id, itens)
        logger.info(f"Execução criada: {batch_id} com {len(itens)} itens")
        st.success(f"Execução criada. Acompanhe na aba Logs. Batch: {batch_id}")
        st.session_state['ultimo_batch_id'] = batch_id
        st.info("Para processamento em background, inicie o worker na aba Logs.")
        for h in list(logger.handlers):
            logger.removeHandler(h)

with aba_logs:
    st.subheader("Execuções")
    ativas = db.execucoes_ativas()
    st.caption(f"Execuções ativas: {ativas}")
    colW1, colW2 = st.columns([1,1])
    with colW1:
        if st.button("Iniciar worker (background)", use_container_width=True):
            try:
                subprocess.Popen([sys.executable, "verificador_peticoes/src/worker.py"], close_fds=True)
                st.success("Worker iniciado em background.")
            except Exception as e:
                st.error(f"Falha ao iniciar worker: {e}")
    with colW2:
        if st.button("Atualizar lista de execuções", use_container_width=True):
            try:
                st.rerun()
            except Exception:
                st.experimental_rerun()
    execucoes = db.obter_execucoes(500)
    df_exec = pd.DataFrame(execucoes)
    if not df_exec.empty:
        st.dataframe(df_exec, use_container_width=True)
        ids = [e.get('batch_id','') for e in execucoes if e.get('batch_id')]
        sel = st.selectbox("Selecione uma execução", options=ids)
        if sel:
            st.subheader("Logs da execução")
            logs = db.obter_logs_por_batch(sel)
            df_logs = pd.DataFrame(logs)
            if not df_logs.empty:
                st.dataframe(df_logs, use_container_width=True)
                st.download_button("Baixar logs (CSV)", df_logs.to_csv(index=False).encode('utf-8'), f"logs_{sel}.csv", "text/csv", use_container_width=True)
            st.subheader("Resultados da execução")
            verifs = db.obter_verificacoes_por_batch(sel)
            df_ver = pd.DataFrame(verifs)
            if not df_ver.empty:
                st.dataframe(df_ver, use_container_width=True)
                st.download_button("Baixar resultados (CSV)", df_ver.to_csv(index=False).encode('utf-8'), f"verificacoes_{sel}.csv", "text/csv", use_container_width=True)
            colA, colB = st.columns(2)
            with colA:
                if st.button("Finalizar execução selecionada", use_container_width=True):
                    ok = db.finalizar_execucao_forcada(sel)
                    if ok:
                        st.success("Execução finalizada.")
                    else:
                        st.error("Falha ao finalizar execução.")
    else:
        st.info("Nenhuma execução encontrada.")

with aba_resultados:
    st.subheader("Resultados salvos")
    filtro = st.text_input("Filtro (processo, arquivo, mensagem)", value="")
    limite = st.number_input("Limite", min_value=1, value=1000, step=10)
    ativas = db.execucoes_ativas()
    confirmar = st.checkbox("Confirmar limpeza do banco")
    btn_limpar = st.button("Limpar banco (backup e recriar)", type="secondary", use_container_width=True, disabled=(not confirmar))
    if btn_limpar:
        finalizadas = db.finalizar_todas_execucoes_ativas()
        if finalizadas > 0:
            st.info(f"Finalizadas {finalizadas} execuções ativas antes do backup.")
        caminho = db.backup_e_reset()
        if caminho:
            st.success(f"Backup criado em {caminho}. Banco reiniciado.")
            try:
                st.rerun()
            except Exception:
                st.experimental_rerun()
        else:
            st.error("Falha ao criar backup e reiniciar banco.")
    if ativas > 0:
        st.warning("Há execuções ativas. Finalize antes de limpar o banco.")
        colX, colY = st.columns([1,1])
        with colX:
            if st.button("Finalizar todas execuções ativas", use_container_width=True):
                n = db.finalizar_todas_execucoes_ativas()
                if n > 0:
                    st.success(f"Finalizadas {n} execuções ativas.")
                else:
                    st.info("Nenhuma execução ativa foi finalizada.")
        with colY:
            if st.button("Atualizar estado", use_container_width=True):
                try:
                    st.rerun()
                except Exception:
                    st.experimental_rerun()
    if st.button("Atualizar resultados", use_container_width=True):
        pass
    dados = db.obter_verificacoes_recentes(int(limite))
    df_all = pd.DataFrame(dados)
    if not df_all.empty:
        colmap = {
            'data_verificacao': 'data',
            'numero_processo': 'processo',
            'identificador_peticao': 'identificador',
            'nome_arquivo_original': 'arquivo',
            'status_verificacao': 'status',
            'peticao_encontrada': 'documento',
            'detalhes': 'mensagem',
            'usuario_projudi': 'usuario',
            'navegador_modo': 'navegador',
            'host_execucao': 'host',
            'batch_id': 'batch'
        }
        df_all = df_all.rename(columns=colmap)
        ultimo = st.session_state.get('ultimo_batch_id', '')
        if 'batch' in df_all.columns:
            df_all['batelada'] = df_all['batch'].apply(lambda x: 'Atual' if x == ultimo and x else '')
            cols = ['batelada'] + [c for c in df_all.columns if c != 'batelada']
            df_all = df_all[cols]
            df_all = df_all.sort_values(by=['batelada', 'data'], ascending=[False, False])
        if filtro.strip():
            f = filtro.strip().lower()
            df_all = df_all[df_all.apply(lambda r: any(f in str(v).lower() for v in r.values), axis=1)]
        st.dataframe(df_all, use_container_width=True)
        csv_all = df_all.to_csv(index=False).encode('utf-8')
        st.download_button("Baixar todos (CSV)", csv_all, "verificacoes_todas.csv", "text/csv", use_container_width=True)
    else:
        st.info("Nenhum resultado encontrado.")
    pass

with aba_prints:
    st.subheader("Prints em tempo real")
    with st.expander("Prints em tempo real (sessão)", expanded=False):
        st.caption("Logs (mais recentes no topo, até 250 linhas)")
        painel_logs = st.empty()
        st.session_state['painel_logs_container'] = painel_logs
        if 'buffer_logs' not in st.session_state:
            st.session_state['buffer_logs'] = []