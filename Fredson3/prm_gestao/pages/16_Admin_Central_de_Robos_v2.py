# ==============================================================================
# CENTRAL DE ROBOS - Controle e Monitoramento v2.0
# ==============================================================================

import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import subprocess
import os
import signal
import time
import sys

# Configuracao da pagina
st.set_page_config(
    page_title="Central de Robos",
    page_icon="🤖",
    layout="wide"
)

# ==============================================================================
# CONFIGURACOES
# ==============================================================================

BANCO_ESTRATEGICO = 'precatorios_estrategico.db'
BANCO_PROCESSOS = 'processos_v2.db'

ROBOS = {
    "projudi": {
        "nome": "Robo PROJUDI",
        "descricao": "Coleta processos do PROJUDI",
        "script": "robo_final_com_banco_v3.py",
        "icone": "[PROJUDI]",
        "tem_config": True
    },
    "analisador": {
        "nome": "Analisador de PDFs",
        "descricao": "Extrai valores dos PDFs baixados",
        "script": "analisador_pdf_v2.py",
        "icone": "[PDF]",
        "tem_config": False
    }
}

# ==============================================================================
# FUNCOES DE CONTROLE
# ==============================================================================

def verificar_status_robo(robo_id):
    """Verifica se o robo esta rodando"""
    pid_file = f".{robo_id}_pid.txt"
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        try:
            if os.name == 'nt':  # Windows
                result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'], capture_output=True, text=True)
                if str(pid) in result.stdout:
                    return "[ON] Rodando", pid
                else:
                    os.remove(pid_file)
                    return "[OFF] Parado", None
            else:  # Linux/Mac
                os.kill(pid, 0)  # Verifica se processo existe
                return "[ON] Rodando", pid
        except (OSError, Exception):
            if os.path.exists(pid_file):
                os.remove(pid_file)
            return "[OFF] Parado", None
    return "[OFF] Parado", None

def iniciar_robo(robo_id, config=None):
    """Inicia o robo"""
    script = ROBOS[robo_id]["script"]
    if not os.path.exists(script):
        return False, f"Script {script} nao encontrado"
    
    try:
        # Arquivo de log
        log_file = f"{robo_id}_log.txt"
        
        # Monta comando
        cmd = [sys.executable, script]
        
        # Adiciona parametros se for PROJUDI
        if robo_id == "projudi" and config:
            if config.get('quantidade'):
                cmd.extend(['--quantidade', str(config['quantidade'])])
            if config.get('palavras_positivas'):
                cmd.extend(['--palavras-positivas', config['palavras_positivas']])
            if config.get('palavras_negativas'):
                cmd.extend(['--palavras-negativas', config['palavras_negativas']])
            if config.get('manter_aberto'):
                cmd.append('--manter-aberto')
        
        # Inicia processo em background completamente desacoplado
        with open(log_file, 'a', encoding='utf-8') as log:
            if os.name == 'nt':  # Windows
                # Cria processo completamente independente no Windows
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW,
                    close_fds=False,
                    stdin=subprocess.DEVNULL
                )
            else:  # Linux/Mac
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                    stdin=subprocess.DEVNULL
                )
        
        # Salva PID
        pid_file = f".{robo_id}_pid.txt"
        with open(pid_file, 'w') as f:
            f.write(str(process.pid))
        
        return True, f"Robo iniciado (PID: {process.pid})"
    except Exception as e:
        return False, f"Erro ao iniciar: {str(e)}"

def parar_robo(robo_id):
    """Para o robo"""
    status, pid = verificar_status_robo(robo_id)
    if pid:
        try:
            if os.name == 'nt':  # Windows
                # Mata processo e todos os filhos
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)], capture_output=True)
            else:  # Linux/Mac
                try:
                    os.killpg(os.getpgid(pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass
            
            time.sleep(1)
            pid_file = f".{robo_id}_pid.txt"
            if os.path.exists(pid_file):
                os.remove(pid_file)
            return True, "Robo parado com sucesso"
        except Exception as e:
            return False, f"Erro ao parar: {str(e)}"
    return False, "Robo nao esta rodando"

def buscar_logs_robo(robo_id, linhas=500):
    """Busca logs do robo"""
    log_file = f"{robo_id}_log.txt"
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                todas_linhas = f.readlines()
                return ''.join(todas_linhas[-linhas:])
        except:
            try:
                with open(log_file, 'r', encoding='latin-1') as f:
                    todas_linhas = f.readlines()
                    return ''.join(todas_linhas[-linhas:])
            except:
                return "Erro ao ler log"
    return "Nenhum log disponivel"

def buscar_estatisticas_hoje():
    """Busca estatisticas de hoje"""
    hoje = datetime.now().date().isoformat()
    
    stats = {}
    
    # Estatisticas PROJUDI
    try:
        with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
            cursor = conn.cursor()
            
            # Total pendentes
            cursor.execute("SELECT COUNT(*) FROM ExtracaoDiario WHERE status_coleta IS NULL OR status_coleta IN ('pendente', 'erro')")
            stats['projudi_pendentes'] = cursor.fetchone()[0]
            
            # Processados hoje
            cursor.execute(f"SELECT COUNT(*) FROM ExtracaoDiario WHERE DATE(data_ultima_tentativa) = ?", (hoje,))
            stats['projudi_hoje'] = cursor.fetchone()[0]
            
            # Sucessos hoje
            cursor.execute(f"SELECT COUNT(*) FROM ExtracaoDiario WHERE DATE(data_ultima_tentativa) = ? AND status_coleta = 'sucesso'", (hoje,))
            stats['projudi_sucessos'] = cursor.fetchone()[0]
            
            # Erros hoje
            cursor.execute(f"SELECT COUNT(*) FROM ExtracaoDiario WHERE DATE(data_ultima_tentativa) = ? AND status_coleta = 'erro'", (hoje,))
            stats['projudi_erros'] = cursor.fetchone()[0]
    except:
        stats['projudi_pendentes'] = 0
        stats['projudi_hoje'] = 0
        stats['projudi_sucessos'] = 0
        stats['projudi_erros'] = 0
    
    # Estatisticas Analisador
    try:
        with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM CalculosPrecos")
            stats['analisador_total'] = cursor.fetchone()[0]
    except:
        stats['analisador_total'] = 0
    
    return stats

def buscar_historico_execucoes(limite=100):
    """Busca historico de execucoes"""
    try:
        with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
            df = pd.read_sql_query(f"""
                SELECT 
                    data_ultima_tentativa as data,
                    numero_processo_cnj as processo,
                    status_coleta as status,
                    mensagem_erro as erro
                FROM ExtracaoDiario 
                WHERE data_ultima_tentativa IS NOT NULL
                ORDER BY data_ultima_tentativa DESC
                LIMIT {limite}
            """, conn)
        return df
    except:
        return pd.DataFrame()

# ==============================================================================
# INTERFACE
# ==============================================================================

st.title("🤖 Central de Robos")
st.markdown("Controle e monitore os robos de coleta")

# Estatisticas gerais
st.markdown("---")
st.subheader("📊 Estatisticas de Hoje")

stats = buscar_estatisticas_hoje()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Processos Pendentes", stats['projudi_pendentes'])
with col2:
    st.metric("Processados Hoje", stats['projudi_hoje'])
with col3:
    st.metric("Sucessos", stats['projudi_sucessos'])
with col4:
    st.metric("Erros", stats['projudi_erros'])

# Area de mensagens fixa
if 'mensagens' not in st.session_state:
    st.session_state.mensagens = []

if st.session_state.mensagens:
    st.markdown("---")
    for msg in st.session_state.mensagens[-3:]:  # Ultimas 3 mensagens
        if msg['tipo'] == 'sucesso':
            st.success(msg['texto'])
        else:
            st.error(msg['texto'])

# Cards dos robos
st.markdown("---")
st.subheader("🤖 Robos")

for robo_id, robo_info in ROBOS.items():
    st.markdown(f"### {robo_info['icone']} {robo_info['nome']}")
    
    # Status
    status, pid = verificar_status_robo(robo_id)
    st.write(f"**Status:** {status}" + (f" | **PID:** {pid}" if pid else ""))
    
    # Configuracoes (apenas para PROJUDI)
    config = {}
    if robo_info.get('tem_config'):
        with st.expander("⚙️ Configuracoes", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                config['quantidade'] = st.number_input(
                    "Quantidade de processos (0 = todos)",
                    min_value=0,
                    value=10,
                    key=f"qtd_{robo_id}"
                )
                config['palavras_positivas'] = st.text_input(
                    "Palavras-chave POSITIVAS (separadas por virgula)",
                    value="cpc,calculo",
                    key=f"pos_{robo_id}"
                )
            with col2:
                config['palavras_negativas'] = st.text_input(
                    "Palavras-chave NEGATIVAS (separadas por virgula)",
                    value="cessao",
                    key=f"neg_{robo_id}"
                )
                config['manter_aberto'] = st.checkbox(
                    "Manter navegador aberto",
                    value=True,
                    key=f"aberto_{robo_id}"
                )
    
    # Botoes de controle
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("▶️ Iniciar", key=f"start_{robo_id}", use_container_width=True):
            sucesso, msg = iniciar_robo(robo_id, config if robo_info.get('tem_config') else None)
            st.session_state.mensagens.append({'tipo': 'sucesso' if sucesso else 'erro', 'texto': msg})
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()
    
    with col2:
        if st.button("⏹️ Parar", key=f"stop_{robo_id}", use_container_width=True):
            sucesso, msg = parar_robo(robo_id)
            st.session_state.mensagens.append({'tipo': 'sucesso' if sucesso else 'erro', 'texto': msg})
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()
    
    # Logs SEMPRE VISIVEIS
    st.markdown("**📋 Logs (ultimas 500 linhas):**")
    logs = buscar_logs_robo(robo_id)
    st.code(logs, language=None, line_numbers=False)
    
    st.markdown("---")

# Historico
st.markdown("---")
st.subheader("📜 Historico de Execucoes (ultimas 100)")

df_historico = buscar_historico_execucoes()
if not df_historico.empty:
    # Formata data
    df_historico['data'] = pd.to_datetime(df_historico['data']).dt.strftime('%d/%m/%Y %H:%M:%S')
    
    # Colorir status
    def colorir_status(val):
        if val == 'sucesso':
            return 'background-color: #90EE90'
        elif val == 'erro':
            return 'background-color: #FFB6C1'
        elif val == 'filtrado':
            return 'background-color: #FFD700'
        return ''
    
    st.dataframe(
        df_historico.style.map(colorir_status, subset=['status']),
        use_container_width=True,
        height=400
    )
else:
    st.info("Nenhuma execucao registrada ainda")

# Auto-refresh SEMPRE ATIVO
time.sleep(5)
if hasattr(st, "rerun"):
    st.rerun()
else:
    st.experimental_rerun()