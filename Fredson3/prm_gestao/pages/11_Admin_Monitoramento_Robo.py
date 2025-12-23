# ==============================================================================
# 9_Monitoramento_Robo.py (v2.0 - Funcional e com Controle de Acesso)
# ==============================================================================
import streamlit as st
import pandas as pd
from modules.db_coleta import buscar_metricas_recentes, limpar_cache
import time
from modules.db import conectar_db, registrar_acao

# Acesso liberado: páginas de Admin não exigem login

# --- Configuração da Página ---
st.set_page_config(layout="wide", page_title="Monitoramento do Robô")

usuario_logado = st.session_state.get('username', 'N/A')
conexao = conectar_db()
registrar_acao(conexao, 'VISUALIZACAO_PAGINA', "Acessou Monitoramento do Robô", {'nome_usuario': usuario_logado, 'pagina_origem': '11_Admin_Monitoramento_Robo.py'})

st.title("🤖 Monitoramento do Robô de Coleta")
st.markdown("Métricas de performance e status da última execução do robô de coleta.")

# --- Busca e Exibição das Métricas ---
total_processos, total_movs, total_decisoes, ultima_execucao = buscar_metricas_recentes()

if ultima_execucao is None:
    registrar_acao(conexao, 'MONITORAMENTO_SEM_DADOS', "Sem dados de execução do robô", {'nome_usuario': usuario_logado, 'pagina_origem': '11_Admin_Monitoramento_Robo.py'})
    st.warning("Nenhum dado de execução encontrado. O robô de coleta ainda não foi executado.")
    conexao.close()
    st.stop()

st.header("Visão Geral da Base de Dados")
col1, col2, col3 = st.columns(3)
col1.metric("Processos Únicos Coletados", f"{total_processos:,}".replace(",", "."))
col2.metric("Total de Movimentações", f"{total_movs:,}".replace(",", "."))
col3.metric("Total de Decisões Extraídas", f"{total_decisoes:,}".replace(",", "."))
registrar_acao(conexao, 'MONITORAMENTO_METRICAS', "Carregou métricas recentes do robô", {'nome_usuario': usuario_logado, 'pagina_origem': '11_Admin_Monitoramento_Robo.py', 'dados_json': {'processos': int(total_processos), 'movimentacoes': int(total_movs), 'decisoes': int(total_decisoes), 'ultima_execucao': str(ultima_execucao)}})

st.header("Status da Última Coleta")

# Calcula o tempo desde a última execução
agora = pd.Timestamp.now(tz='America/Sao_Paulo') # Usando um fuso horário como referência
ultima_execucao_local = ultima_execucao.tz_localize('UTC').tz_convert('America/Sao_Paulo')
diferenca = agora - ultima_execucao_local

# Formata a diferença de tempo para exibição
dias = diferenca.days
horas = diferenca.seconds // 3600
minutos = (diferenca.seconds % 3600) // 60

tempo_decorrido_str = ""
if dias > 0:
    tempo_decorrido_str += f"{dias}d "
if horas > 0:
    tempo_decorrido_str += f"{horas}h "
if minutos > 0:
    tempo_decorrido_str += f"{minutos}m"
tempo_decorrido_str = tempo_decorrido_str.strip()

col1, col2 = st.columns(2)
col1.metric(
    "Última Atividade do Robô",
    ultima_execucao_local.strftime('%d/%m/%Y %H:%M:%S')
)
col2.metric(
    "Tempo Decorrido",
    tempo_decorrido_str
)

# Indicador de status visual
if diferenca.total_seconds() < 3600: # Menos de 1 hora
    st.success("✅ O robô executou recentemente.")
elif diferenca.total_seconds() < 86400: # Menos de 24 horas
    st.info("ℹ️ O robô executou hoje.")
else:
    st.error("🚨 ATENÇÃO: O robô não executa há mais de 24 horas.")

# --- Botão de Limpeza de Cache ---
st.sidebar.divider()
if st.sidebar.button("Limpar Cache e Recarregar Dados", key="limpar_cache_monitoramento"):
    registrar_acao(conexao, 'LIMPAR_CACHE_MONITORAMENTO', "Solicitou limpar cache do monitoramento", {'nome_usuario': usuario_logado, 'pagina_origem': '11_Admin_Monitoramento_Robo.py'})
    limpar_cache()
    conexao.close()
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

conexao.close()
