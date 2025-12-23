# ==============================================================================
# 8_Dossie_Coleta.py (v2.0 - Funcional e com Controle de Acesso)
# ==============================================================================
import streamlit as st
from modules.db_coleta import buscar_dados_brutos_por_cnj, limpar_cache
from modules.utils import formatar_cnj # Supondo que você tenha essa função
from modules.db import conectar_db, registrar_acao

# Acesso liberado: páginas de Admin não exigem login

# --- Configuração da Página ---
st.set_page_config(layout="wide", page_title="Dossiê da Coleta")
st.title("🔬 Dossiê Bruto da Coleta")

# Log de visualização da página
usuario_logado = st.session_state.get('username', 'N/A')
conexao_log = conectar_db()
registrar_acao(conexao_log, 'VISUALIZACAO_PAGINA', 'Acessou Dossiê Bruto da Coleta.', {'nome_usuario': usuario_logado, 'pagina_origem': '13_Admin_Dossie_Coleta.py'})

# --- Campo de Busca ---
cnj_input = st.text_input(
    "Digite o número do processo (CNJ) para ver os dados brutos coletados:",
    placeholder="Apenas números ou formatado..."
)

if cnj_input:
    # Formata o CNJ para o padrão de busca
    cnj_para_busca = formatar_cnj(cnj_input)
    registrar_acao(conexao_log, 'BUSCA_CNJ_COLETA', f"Consultou CNJ: {cnj_para_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '13_Admin_Dossie_Coleta.py', 'dados_json': {'cnj': cnj_para_busca}})
    
    with st.spinner(f"Buscando dados para o processo {cnj_para_busca}..."):
        df_processo, df_historico, df_movimentacoes = buscar_dados_brutos_por_cnj(cnj_para_busca)

    if df_processo is None:
        registrar_acao(conexao_log, 'BUSCA_SEM_RESULTADO', f"Dossiê não encontrado para CNJ: {cnj_para_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '13_Admin_Dossie_Coleta.py', 'dados_json': {'cnj': cnj_para_busca}})
        st.warning(f"Nenhum dado encontrado para o processo '{cnj_para_busca}'. Verifique o número ou execute o robô de coleta.")
    else:
        registrar_acao(conexao_log, 'BUSCA_RESULTADO', f"Dossiê encontrado para CNJ: {cnj_para_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '13_Admin_Dossie_Coleta.py', 'dados_json': {'cnj': cnj_para_busca, 'len_processo': len(df_processo) if df_processo is not None else 0, 'len_historico': len(df_historico) if df_historico is not None else 0, 'len_movimentacoes': len(df_movimentacoes) if df_movimentacoes is not None else 0}})
        st.success(f"Dossiê encontrado para o processo **{cnj_para_busca}**.")
        
        st.subheader("Dados Gerais do Processo")
        st.dataframe(df_processo, use_container_width=True)

        st.subheader("Histórico de Estados do Processo")
        st.dataframe(df_historico, use_container_width=True)

        st.subheader("Histórico de Movimentações")
        st.dataframe(df_movimentacoes, use_container_width=True)

# --- Botão de Limpeza de Cache ---
st.sidebar.divider()
if st.sidebar.button("Limpar Cache de Dados", key="limpar_cache_dossie_coleta"):
    try:
        registrar_acao(conexao_log, 'LIMPAR_CACHE_COLETA_DOSSIE', 'Limpou cache do Dossiê de Coleta.', {'nome_usuario': usuario_logado, 'pagina_origem': '13_Admin_Dossie_Coleta.py'})
    except Exception:
        pass
    limpar_cache()
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

# Fecha conexão de log
try:
    conexao_log.close()
except Exception:
    pass
