# ==============================================================================
# db_coleta.py (v3.2 - Versão de Compatibilidade Total)
#
# Contém TODAS as funções necessárias para TODOS os painéis, incluindo
# a função antiga 'buscar_dados_brutos_por_cnj' para compatibilidade.
# ==============================================================================
import streamlit as st
import sqlite3
import pandas as pd
import os

# --- Caminho do Banco de Dados ---
PASTA_PROJETO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOME_BANCO_DADOS = os.path.join(PASTA_PROJETO, 'processos_v2.db')

# --- Funções de Busca ---

@st.cache_data(ttl=300)
def conectar_e_buscar_dados(query, params=None):
    if not os.path.exists(NOME_BANCO_DADOS):
        return pd.DataFrame()
    try:
        with sqlite3.connect(NOME_BANCO_DADOS) as conn:
            df = pd.read_sql_query(query, conn, params=params)
            return df
    except Exception as e:
        st.error(f"Erro ao conectar ou buscar dados no banco de coleta: {e}")
        return pd.DataFrame()

# ==============================================================================
# FUNÇÃO ANTIGA - MANTIDA PARA COMPATIBILIDADE
# ==============================================================================
def buscar_dados_brutos_por_cnj(cnj_buscado):
    """
    Busca dados para um CNJ específico. Retorna 3 DataFrames (processo, historico, movimentacoes).
    """
    query_processo = "SELECT * FROM Processos WHERE numero_processo = ?"
    df_processo = conectar_e_buscar_dados(query_processo, params=(cnj_buscado,))
    
    if df_processo.empty:
        return None, None, None

    processo_id = df_processo['id'].iloc[0]
    
    query_historico = "SELECT * FROM HistoricoEstado WHERE processo_id = ? ORDER BY data_coleta DESC"
    query_movimentacoes = "SELECT * FROM Movimentacoes WHERE processo_id = ? ORDER BY id DESC"
    
    df_historico = conectar_e_buscar_dados(query_historico, params=(processo_id,))
    df_movimentacoes = conectar_e_buscar_dados(query_movimentacoes, params=(processo_id,))
    
    return df_processo, df_historico, df_movimentacoes

# ==============================================================================
# FUNÇÕES NOVAS - PARA OS PAINÉIS MAIS RECENTES
# ==============================================================================
def buscar_metricas_recentes():
    query_processos = "SELECT COUNT(id), MAX(data_ultima_coleta) FROM Processos"
    query_movs = "SELECT COUNT(id) FROM Movimentacoes"
    query_decisoes = "SELECT COUNT(id) FROM Decisoes"

    df_processos = conectar_e_buscar_dados(query_processos)
    df_movs = conectar_e_buscar_dados(query_movs)
    df_decisoes = conectar_e_buscar_dados(query_decisoes)

    total_processos = df_processos.iloc[0, 0] if not df_processos.empty else 0
    ultima_execucao = pd.to_datetime(df_processos.iloc[0, 1]) if not df_processos.empty and df_processos.iloc[0, 1] else None
    total_movs = df_movs.iloc[0, 0] if not df_movs.empty else 0
    total_decisoes = df_decisoes.iloc[0, 0] if not df_decisoes.empty else 0
    
    return total_processos, total_movs, total_decisoes, ultima_execucao

def buscar_dossie_completo_por_cnj_v2(cnj: str):
    if not cnj: return None
    dossie = {}
    query_processo = """
        SELECT p.*, h.* FROM Processos p
        LEFT JOIN HistoricoEstado h ON p.id = h.processo_id
        WHERE p.numero_processo = ? ORDER BY h.data_coleta DESC LIMIT 1
    """
    df_processo = conectar_e_buscar_dados(query_processo, params=(cnj,))
    if df_processo.empty: return None
    
    dossie['processo'] = df_processo.to_dict('records')[0]
    processo_id = dossie['processo']['id']

    query_movs = "SELECT * FROM Movimentacoes WHERE processo_id = ? ORDER BY data_movimentacao DESC"
    dossie['movimentacoes'] = conectar_e_buscar_dados(query_movs, params=(processo_id,))
    
    query_decisoes = """
        SELECT m.data_movimentacao, m.descricao, d.texto_completo FROM Decisoes d
        JOIN Movimentacoes m ON d.movimentacao_id = m.id
        WHERE m.processo_id = ? ORDER BY m.data_movimentacao DESC
    """
    dossie['decisoes'] = conectar_e_buscar_dados(query_decisoes, params=(processo_id,))
    return dossie

# Função para limpar o cache
def limpar_cache():
    st.cache_data.clear()
    st.success("Cache de dados foi limpo com sucesso!")
