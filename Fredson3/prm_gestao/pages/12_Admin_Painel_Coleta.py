# ==============================================================================
# 7_Painel_Coleta.py (v4.3 - Correção do ValueError)
#
# Painel de análise avançado com busca profunda e controle de acesso.
# CORREÇÃO: A lógica de filtro foi ajustada para evitar o ValueError quando
# nenhuma correspondência de palavra-chave é encontrada.
# ==============================================================================
import streamlit as st
import pandas as pd
import re
import sqlite3
import os
import io
import io
from modules.db_coleta import conectar_e_buscar_dados, limpar_cache, NOME_BANCO_DADOS
from modules.db import conectar_db, registrar_acao

# Acesso liberado: páginas de Admin não exigem login

# --- Configuração da Página ---
st.set_page_config(layout="wide", page_title="Painel de Análise Jurídica")

st.title("⚖️ Painel de Análise Jurídica")
st.markdown("Busca unificada em movimentações e no conteúdo completo das decisões coletadas.")

# Log de visualização da página
usuario_logado = st.session_state.get('username', 'N/A')
conexao_log = conectar_db()
registrar_acao(
    conexao_log,
    'VISUALIZACAO_PAGINA',
    "Acessou Painel de Análise Jurídica.",
    {'nome_usuario': usuario_logado, 'pagina_origem': '12_Admin_Painel_Coleta.py'}
)

# --- Função para carregar os dados base ---
@st.cache_data(ttl=600)
def carregar_dados_base():
    query = """
        SELECT
            p.numero_processo,
            h.fase_processual,
            h.classe_judicial,
            m.data_movimentacao,
            m.descricao AS descricao_movimentacao,
            d.texto_completo AS texto_decisao
        FROM Processos p
        LEFT JOIN HistoricoEstado h ON p.id = h.processo_id AND h.id = (
            SELECT MAX(id) FROM HistoricoEstado WHERE processo_id = p.id
        )
        LEFT JOIN Movimentacoes m ON p.id = m.processo_id
        LEFT JOIN Decisoes d ON m.id = d.movimentacao_id
    """
    df = conectar_e_buscar_dados(query)
    if df is not None and not df.empty:
        df['descricao_movimentacao'] = df['descricao_movimentacao'].astype(str).fillna('')
        df['texto_decisao'] = df['texto_decisao'].astype(str).fillna('')
    return df

# --- Função para criar a coluna de contexto ---
def criar_contexto(row, regex_palavras):
    # Prioridade 1: Match no texto da decisão
    if row['texto_decisao']:
        match = re.search(regex_palavras, row['texto_decisao'], re.IGNORECASE)
        if match:
            start = max(0, match.start() - 200)
            end = min(len(row['texto_decisao']), match.end() + 200)
            trecho = row['texto_decisao'][start:end]
            trecho_highlighted = re.sub(f"({match.group(0)})", r"**\1**", trecho, flags=re.IGNORECASE)
            return f"[DECISÃO] ...{trecho_highlighted}..."

    # Prioridade 2: Match na descrição da movimentação
    match_desc = re.search(regex_palavras, row['descricao_movimentacao'], re.IGNORECASE)
    if match_desc:
        return f"[MOV.] {row['descricao_movimentacao'][:200]}"

    return row['descricao_movimentacao'][:200]


# --- Função para apagar duplicados exatos no banco ---
def apagar_duplicados_exatos():
    resumo = {}
    try:
        if not os.path.exists(NOME_BANCO_DADOS):
            return {"erro": "Arquivo de banco não encontrado."}

        with sqlite3.connect(NOME_BANCO_DADOS) as conn:
            conn.execute("BEGIN")
            cur = conn.cursor()

            # Processos
            total = cur.execute("SELECT COUNT(*) FROM Processos").fetchone()[0]
            unicos = cur.execute("SELECT COUNT(*) FROM (SELECT MIN(id) FROM Processos GROUP BY numero_processo)").fetchone()[0]
            removidos_previstos = total - unicos
            cur.execute("DELETE FROM Processos WHERE id NOT IN (SELECT MIN(id) FROM Processos GROUP BY numero_processo)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_Processos_numero ON Processos(numero_processo)")
            resumo["Processos"] = max(removidos_previstos, 0)

            # Movimentacoes
            total = cur.execute("SELECT COUNT(*) FROM Movimentacoes").fetchone()[0]
            unicos = cur.execute("SELECT COUNT(*) FROM (SELECT MIN(id) FROM Movimentacoes GROUP BY processo_id, data_movimentacao, descricao)").fetchone()[0]
            removidos_previstos = total - unicos
            cur.execute("DELETE FROM Movimentacoes WHERE id NOT IN (SELECT MIN(id) FROM Movimentacoes GROUP BY processo_id, data_movimentacao, descricao)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_Movimentacoes_unique ON Movimentacoes(processo_id, data_movimentacao, descricao)")
            resumo["Movimentacoes"] = max(removidos_previstos, 0)

            # Decisoes
            total = cur.execute("SELECT COUNT(*) FROM Decisoes").fetchone()[0]
            unicos = cur.execute("SELECT COUNT(*) FROM (SELECT MIN(id) FROM Decisoes GROUP BY movimentacao_id, texto_completo)").fetchone()[0]
            removidos_previstos = total - unicos
            cur.execute("DELETE FROM Decisoes WHERE id NOT IN (SELECT MIN(id) FROM Decisoes GROUP BY movimentacao_id, texto_completo)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_Decisoes_unique ON Decisoes(movimentacao_id, texto_completo)")
            resumo["Decisoes"] = max(removidos_previstos, 0)

            # HistoricoEstado
            total = cur.execute("SELECT COUNT(*) FROM HistoricoEstado").fetchone()[0]
            unicos = cur.execute("SELECT COUNT(*) FROM (SELECT MIN(id) FROM HistoricoEstado GROUP BY processo_id, fase_processual, classe_judicial, data_coleta)").fetchone()[0]
            removidos_previstos = total - unicos
            cur.execute("DELETE FROM HistoricoEstado WHERE id NOT IN (SELECT MIN(id) FROM HistoricoEstado GROUP BY processo_id, fase_processual, classe_judicial, data_coleta)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_HistoricoEstado_unique ON HistoricoEstado(processo_id, fase_processual, classe_judicial, data_coleta)")
            resumo["HistoricoEstado"] = max(removidos_previstos, 0)

            conn.commit()
    except Exception as e:
        resumo["erro"] = str(e)
    return resumo

# --- Carregamento dos dados ---
df_base = carregar_dados_base()

if df_base is None or df_base.empty:
    st.error("O banco de dados de coleta está vazio ou não foi possível carregar os dados. Execute o robô primeiro.")
    st.stop()

# --- Seção de Filtros na Barra Lateral ---
st.sidebar.header("Filtros de Busca")
st.sidebar.subheader("Busca por Palavras-Chave")
palavras_chave_predefinidas = ["Cessão", "RPV", "Precatório", "Bloqueio", "Penhora", "Alvará", "Pagamento", "Quitação", "Homologo"]
palavras_chave_texto = st.sidebar.text_area(
    "Busca no conteúdo das decisões e nas movimentações (uma por linha):",
    value="\n".join(palavras_chave_predefinidas),
    height=250
)
lista_palavras_chave = [palavra.strip() for palavra in palavras_chave_texto.split("\n") if palavra.strip()]

st.sidebar.subheader("Filtros Adicionais")
cnj_filtro = st.sidebar.text_input("Buscar por Número do Processo (CNJ):")
fases_disponiveis = sorted(df_base['fase_processual'].dropna().unique())
fase_selecionada = st.sidebar.multiselect("Filtrar por Fase Processual:", options=fases_disponiveis)

# ==============================================================================
# LÓGICA DE FILTRO CORRIGIDA
# ==============================================================================
df_filtrado = df_base.copy()

# 1. Aplica o filtro de palavras-chave primeiro
if lista_palavras_chave:
    regex_palavras = '|'.join(re.escape(palavra) for palavra in lista_palavras_chave)
    cond_descricao = df_filtrado['descricao_movimentacao'].str.contains(regex_palavras, case=False, na=False, regex=True)
    cond_decisao = df_filtrado['texto_decisao'].str.contains(regex_palavras, case=False, na=False, regex=True)
    df_filtrado = df_filtrado[cond_descricao | cond_decisao].copy()
    
    # 2. SÓ ENTÃO, se o resultado não for vazio, cria o contexto
    if not df_filtrado.empty:
        df_filtrado['Contexto'] = df_filtrado.apply(criar_contexto, args=(regex_palavras,), axis=1)
    else:
        # Se ficou vazio, garante que a coluna 'Contexto' exista para evitar erros posteriores
        df_filtrado['Contexto'] = None
else:
    # Se não há palavras-chave, o contexto é apenas o início da descrição
    df_filtrado['Contexto'] = df_filtrado['descricao_movimentacao'].str[:200]

# Coluna com conteúdo completo (prioriza texto da decisão; se vazio, usa descrição da movimentação)
df_filtrado['Conteudo_Completo'] = df_filtrado['texto_decisao'].combine_first(df_filtrado['descricao_movimentacao']).astype(str)

# 3. Aplica os filtros adicionais no resultado já processado
if cnj_filtro:
    df_filtrado = df_filtrado[df_filtrado['numero_processo'].str.contains(cnj_filtro, case=False, na=False)]
if fase_selecionada:
    df_filtrado = df_filtrado[df_filtrado['fase_processual'].isin(fase_selecionada)]

# Log da aplicação de filtros e resultados
try:
    registrar_acao(
        conexao_log,
        'BUSCA_COLETA',
        'Aplicou filtros no Painel de Coleta.',
        {
            'nome_usuario': usuario_logado,
            'pagina_origem': '12_Admin_Painel_Coleta.py',
            'dados_json': {
                'palavras_chave': lista_palavras_chave,
                'cnj': cnj_filtro,
                'fases': fase_selecionada,
                'processos_unicos': int(df_filtrado['numero_processo'].nunique()) if not df_filtrado.empty else 0,
                'movimentos': int(len(df_filtrado))
            }
        }
    )
except Exception:
    pass

# --- Exibição dos Resultados ---
header_col, action_col = st.columns([5, 2])
with header_col:
    st.subheader("Resultados da Análise")
with action_col:
    if st.button("Apagar duplicados exatos", help="Remove registros idênticos nas tabelas principais"):
        resultado = apagar_duplicados_exatos()
        try:
            registrar_acao(
                conexao_log,
                'DEDUP_DB',
                'Executou remoção de duplicados exatos.',
                {
                    'nome_usuario': usuario_logado,
                    'pagina_origem': '12_Admin_Painel_Coleta.py',
                    'dados_json': resultado
                }
            )
        except Exception:
            pass
        if "erro" in resultado:
            st.error(f"Falha ao apagar duplicados: {resultado['erro']}")
        else:
            st.success(
                f"Duplicados removidos — Processos: {resultado['Processos']}, Movimentações: {resultado['Movimentacoes']}, Decisões: {resultado['Decisoes']}, Histórico: {resultado['HistoricoEstado']}"
            )

if df_filtrado.empty:
    st.info("Nenhum resultado encontrado para os filtros aplicados.")
else:
    st.write(f"Encontrados **{df_filtrado['numero_processo'].nunique()}** processos únicos e **{len(df_filtrado)}** movimentações correspondentes.")
    
    colunas_para_exibir = ['numero_processo', 'data_movimentacao', 'Contexto', 'Conteudo_Completo']
    
    df_export = df_filtrado[colunas_para_exibir].reset_index(drop=True)
    st.dataframe(
        df_export,
        use_container_width=True
    )

    # Botões de download (CSV e XLSX) dos resultados filtrados
    try:
        file_ts = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        csv_bytes = df_export.to_csv(index=False).encode('utf-8-sig')

        # Escolhe engine de Excel automaticamente com fallback
        excel_engine = None
        try:
            import xlsxwriter  # noqa: F401
            excel_engine = 'xlsxwriter'
        except Exception:
            try:
                import openpyxl  # noqa: F401
                excel_engine = 'openpyxl'
            except Exception:
                excel_engine = None

        xlsx_bytes = None
        if excel_engine is not None:
            xlsx_buffer = io.BytesIO()
            with pd.ExcelWriter(xlsx_buffer, engine=excel_engine) as writer:
                df_export.to_excel(writer, index=False, sheet_name='Resultados')
            xlsx_bytes = xlsx_buffer.getvalue()

        # Também salva os arquivos localmente na pasta 'downloads_projudi'
        try:
            export_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "downloads_projudi")
            os.makedirs(export_dir, exist_ok=True)
            csv_path = os.path.join(export_dir, f"painel_coleta_resultados_{file_ts}.csv")
            with open(csv_path, "wb") as f:
                f.write(csv_bytes)
            if xlsx_bytes is not None:
                xlsx_path = os.path.join(export_dir, f"painel_coleta_resultados_{file_ts}.xlsx")
                with open(xlsx_path, "wb") as f:
                    f.write(xlsx_bytes)
            st.caption(f"Arquivos exportados também salvos em '{export_dir}'.")
        except Exception:
            pass

        col_csv, col_xlsx = st.columns(2)
        with col_csv:
            st.download_button(
                label="Baixar tabela (CSV)",
                data=csv_bytes,
                file_name=f"painel_coleta_resultados_{file_ts}.csv",
                mime="text/csv"
            )
        with col_xlsx:
            if xlsx_bytes is not None:
                st.download_button(
                    label="Baixar tabela (XLSX)",
                    data=xlsx_bytes,
                    file_name=f"painel_coleta_resultados_{file_ts}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("Para exportar XLSX, instale um engine: `pip install xlsxwriter` ou `pip install openpyxl`.")
        try:
            registrar_acao(
                conexao_log,
                'DOWNLOAD_COLETA',
                'Exportou resultados do Painel de Coleta.',
                {
                    'nome_usuario': usuario_logado,
                    'pagina_origem': '12_Admin_Painel_Coleta.py',
                    'dados_json': {
                        'processos_unicos': int(df_filtrado['numero_processo'].nunique()),
                        'movimentos': int(len(df_filtrado))
                    }
                }
            )
        except Exception:
            pass
    except Exception as e:
        st.warning(f"Falha ao preparar arquivos para download: {e}")

# --- Botão de Limpeza de Cache ---
st.sidebar.divider()
if st.sidebar.button("Limpar Cache e Recarregar Dados"):
    try:
        registrar_acao(conexao_log, 'LIMPAR_CACHE_COLETA', 'Limpou cache e recarregou dados do Painel Coleta.', {'nome_usuario': usuario_logado, 'pagina_origem': '12_Admin_Painel_Coleta.py'})
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
