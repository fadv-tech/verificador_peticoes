# coding: utf-8

import streamlit as st
import pandas as pd
from modules.db import conectar_db, registrar_acao, buscar_grupos, vincular_chave_agrupamento_a_grupo, atualizar_status_relacionamento_massa
from modules.utils import formatar_valor

st.set_page_config(page_title="Dashboard de Credores", layout="wide")

@st.cache_data(ttl=300)
def buscar_credores_consolidados_robusto(_conexao, grupo_id=None, perfil=None):
    """
    Busca credores consolidados COM a lista de processos onde tem crédito.
    Se perfil não for Admin, filtra por créditos vinculados ao grupo do usuário.
    """
    if perfil == 'Admin' or not grupo_id:
        query = """
        WITH CredorAgrupado AS (
            SELECT
                COALESCE(NULLIF(TRIM(cp.cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) AS chave_agrupamento,
                UPPER(TRIM(cp.requerente)) as nome_padronizado,
                TRIM(cp.cpf_credor) as cpf_padronizado,
                SUM(cp.valor_liquido_final) AS valor_total,
                COUNT(cp.id) AS qtd_creditos,
                GROUP_CONCAT(DISTINCT cp.requerido_1) AS devedores,
                GROUP_CONCAT(DISTINCT cp.numero_processo) AS processos
            FROM CalculosPrecos cp
            WHERE cp.requerente IS NOT NULL AND cp.requerente != ''
            GROUP BY chave_agrupamento
        )
        SELECT
            ca.chave_agrupamento,
            ca.nome_padronizado AS "Credor",
            ca.cpf_padronizado AS "Documento",
            ca.valor_total AS "Valor Total",
            ca.qtd_creditos AS "Qtd. Creditos",
            ca.devedores AS "Devedores",
            ca.processos AS "Processos",
            COALESCE(gcr.status_relacionamento, 'Nao Contatado') AS "Status do Relacionamento"
        FROM CredorAgrupado ca
        LEFT JOIN GestaoCredores gcr ON ca.chave_agrupamento = gcr.chave_agrupamento
        ORDER BY ca.valor_total DESC
        """
        return pd.read_sql_query(query, _conexao)
    else:
        query = """
        WITH CredorAgrupado AS (
            SELECT
                COALESCE(NULLIF(TRIM(cp.cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) AS chave_agrupamento,
                UPPER(TRIM(cp.requerente)) as nome_padronizado,
                TRIM(cp.cpf_credor) as cpf_padronizado,
                SUM(cp.valor_liquido_final) AS valor_total,
                COUNT(cp.id) AS qtd_creditos,
                GROUP_CONCAT(DISTINCT cp.requerido_1) AS devedores,
                GROUP_CONCAT(DISTINCT cp.numero_processo) AS processos
            FROM CalculosPrecos cp
            WHERE cp.requerente IS NOT NULL AND cp.requerente != ''
              AND cp.id IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)
            GROUP BY chave_agrupamento
        )
        SELECT
            ca.chave_agrupamento,
            ca.nome_padronizado AS "Credor",
            ca.cpf_padronizado AS "Documento",
            ca.valor_total AS "Valor Total",
            ca.qtd_creditos AS "Qtd. Creditos",
            ca.devedores AS "Devedores",
            ca.processos AS "Processos",
            COALESCE(gcr.status_relacionamento, 'Nao Contatado') AS "Status do Relacionamento"
        FROM CredorAgrupado ca
        LEFT JOIN GestaoCredores gcr ON ca.chave_agrupamento = gcr.chave_agrupamento
        ORDER BY ca.valor_total DESC
        """
        return pd.read_sql_query(query, _conexao, params=(grupo_id,))

st.title("Dashboard Geral de Credores")
st.write("Visao consolidada de todos os credores da carteira.")

conexao = conectar_db()
if 'authentication_status' not in st.session_state or not st.session_state.get('authentication_status'):
    st.warning("Por favor, faca o login para acessar esta pagina.")
    st.stop()

usuario_logado = st.session_state.get('username', 'N/A')
perfil_atual = st.session_state.get('perfil')
grupo_atual = st.session_state.get('grupo_id')

registrar_acao(
    conexao,
    'VISUALIZACAO_PAGINA',
    "Acessou/Recarregou o Dashboard Geral.",
    {'nome_usuario': usuario_logado, 'pagina_origem': '1_Dashboard_Geral.py'}
)

# Gate: não-admin sem grupo não pode visualizar o dashboard
if perfil_atual != 'Admin' and not grupo_atual:
    st.error("Conteúdo indisponível.")
    st.stop()

df_credores = buscar_credores_consolidados_robusto(conexao, grupo_atual, perfil_atual)
# Adiciona coluna de grupos para Admin
if perfil_atual == 'Admin':
    try:
        df_grupos_mapa = pd.read_sql_query("""
            SELECT
                COALESCE(NULLIF(TRIM(cp.cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) AS chave_agrupamento,
                GROUP_CONCAT(DISTINCT g.nome) AS grupos
            FROM CalculosPrecos cp
            JOIN GruposCreditos gc ON gc.credito_id = cp.id
            JOIN Grupos g ON g.id = gc.grupo_id
            WHERE cp.requerente IS NOT NULL AND cp.requerente != ''
            GROUP BY chave_agrupamento
        """, conexao)
        df_credores = df_credores.merge(df_grupos_mapa, on='chave_agrupamento', how='left')
        df_credores.rename(columns={'grupos': 'Grupos'}, inplace=True)
        df_credores['Grupos'] = df_credores['Grupos'].fillna('Sem vínculo')
    except Exception:
        pass
if df_credores.empty:
    st.info("Nenhum credor encontrado no banco de dados.")
    st.stop()

termo_busca = st.text_input("Buscar Credor por Nome, Documento, Devedor ou Processo:", placeholder="Digite para filtrar...")
df_filtrado = df_credores.copy()

if termo_busca:
    if st.session_state.get('last_search_term') != termo_busca:
        registrar_acao(
            conexao,
            'BUSCA_DADOS',
            f"Buscou pelo termo: '{termo_busca}'",
            {'nome_usuario': usuario_logado, 'pagina_origem': '1_Dashboard_Geral.py'}
        )
        st.session_state['last_search_term'] = termo_busca
    
    termo_lower = termo_busca.lower()
    df_filtrado = df_credores[
        df_credores['Credor'].str.lower().str.contains(termo_lower, na=False) |
        df_credores['Documento'].str.contains(termo_lower, na=False) |
        df_credores['Devedores'].str.lower().str.contains(termo_lower, na=False) |
        df_credores['Processos'].str.contains(termo_lower, na=False)
    ]

st.write(f"Exibindo **{len(df_filtrado)}** de **{len(df_credores)}** credores.")
if st.button("Atualizar tabela", type="secondary", key="btn_refresh_tabela"):
    st.cache_data.clear()
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df_to_csv(df_filtrado)
if st.download_button(
   label="Baixar lista como CSV",
   data=csv,
   file_name='credores_dashboard.csv',
   mime='text/csv',
   on_click=lambda: registrar_acao(
       conexao,
       'DOWNLOAD_DADOS',
       f"Fez o download de {len(df_filtrado)} registros do Dashboard.",
       {'nome_usuario': usuario_logado, 'pagina_origem': '1_Dashboard_Geral.py'}
   )
):
    pass

df_display = df_filtrado.copy()
df_display.insert(0, 'Selecionar', False)
df_display['Valor Total'] = df_display['Valor Total'].apply(formatar_valor)

if 'before_edit_df' not in st.session_state:
    st.session_state.before_edit_df = df_display.copy()

column_config = {
    "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
    "chave_agrupamento": None,
    "Processos": st.column_config.TextColumn("Processos", width="medium")
}
if perfil_atual == 'Admin' and 'Grupos' in df_display.columns:
    column_config["Grupos"] = st.column_config.TextColumn("Grupos", width="small")

edited_df = st.data_editor(
    df_display,
    column_config=column_config,
    use_container_width=True,
    hide_index=True,
    key="data_editor_credores"
)

if not st.session_state.before_edit_df.equals(edited_df):
    try:
        diff = st.session_state.before_edit_df.compare(edited_df)
        for index, row in diff.iterrows():
            credor_info = df_filtrado.loc[index]
            chave_credor = credor_info['chave_agrupamento']
            col_name = diff.columns[0][0]
            val_antigo = row[diff.columns[0]]
            val_novo = row[diff.columns[1]]
            detalhes_log = f"Tentou editar o campo '{col_name}' do credor '{credor_info['Credor']}' de '{val_antigo}' para '{val_novo}' diretamente na tabela."
            dados_log = {
                'nome_usuario': usuario_logado,
                'pagina_origem': '1_Dashboard_Geral.py',
                'chave_agrupamento_credor': chave_credor,
                'dados_json': {'campo': col_name, 'de': val_antigo, 'para': val_novo}
            }
            registrar_acao(conexao, 'EDICAO_EM_CELULA', detalhes_log, dados_log)
    except Exception:
        pass
    st.session_state.before_edit_df = edited_df.copy()

credores_selecionados = edited_df[edited_df.Selecionar]
num_selecionados = len(credores_selecionados)

if num_selecionados > 0:
    st.divider()
    st.subheader(f"Acoes para {num_selecionados} credor(es) selecionado(s)")
    
    if num_selecionados == 1:
        if st.button("Abrir Dossie do Credor Selecionado", type="primary", use_container_width=True):
            credor_info = df_filtrado.loc[credores_selecionados.index[0]]
            chave_credor = credor_info['chave_agrupamento']
            registrar_acao(
                conexao,
                'NAVEGACAO_PARA_Dossie',
                f"Clicou para abrir o dossie do credor.",
                {
                    'nome_usuario': usuario_logado,
                    'pagina_origem': '1_Dashboard_Geral.py',
                    'chave_agrupamento_credor': chave_credor
                }
            )
            st.session_state['chave_credor_selecionado'] = chave_credor
            st.switch_page("pages/3_Dossie_do_Credor.py")
    
    st.markdown("**Acoes em Massa:**")
    indices_selecionados = credores_selecionados.index
    credores_para_mover = df_filtrado.loc[indices_selecionados]
    chaves_selecionadas = credores_para_mover['chave_agrupamento'].tolist()
    
    col1, col2 = st.columns([3, 1])
    with col1:
        novo_status = st.selectbox(
            "Mover o(s) relacionamento(s) para:",
            ["Nao Contatado", "Primeiro Contato", "Follow-up", "Em Negociacao Ativa", "Relacionamento Pausado"],
            key="selectbox_status_relacionamento_massa"
        )
    with col2:
        st.write("")
        if st.button(f"Mover {len(chaves_selecionadas)} Credor(es)", use_container_width=True, type="primary"):
            if chaves_selecionadas:
                sucesso, log_operacao = atualizar_status_relacionamento_massa(conexao, credores_para_mover, novo_status)
                if sucesso:
                    # Mostra mensagem de sucesso
                    st.success(f"✓ {len(chaves_selecionadas)} credor(es) movido(s) para '{novo_status}' com sucesso!")
                    
                    # Mostra detalhes
                    with st.expander("Ver detalhes da operacao"):
                        for msg in log_operacao:
                            st.write(msg)
                    
                    # Registra no log
                    detalhes_log = f"Moveu {len(chaves_selecionadas)} credor(es) para o status '{novo_status}'."
                    dados_log = {
                        'nome_usuario': usuario_logado,
                        'pagina_origem': '1_Dashboard_Geral.py',
                        'dados_json': {
                            'chaves_afetadas': chaves_selecionadas,
                            'novo_status': novo_status
                        }
                    }
                    registrar_acao(conexao, 'RELACIONAMENTO_EM_MASSA', detalhes_log, dados_log)
                    
                    # Aguarda 2 segundos para mostrar mensagem e recarrega
                    import time
                    time.sleep(2)
                    st.cache_data.clear()  # <-- LIMPA O CACHE PARA FORÇAR DADOS ATUALIZADOS
                    if hasattr(st, "rerun"):
                        st.rerun()
                    else:
                        st.experimental_rerun()
                else:
                    st.error("Erro ao mover credores. Verifique os logs.")
    
    # Atribuição de Grupo (em massa) - apenas Admin
    if perfil_atual == 'Admin':
        st.markdown("**Atribuição de Grupo (em massa):**")
        df_grupos = buscar_grupos(conexao)
        if df_grupos.empty:
            st.info("Nenhum grupo cadastrado. Crie grupos na Central do Administrador.")
        else:
            grupos_nomes = df_grupos['nome'].tolist()
            grupos_ids = df_grupos['id'].tolist()
            idx_grupo_sel = st.selectbox("Selecionar grupo para vincular", options=list(range(len(grupos_nomes))), format_func=lambda i: grupos_nomes[i], key="selectbox_grupo_mass")
            grupo_id_sel = grupos_ids[idx_grupo_sel]
            if st.button(f"Vincular {len(chaves_selecionadas)} credor(es) ao grupo", use_container_width=True, key="btn_vinculo_mass"):
                total_vinculos = 0
                detalhes_msgs = []
                for chave in chaves_selecionadas:
                    ok, total = vincular_chave_agrupamento_a_grupo(conexao, grupo_id_sel, chave)
                    if ok:
                        total_vinculos += total
                        detalhes_msgs.append(f"{chave}: {total} crédito(s) vinculados.")
                    else:
                        detalhes_msgs.append(f"{chave}: falha ao vincular.")
                st.success(f"✓ Vínculo concluído: {len(chaves_selecionadas)} credor(es), {total_vinculos} crédito(s) afetados.")
                with st.expander("Ver detalhes da vinculação"):
                    for m in detalhes_msgs:
                        st.write(m)
                registrar_acao(conexao, 'VINCULO_GRUPO_EM_MASSA', f"Vinculou {len(chaves_selecionadas)} credor(es) ao grupo '{grupos_nomes[idx_grupo_sel]}'.", {
                    'nome_usuario': usuario_logado,
                    'pagina_origem': '1_Dashboard_Geral_v2.py',
                    'dados_json': {
                        'chaves_afetadas': chaves_selecionadas,
                        'grupo_id': grupo_id_sel,
                        'grupo_nome': grupos_nomes[idx_grupo_sel],
                        'total_creditos_afetados': total_vinculos
                    }
                })
                import time
                time.sleep(2)
                st.cache_data.clear()
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()

conexao.close()

# --- Consulta de Créditos "Em Documentação" ---
st.subheader("Créditos com Status: Em Documentação")
if st.button("Buscar Créditos 'Em Documentação'"):
    conn = conectar_db()
    if conn:
        try:
            query_doc = "SELECT * FROM GestaoCreditos WHERE status_workflow = 'Em Documentação'"
            df_doc = pd.read_sql_query(query_doc, conn)
            if not df_doc.empty:
                st.write(f"Foram encontrados {len(df_doc)} créditos com status 'Em Documentação':")
                st.dataframe(df_doc)
            else:
                st.info("Nenhum crédito encontrado com status 'Em Documentação'.")
        except Exception as e:
            st.error(f"Erro ao buscar créditos 'Em Documentação': {e}")
        finally:
            conn.close()
    else:
        st.error("Não foi possível conectar ao banco de dados.")

conexao.close()
