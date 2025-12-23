# coding: utf-8

import streamlit as st
import pandas as pd
from modules.db import conectar_db, registrar_acao

@st.cache_data(ttl=300)
def buscar_credores_consolidados_robusto(_conexao, grupo_id=None, perfil=None):
    if perfil == 'Admin' or not grupo_id:
        query = """
        WITH CredorAgrupado AS (
            SELECT
                COALESCE(NULLIF(TRIM(cp.cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) AS chave_agrupamento,
                UPPER(TRIM(cp.requerente)) as nome_padronizado,
                TRIM(cp.cpf_credor) as cpf_padronizado,
                SUM(cp.valor_liquido_final) AS valor_total,
                COUNT(cp.id) AS qtd_creditos,
                GROUP_CONCAT(DISTINCT cp.requerido_1) AS devedores
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
                GROUP_CONCAT(DISTINCT cp.requerido_1) AS devedores
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
            COALESCE(gcr.status_relacionamento, 'Nao Contatado') AS "Status do Relacionamento"
        FROM CredorAgrupado ca
        LEFT JOIN GestaoCredores gcr ON ca.chave_agrupamento = gcr.chave_agrupamento
        ORDER BY ca.valor_total DESC
        """
        return pd.read_sql_query(query, _conexao, params=(grupo_id,))

st.set_page_config(page_title="Mesa de Negociacao", layout="wide")
st.title("Mesa de Negociacao (Kanban de Relacionamento)")
st.markdown("Acompanhe o andamento dos relacionamentos em cada etapa do funil.")

try:
    conexao = conectar_db()
except Exception as e:
    st.error(f"Falha critica ao conectar ao banco de dados: {e}")
    st.stop()

if 'authentication_status' not in st.session_state or not st.session_state.get('authentication_status'):
    st.warning("Por favor, faca o login para acessar esta pagina.")
    st.stop()

usuario_logado = st.session_state.get('username', 'N/A')

registrar_acao(
    conexao,
    'VISUALIZACAO_PAGINA',
    "Acessou/Recarregou a Mesa de Negociacao.",
    {'nome_usuario': usuario_logado, 'pagina_origem': '2_Mesa_de_Negociacao.py'}
)

perfil_atual = st.session_state.get('perfil')
grupo_atual = st.session_state.get('grupo_id')

# Gate: não-admin sem grupo não pode visualizar a mesa
if perfil_atual != 'Admin' and not grupo_atual:
    st.error("Conteúdo indisponível.")
    st.stop()

with st.spinner("Carregando credores..."):
    df_credores = buscar_credores_consolidados_robusto(conexao, grupo_atual, perfil_atual)

if df_credores.empty:
    st.info("Nenhum credor para exibir.")
    st.stop()

df_credores.rename(columns={
    "Credor": "Nome",
    "Documento": "CPF/CNPJ",
    "Valor Total": "Valor",
    "Qtd. Creditos": "Qtd",
    "Status do Relacionamento": "Status"
}, inplace=True)

status_list = [
    "Nao Contatado",
    "Primeiro Contato",
    "Follow-up",
    "Em Negociacao Ativa",
    "Relacionamento Pausado"
]

cols = st.columns(len(status_list))

for idx, status in enumerate(status_list):
    with cols[idx]:
        st.markdown(f"### {status}")
        df_status = df_credores[df_credores['Status'] == status]
        
        if df_status.empty:
            st.info("Nenhum credor")
        else:
            for _, row in df_status.iterrows():
                with st.container():
                    st.markdown(f"**{row['Nome']}**")
                    st.caption(f"CPF/CNPJ: {row['CPF/CNPJ']}")
                    st.caption(f"Valor: R$ {row['Valor']:,.2f}")
                    st.caption(f"Qtd: {row['Qtd']}")
                    
                    if st.button(f"Abrir Dossie", key=f"btn_{row['chave_agrupamento']}"):
                        registrar_acao(
                            conexao,
                            'NAVEGACAO_PARA_Dossie',
                            f"Clicou para abrir o dossie do credor.",
                            {
                                'nome_usuario': usuario_logado,
                                'pagina_origem': '2_Mesa_de_Negociacao.py',
                                'chave_agrupamento_credor': row['chave_agrupamento']
                            }
                        )
                        st.session_state['chave_credor_selecionado'] = row['chave_agrupamento']
                        st.switch_page("pages/3_Dossie_do_Credor.py")
                    
                    st.markdown("---")

conexao.close()
