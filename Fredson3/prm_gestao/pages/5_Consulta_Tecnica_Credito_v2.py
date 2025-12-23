# coding: utf-8

import streamlit as st
import pandas as pd
from modules.db import conectar_db, registrar_acao

st.set_page_config(page_title="Consulta Tecnica", layout="wide")

# ============================================================================
# CONTROLE DE ACESSO
# ============================================================================
if 'authentication_status' not in st.session_state or not st.session_state.get('authentication_status'):
    st.warning("Por favor, faca o login para acessar esta pagina.")
    st.stop()

usuario_logado = st.session_state.get('username', 'N/A')
perfil_atual = st.session_state.get('perfil')
grupo_atual = st.session_state.get('grupo_id')
# Gate: não-admin sem grupo não pode usar consulta técnica
if perfil_atual != 'Admin' and not grupo_atual:
    st.error("Conteúdo indisponível.")
    st.stop()

st.title("Consulta Tecnica de Credito")
st.markdown("Busque processos e visualize todos os dados extraidos dos PDFs")

conexao = conectar_db()

registrar_acao(
    conexao,
    'VISUALIZACAO_PAGINA',
    "Acessou Consulta Tecnica.",
    {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito.py'}
)

tab1, tab2, tab3 = st.tabs(["Buscar por CPF", "Buscar por Processo", "Buscar por Nome"])

# ============================================================================
# TAB 1: BUSCAR POR CPF - MOSTRA TODOS OS CRÉDITOS DO CPF
# ============================================================================
with tab1:
    st.subheader("Buscar por CPF/CNPJ")
    cpf_busca = st.text_input("Digite o CPF/CNPJ (com formatacao):", key="cpf_input")
    
    if st.button("Buscar", key="btn_cpf"):
        if cpf_busca:
            registrar_acao(conexao, 'BUSCA_POR_CPF', f"Buscou CPF/CNPJ: {cpf_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'cpf_cnpj': cpf_busca}})
            query_admin = """
            SELECT 
                c.id,
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.requerido,
                c.valor_liquido_final,
                c.arquivo_de_origem
            FROM CalculosPrecos c
            WHERE c.cpf_credor = ?
            ORDER BY c.numero_processo
            """
            query_group = """
            SELECT 
                c.id,
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.requerido,
                c.valor_liquido_final,
                c.arquivo_de_origem
            FROM CalculosPrecos c
            WHERE c.cpf_credor = ?
              AND c.id IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)
            ORDER BY c.numero_processo
            """
            if perfil_atual == 'Admin':
                df = pd.read_sql_query(query_admin, conexao, params=[cpf_busca])
            else:
                df = pd.read_sql_query(query_group, conexao, params=[cpf_busca, grupo_atual])
            
            if df.empty:
                registrar_acao(conexao, 'BUSCA_SEM_RESULTADO', f"Sem resultados para CPF/CNPJ: {cpf_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'cpf_cnpj': cpf_busca}})
                st.warning("Nenhum resultado encontrado")
            else:
                registrar_acao(conexao, 'BUSCA_RESULTADO', f"{len(df)} crédito(s) para CPF/CNPJ: {cpf_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'cpf_cnpj': cpf_busca, 'qtd_resultados': int(len(df)), 'valor_total': float(df['valor_liquido_final'].sum())}})
                st.success(f"✓ {len(df)} credito(s) encontrado(s) para este CPF")
                
                # Mostra resumo
                valor_total = df['valor_liquido_final'].sum()
                st.metric("Valor Total", f"R$ {valor_total:,.2f}")
                
                # Mostra cada crédito
                for idx, row in df.iterrows():
                    with st.expander(f"Processo: {row['numero_processo']} - R$ {row['valor_liquido_final']:,.2f}", expanded=False):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Dados Principais:**")
                            st.write(f"**Processo:** {row['numero_processo']}")
                            st.write(f"**CPF/CNPJ:** {row['cpf_credor']}")
                            st.write(f"**Requerente:** {row['requerente']}")
                            st.write(f"**Requerido:** {row['requerido']}")
                            st.write(f"**Valor Liquido:** R$ {row['valor_liquido_final']:,.2f}" if row['valor_liquido_final'] else "**Valor Liquido:** N/A")
                            st.write(f"**Arquivo:** {row['arquivo_de_origem']}")
                        
                        with col2:
                            st.markdown("**Dados Adicionais:**")
                            
                            query_dados = """
                            SELECT chave, valor
                            FROM DadosProcesso
                            WHERE numero_processo = ?
                            ORDER BY chave
                            """
                            df_dados = pd.read_sql_query(query_dados, conexao, params=[row['numero_processo']])
                            
                            if df_dados.empty:
                                st.info("Nenhum dado adicional")
                            else:
                                for _, dado in df_dados.iterrows():
                                    chave_formatada = dado['chave'].replace('_', ' ').title()
                                    st.write(f"**{chave_formatada}:** {dado['valor']}")

# ============================================================================
# TAB 2: BUSCAR POR PROCESSO - MOSTRA TODOS OS CRÉDITOS DO PROCESSO
# ============================================================================
with tab2:
    st.subheader("Buscar por Numero do Processo")
    processo_busca = st.text_input("Digite o numero do processo:", key="processo_input")
    
    if st.button("Buscar", key="btn_processo"):
        if processo_busca:
            registrar_acao(conexao, 'BUSCA_POR_PROCESSO', f"Buscou Processo: {processo_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'processo': processo_busca}})
            query_admin = """
            SELECT 
                c.id,
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.requerido,
                c.valor_liquido_final,
                c.arquivo_de_origem
            FROM CalculosPrecos c
            WHERE c.numero_processo = ?
            ORDER BY c.requerente
            """
            query_group = """
            SELECT 
                c.id,
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.requerido,
                c.valor_liquido_final,
                c.arquivo_de_origem
            FROM CalculosPrecos c
            WHERE c.numero_processo = ?
              AND c.id IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)
            ORDER BY c.requerente
            """
            if perfil_atual == 'Admin':
                df = pd.read_sql_query(query_admin, conexao, params=[processo_busca])
            else:
                df = pd.read_sql_query(query_group, conexao, params=[processo_busca, grupo_atual])
            
            if df.empty:
                registrar_acao(conexao, 'BUSCA_SEM_RESULTADO', f"Sem resultados para Processo: {processo_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'processo': processo_busca}})
                st.warning("Nenhum resultado encontrado")
            else:
                registrar_acao(conexao, 'BUSCA_RESULTADO', f"{len(df)} crédito(s) para Processo: {processo_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'processo': processo_busca, 'qtd_resultados': int(len(df)), 'valor_total': float(df['valor_liquido_final'].sum())}})
                # Mostra resumo do processo
                st.success(f"✓ Processo encontrado com {len(df)} credito(s)")
                
                # Valor total do processo
                valor_total = df['valor_liquido_final'].sum()
                
                col_info1, col_info2 = st.columns(2)
                with col_info1:
                    st.metric("Total de Credores", len(df))
                with col_info2:
                    st.metric("Valor Total do Processo", f"R$ {valor_total:,.2f}")
                
                st.divider()
                
                # Mostra dados adicionais do processo (uma vez só)
                st.markdown("### Dados Adicionais do Processo")
                query_dados = """
                SELECT chave, valor, data_extracao
                FROM DadosProcesso
                WHERE numero_processo = ?
                ORDER BY chave
                """
                df_dados = pd.read_sql_query(query_dados, conexao, params=[processo_busca])
                
                if not df_dados.empty:
                    col_dados1, col_dados2 = st.columns(2)
                    
                    meio = len(df_dados) // 2
                    
                    with col_dados1:
                        for _, dado in df_dados.iloc[:meio].iterrows():
                            chave_formatada = dado['chave'].replace('_', ' ').title()
                            
                            if dado['chave'] in ['valor_total_condenacao', 'amortizacao', 'custas', 'valor_bruto', 'inss', 'ir', 'honorarios']:
                                try:
                                    valor_num = float(dado['valor'])
                                    st.write(f"**{chave_formatada}:** R$ {valor_num:,.2f}")
                                except:
                                    st.write(f"**{chave_formatada}:** {dado['valor']}")
                            else:
                                st.write(f"**{chave_formatada}:** {dado['valor']}")
                    
                    with col_dados2:
                        for _, dado in df_dados.iloc[meio:].iterrows():
                            chave_formatada = dado['chave'].replace('_', ' ').title()
                            
                            if dado['chave'] in ['valor_total_condenacao', 'amortizacao', 'custas', 'valor_bruto', 'inss', 'ir', 'honorarios']:
                                try:
                                    valor_num = float(dado['valor'])
                                    st.write(f"**{chave_formatada}:** R$ {valor_num:,.2f}")
                                except:
                                    st.write(f"**{chave_formatada}:** {dado['valor']}")
                            else:
                                st.write(f"**{chave_formatada}:** {dado['valor']}")
                    
                    st.caption(f"Ultima extracao: {df_dados.iloc[0]['data_extracao']}")
                else:
                    st.info("Nenhum dado adicional encontrado")
                
                st.divider()
                
                # Mostra TODOS os créditos do processo
                st.markdown("### Credores do Processo")
                
                for idx, row in df.iterrows():
                    with st.expander(f"Credor {idx+1}: {row['requerente']} - R$ {row['valor_liquido_final']:,.2f}", expanded=True):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**CPF/CNPJ:** {row['cpf_credor']}")
                            st.write(f"**Nome:** {row['requerente']}")
                            st.write(f"**Valor Liquido:** R$ {row['valor_liquido_final']:,.2f}" if row['valor_liquido_final'] else "**Valor Liquido:** N/A")
                        
                        with col2:
                            st.write(f"**Requerido:** {row['requerido']}")
                            st.write(f"**Arquivo:** {row['arquivo_de_origem']}")

# ============================================================================
# TAB 3: BUSCAR POR NOME - MOSTRA TODOS OS CRÉDITOS DO NOME
# ============================================================================
with tab3:
    st.subheader("Buscar por Nome do Requerente")
    nome_busca = st.text_input("Digite o nome (parcial):", key="nome_input")
    
    if st.button("Buscar", key="btn_nome"):
        if nome_busca:
            registrar_acao(conexao, 'BUSCA_POR_NOME', f"Buscou Nome: {nome_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'nome': nome_busca}})
            query = """
            SELECT 
                c.id,
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.requerido,
                c.valor_liquido_final,
                c.arquivo_de_origem
            FROM CalculosPrecos c
            WHERE UPPER(c.requerente) LIKE UPPER(?)
            ORDER BY c.requerente, c.numero_processo
            LIMIT 100
            """
            query_group = """
            SELECT 
                c.id,
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.requerido,
                c.valor_liquido_final,
                c.arquivo_de_origem
            FROM CalculosPrecos c
            WHERE UPPER(c.requerente) LIKE UPPER(?)
              AND c.id IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)
            ORDER BY c.requerente, c.numero_processo
            LIMIT 100
            """
            if perfil_atual == 'Admin':
                df = pd.read_sql_query(query_admin, conexao, params=[f'%{nome_busca}%'])
            else:
                df = pd.read_sql_query(query_group, conexao, params=[f'%{nome_busca}%', grupo_atual])
            
            if df.empty:
                registrar_acao(conexao, 'BUSCA_SEM_RESULTADO', f"Sem resultados para Nome: {nome_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'nome': nome_busca}})
                st.warning("Nenhum resultado encontrado")
            else:
                registrar_acao(conexao, 'BUSCA_RESULTADO', f"{len(df)} crédito(s) para Nome: {nome_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '5_Consulta_Tecnica_Credito_v2.py', 'dados_json': {'nome': nome_busca, 'qtd_resultados': int(len(df)), 'valor_total': float(df['valor_liquido_final'].sum())}})
                st.success(f"✓ {len(df)} credito(s) encontrado(s)")
                
                # Mostra resumo
                valor_total = df['valor_liquido_final'].sum()
                st.metric("Valor Total", f"R$ {valor_total:,.2f}")
                
                # Agrupa por nome
                nomes_unicos = df['requerente'].unique()
                
                for nome in nomes_unicos:
                    df_nome = df[df['requerente'] == nome]
                    valor_nome = df_nome['valor_liquido_final'].sum()
                    
                    with st.expander(f"{nome} - {len(df_nome)} credito(s) - R$ {valor_nome:,.2f}", expanded=False):
                        st.dataframe(
                            df_nome[['numero_processo', 'cpf_credor', 'requerido', 'valor_liquido_final']],
                            use_container_width=True,
                            hide_index=True
                        )

conexao.close()
