# coding: utf-8

import streamlit as st
import sqlite3
import pandas as pd
from modules.db import conectar_db, registrar_acao

st.set_page_config(page_title="Consultas SQL", page_icon="🔍", layout="wide")

BANCO = "precatorios_estrategico.db"

usuario_logado = st.session_state.get('username', 'N/A')
conexao_log = conectar_db()

def executar_query(query):
    try:
        conn = sqlite3.connect(BANCO)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df, None
    except Exception as e:
        return None, str(e)

st.title("🔍 Consultas SQL")
st.markdown("Queries prontas para análise dos dados")

# Log de visualização da página
registrar_acao(
    conexao_log,
    'VISUALIZACAO_PAGINA',
    "Acessou Consultas SQL.",
    {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'}
)

tab1, tab2, tab3, tab4 = st.tabs(["📊 Resumos", "🔍 Consultas", "💰 Financeiro", "📋 Export"])

# ============================================================
# TAB 1: RESUMOS
# ============================================================
with tab1:
    st.header("Resumos e Estatísticas")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📊 Resumo Geral", use_container_width=True):
            query = """
            SELECT 
                COUNT(*) as total_processos,
                COUNT(DISTINCT cpf_credor) as total_credores,
                SUM(valor_liquido_final) as valor_total,
                AVG(valor_liquido_final) as valor_medio
            FROM CalculosPrecos
            WHERE valor_liquido_final IS NOT NULL
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Resumo Geral' (Resumos).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
        
        if st.button("📈 Top 10 Maiores Valores", use_container_width=True):
            query = """
            SELECT 
                numero_processo,
                cpf_credor,
                requerente,
                valor_liquido_final
            FROM CalculosPrecos
            WHERE valor_liquido_final IS NOT NULL
            ORDER BY valor_liquido_final DESC
            LIMIT 10
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Top 10 Maiores Valores' (Resumos).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
        
        if st.button("👥 Credores com Mais Processos", use_container_width=True):
            query = """
            SELECT 
                cpf_credor,
                MAX(requerente) as requerente,
                COUNT(*) as qtd_processos,
                SUM(valor_liquido_final) as valor_total
            FROM CalculosPrecos
            WHERE cpf_credor IS NOT NULL
            GROUP BY cpf_credor
            HAVING COUNT(*) > 1
            ORDER BY qtd_processos DESC
            LIMIT 20
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Credores com Mais Processos' (Resumos).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
    
    with col2:
        if st.button("💰 Credores com Maior Valor Total", use_container_width=True):
            query = """
            SELECT 
                cpf_credor,
                MAX(requerente) as nome,
                COUNT(*) as qtd_processos,
                SUM(valor_liquido_final) as valor_total,
                AVG(valor_liquido_final) as valor_medio
            FROM CalculosPrecos
            WHERE valor_liquido_final IS NOT NULL
            GROUP BY cpf_credor
            ORDER BY valor_total DESC
            LIMIT 20
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Credores com Maior Valor Total' (Resumos).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
        
        if st.button("📑 Processos por Tipo (RPV/Precatório)", use_container_width=True):
            query = """
            SELECT 
                d.valor as tipo,
                COUNT(DISTINCT d.numero_processo) as quantidade,
                SUM(c.valor_liquido_final) as valor_total
            FROM DadosProcesso d
            JOIN CalculosPrecos c ON d.numero_processo = c.numero_processo
            WHERE d.chave = 'tipo'
            GROUP BY d.valor
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Processos por Tipo' (Resumos).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
        
        if st.button("📊 Processos por Índice de Correção", use_container_width=True):
            query = """
            SELECT 
                d.valor as indice,
                COUNT(DISTINCT d.numero_processo) as quantidade
            FROM DadosProcesso d
            WHERE d.chave = 'indice_correcao'
            GROUP BY d.valor
            ORDER BY quantidade DESC
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Processos por Índice de Correção' (Resumos).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)

# ============================================================
# TAB 2: CONSULTAS
# ============================================================
with tab2:
    st.header("Consultas Específicas")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Buscar por CPF")
        cpf_busca = st.text_input("CPF (com formatação)")
        if st.button("Buscar CPF"):
            if cpf_busca:
                query = f"""
                SELECT 
                    c.numero_processo,
                    c.cpf_credor,
                    c.requerente,
                    c.requerido,
                    c.valor_liquido_final
                FROM CalculosPrecos c
                WHERE c.cpf_credor = '{cpf_busca}'
                """
                registrar_acao(conexao_log, 'BUSCA_POR_CPF', f"Buscou CPF: {cpf_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'cpf': cpf_busca}})
                df, erro = executar_query(query)
                if erro:
                    registrar_acao(conexao_log, 'BUSCA_ERRO', f"Erro na busca por CPF: {erro}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'cpf': cpf_busca}})
                    st.error(f"Erro: {erro}")
                elif len(df) == 0:
                    registrar_acao(conexao_log, 'BUSCA_SEM_RESULTADO', f"Busca CPF sem resultados: {cpf_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'cpf': cpf_busca}})
                    st.warning("Nenhum resultado encontrado")
                else:
                    registrar_acao(conexao_log, 'BUSCA_RESULTADO', f"Busca CPF retornou {len(df)} registro(s).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'cpf': cpf_busca, 'qtd_resultados': len(df)}})
                    st.dataframe(df, use_container_width=True)
        
        st.subheader("Buscar por Nome")
        nome_busca = st.text_input("Nome (parcial)")
        if st.button("Buscar Nome"):
            if nome_busca:
                query = f"""
                SELECT 
                    numero_processo,
                    cpf_credor,
                    requerente,
                    valor_liquido_final
                FROM CalculosPrecos
                WHERE requerente LIKE '%{nome_busca}%'
                ORDER BY requerente
                """
                registrar_acao(conexao_log, 'BUSCA_POR_NOME', f"Buscou Nome: {nome_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'nome': nome_busca}})
                df, erro = executar_query(query)
                if erro:
                    registrar_acao(conexao_log, 'BUSCA_ERRO', f"Erro na busca por Nome: {erro}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'nome': nome_busca}})
                    st.error(f"Erro: {erro}")
                elif len(df) == 0:
                    registrar_acao(conexao_log, 'BUSCA_SEM_RESULTADO', f"Busca Nome sem resultados: {nome_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'nome': nome_busca}})
                    st.warning("Nenhum resultado encontrado")
                else:
                    registrar_acao(conexao_log, 'BUSCA_RESULTADO', f"Busca Nome retornou {len(df)} registro(s).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'nome': nome_busca, 'qtd_resultados': len(df)}})
                    st.dataframe(df, use_container_width=True)
    
    with col2:
        st.subheader("Buscar por Número do Processo")
        processo_busca = st.text_input("Número do Processo")
        if st.button("Buscar Processo"):
            if processo_busca:
                query = f"""
                SELECT 
                    c.numero_processo,
                    c.cpf_credor,
                    c.requerente,
                    c.requerido,
                    c.valor_liquido_final,
                    d.chave,
                    d.valor
                FROM CalculosPrecos c
                LEFT JOIN DadosProcesso d ON c.numero_processo = d.numero_processo
                WHERE c.numero_processo = '{processo_busca}'
                ORDER BY d.chave
                """
                registrar_acao(conexao_log, 'BUSCA_POR_PROCESSO', f"Buscou Processo: {processo_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'processo': processo_busca}})
                df, erro = executar_query(query)
                if erro:
                    registrar_acao(conexao_log, 'BUSCA_ERRO', f"Erro na busca por Processo: {erro}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'processo': processo_busca}})
                    st.error(f"Erro: {erro}")
                elif len(df) == 0:
                    registrar_acao(conexao_log, 'BUSCA_SEM_RESULTADO', f"Busca Processo sem resultados: {processo_busca}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'processo': processo_busca}})
                    st.warning("Nenhum resultado encontrado")
                else:
                    registrar_acao(conexao_log, 'BUSCA_RESULTADO', f"Busca Processo retornou {len(df)} registro(s).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'processo': processo_busca, 'qtd_resultados': len(df)}})
                    st.dataframe(df, use_container_width=True)
        
        st.subheader("Processos Acima de Valor")
        valor_min = st.number_input("Valor mínimo (R$)", min_value=0.0, value=50000.0)
        if st.button("Buscar por Valor"):
            query = f"""
            SELECT 
                numero_processo,
                cpf_credor,
                requerente,
                valor_liquido_final
            FROM CalculosPrecos
            WHERE valor_liquido_final > {valor_min}
            ORDER BY valor_liquido_final DESC
            """
            registrar_acao(conexao_log, 'BUSCA_POR_VALOR', f"Buscou processos acima de R$ {valor_min:,.2f}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'valor_min': valor_min}})
            df, erro = executar_query(query)
            if erro:
                registrar_acao(conexao_log, 'BUSCA_ERRO', f"Erro na busca por Valor: {erro}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'valor_min': valor_min}})
                st.error(f"Erro: {erro}")
            elif len(df) == 0:
                registrar_acao(conexao_log, 'BUSCA_SEM_RESULTADO', f"Busca por Valor sem resultados: R$ {valor_min:,.2f}", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'valor_min': valor_min}})
                st.warning("Nenhum resultado encontrado")
            else:
                registrar_acao(conexao_log, 'BUSCA_RESULTADO', f"Busca por Valor retornou {len(df)} registro(s).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py', 'dados_json': {'valor_min': valor_min, 'qtd_resultados': len(df)}})
                st.dataframe(df, use_container_width=True)

# ============================================================
# TAB 3: FINANCEIRO
# ============================================================
with tab3:
    st.header("Análises Financeiras")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("💼 Top 20 Honorários", use_container_width=True):
            query = """
            SELECT 
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.valor_liquido_final,
                d.valor as honorarios
            FROM CalculosPrecos c
            JOIN DadosProcesso d ON c.numero_processo = d.numero_processo
            WHERE d.chave = 'honorarios'
              AND CAST(d.valor AS REAL) > 0
            ORDER BY CAST(d.valor AS REAL) DESC
            LIMIT 20
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Top 20 Honorários' (Financeiro).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
        
        if st.button("🏥 Top 20 INSS", use_container_width=True):
            query = """
            SELECT 
                c.numero_processo,
                c.cpf_credor,
                c.requerente,
                c.valor_liquido_final,
                d.valor as inss
            FROM CalculosPrecos c
            JOIN DadosProcesso d ON c.numero_processo = d.numero_processo
            WHERE d.chave = 'inss'
              AND CAST(d.valor AS REAL) > 0
            ORDER BY CAST(d.valor AS REAL) DESC
            LIMIT 20
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Top 20 INSS' (Financeiro).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
    
    with col2:
        if st.button("📊 Estatísticas por Requerido", use_container_width=True):
            query = """
            SELECT 
                requerido,
                COUNT(*) as qtd_processos,
                SUM(valor_liquido_final) as valor_total,
                AVG(valor_liquido_final) as valor_medio
            FROM CalculosPrecos
            WHERE requerido IS NOT NULL
              AND valor_liquido_final IS NOT NULL
            GROUP BY requerido
            ORDER BY qtd_processos DESC
            LIMIT 20
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Estatísticas por Requerido' (Financeiro).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)
        
        if st.button("📋 Processos por Natureza", use_container_width=True):
            query = """
            SELECT 
                d.valor as natureza,
                COUNT(DISTINCT d.numero_processo) as quantidade,
                SUM(c.valor_liquido_final) as valor_total
            FROM DadosProcesso d
            JOIN CalculosPrecos c ON d.numero_processo = c.numero_processo
            WHERE d.chave = 'natureza'
            GROUP BY d.valor
            """
            registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Processos por Natureza' (Financeiro).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            df, erro = executar_query(query)
            if erro:
                st.error(f"Erro: {erro}")
            else:
                st.dataframe(df, use_container_width=True)

# ============================================================
# TAB 4: EXPORT
# ============================================================
with tab4:
    st.header("Exportar Dados")
    
    if st.button("📥 Gerar CSV Completo", use_container_width=True):
        query = """
        SELECT 
            c.numero_processo,
            c.cpf_credor,
            c.requerente,
            c.requerido,
            c.valor_liquido_final,
            MAX(CASE WHEN d.chave = 'tipo' THEN d.valor END) as tipo,
            MAX(CASE WHEN d.chave = 'natureza' THEN d.valor END) as natureza,
            MAX(CASE WHEN d.chave = 'indice_correcao' THEN d.valor END) as indice,
            MAX(CASE WHEN d.chave = 'honorarios' THEN d.valor END) as honorarios,
            MAX(CASE WHEN d.chave = 'inss' THEN d.valor END) as inss,
            MAX(CASE WHEN d.chave = 'ir' THEN d.valor END) as ir,
            MAX(CASE WHEN d.chave = 'valor_bruto' THEN d.valor END) as valor_bruto
        FROM CalculosPrecos c
        LEFT JOIN DadosProcesso d ON c.numero_processo = d.numero_processo
        GROUP BY c.id
        ORDER BY c.id
        """
        df, erro = executar_query(query)
        if erro:
            st.error(f"Erro: {erro}")
        else:
            registrar_acao(conexao_log, 'EXPORT_CSV', f"Gerou CSV completo com {len(df)} registros.", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
            st.success(f"Total de registros: {len(df)}")
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False).encode('utf-8')
            ret = st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name="processos_completos.csv",
                mime="text/csv"
            )
            if ret:
                registrar_acao(conexao_log, 'DOWNLOAD_CSV', "Baixou CSV completo de processos.", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
    
    st.markdown("---")
    
    if st.button("📊 Últimos 50 Processos Adicionados", use_container_width=True):
        query = """
        SELECT 
            numero_processo,
            cpf_credor,
            requerente,
            valor_liquido_final,
            arquivo_de_origem
        FROM CalculosPrecos
        ORDER BY id DESC
        LIMIT 50
        """
        registrar_acao(conexao_log, 'EXECUCAO_SQL', "Executou 'Últimos 50 Processos Adicionados' (Export).", {'nome_usuario': usuario_logado, 'pagina_origem': '17_Consultas_SQL.py'})
        df, erro = executar_query(query)
        if erro:
            st.error(f"Erro: {erro}")
        else:
            st.dataframe(df, use_container_width=True)

# Fecha a conexão de log
try:
    conexao_log.close()
except Exception:
    pass

