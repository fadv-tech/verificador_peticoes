# pages/12_Explorador_de_Banco_de_Dados.py

import streamlit as st
import sqlite3
import os
import pandas as pd
from datetime import datetime
from modules.db import conectar_db, registrar_acao

# --- Configuração da Página ---
st.set_page_config(
    page_title="Explorador de Banco de Dados",
    page_icon="🗺️",
    layout="wide"
)
usuario_logado = st.session_state.get('username', 'N/A')
conexao = conectar_db()
st.title("🗺️ Explorador de Banco de Dados")
st.write(f"Análise executada em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
st.info(
    "Esta página não assume nada. Ela varre a pasta raiz do projeto em busca de arquivos de banco de dados (.db) "
    "e mapeia a estrutura e os dados de cada um deles."
)
registrar_acao(
    conexao,
    'VISUALIZACAO_PAGINA',
    "Acessou Explorador de Banco de Dados.",
    {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py'}
)

def get_db_files_in_root():
    """Encontra todos os arquivos .db na pasta atual (raiz do projeto)."""
    root_path = '.'  # Diretório atual
    try:
        files = [f for f in os.listdir(root_path) if f.endswith('.db')]
        if not files:
            st.warning("Nenhum arquivo de banco de dados (.db) foi encontrado na pasta raiz do projeto.")
            return []
        st.success(f"Arquivos de banco de dados encontrados: `{', '.join(files)}`")
        registrar_acao(conexao, 'EXPLORADOR_INVENTARIO', f"Encontrou {len(files)} arquivo(s) .db", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py', 'dados_json': {'qtd_arquivos': len(files), 'lista_arquivos': files}})
        return files
    except Exception as e:
        st.error(f"Ocorreu um erro ao tentar listar os arquivos na pasta raiz: {e}")
        registrar_acao(conexao, 'EXPLORADOR_ERRO_INVENTARIO', "Erro ao listar arquivos .db", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py', 'dados_json': {'erro': str(e)}})
        return []

def explore_database(db_file):
    """Conecta a um arquivo de banco de dados e explora sua estrutura e dados."""
    st.markdown(f"---")
    st.header(f"Análise do Banco de Dados: `{db_file}`")

    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
        cursor = conn.cursor()

        # 1. Listar todas as tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = [table[0] for table in cursor.fetchall()]

        if not tables:
            st.warning("Este banco de dados não contém nenhuma tabela.")
            registrar_acao(conexao, 'EXPLORADOR_DB_SEM_TABELAS', f"DB sem tabelas: {db_file}", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py'})
            conn.close()
            return

        st.write(f"**Tabelas encontradas ({len(tables)}):** `{', '.join(tables)}`")
        registrar_acao(conexao, 'EXPLORADOR_ANALISE_DB', f"Explorou DB: {db_file}", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py', 'dados_json': {'qtd_tabelas': len(tables), 'tabelas': tables}})

        # 2. Analisar cada tabela
        for table_name in tables:
            with st.expander(f"Tabela: `{table_name}`"):
                try:
                    # 2.1. Obter schema da tabela
                    st.subheader("Schema da Tabela (Colunas e Tipos)")
                    schema_df = pd.read_sql_query(f"PRAGMA table_info('{table_name}');", conn)
                    st.dataframe(schema_df[['name', 'type', 'notnull', 'pk']].rename(columns={
                        'name': 'Nome da Coluna',
                        'type': 'Tipo de Dado',
                        'notnull': 'Não Nulo (1=Sim)',
                        'pk': 'Chave Primária (1=Sim)'
                    }), use_container_width=True)

                    # 2.2. Contar registros
                    count_cursor = conn.cursor()
                    count_cursor.execute(f"SELECT COUNT(*) FROM '{table_name}';")
                    record_count = count_cursor.fetchone()[0]
                    st.info(f"**Total de Registros:** {record_count}")

                    # 2.3. Mostrar amostra dos dados
                    if record_count > 0:
                        st.subheader("Amostra de Dados (primeiras 5 linhas)")
                        sample_df = pd.read_sql_query(f"SELECT * FROM '{table_name}' LIMIT 5;", conn)
                        st.dataframe(sample_df, use_container_width=True)
                    else:
                        st.write("A tabela está vazia (não há dados para mostrar).")

                except Exception as e:
                    st.error(f"Não foi possível analisar a tabela `{table_name}`: {e}")
                    registrar_acao(conexao, 'EXPLORADOR_ERRO_TABELA', f"Erro ao analisar tabela: {table_name}", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py', 'dados_json': {'db': db_file, 'erro': str(e)}})
        
        conn.close()

    except sqlite3.Error as e:
        st.error(f"Não foi possível conectar ou ler o banco de dados `{db_file}`. Erro: {e}")
        registrar_acao(conexao, 'EXPLORADOR_ERRO_CONEXAO_DB', f"Erro conexão DB: {db_file}", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py', 'dados_json': {'erro': str(e)}})
    except Exception as e:
        st.error(f"Ocorreu um erro inesperado ao explorar `{db_file}`: {e}")
        registrar_acao(conexao, 'EXPLORADOR_ERRO_DB_GERAL', f"Erro geral ao explorar DB: {db_file}", {'nome_usuario': usuario_logado, 'pagina_origem': '15_Explorador_de_Banco_de_Dados.py', 'dados_json': {'erro': str(e)}})


# --- Execução Principal ---
db_files = get_db_files_in_root()

if db_files:
    for db_file in db_files:
        explore_database(db_file)

conexao.close()
