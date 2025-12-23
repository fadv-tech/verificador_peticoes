# ==============================================================================
# pages/14_Admin_Central.py (NOME SUGERIDO, MANTENHA O SEU SE PREFERIR)
#
# OBJETIVO:
# - UMA PÁGINA para governar todas as outras.
# - SEÇÃO 1: Gerenciamento completo de usuários (Criar, Editar, Deletar).
# - SEÇÃO 2: Auditoria completa e "ultra rastreável" das ações dos usuários.
# - Acesso restrito apenas a usuários com perfil 'Admin'.
# ==============================================================================

import streamlit as st
import sqlite3
import pandas as pd
import hashlib
import os
import json
from datetime import datetime

# --- 1. FUNÇÕES DE BANCO DE DADOS E SEGURANÇA (AUTOCONTIDAS) ---

def conectar_db():
    """Conecta ao banco de dados de gestão."""
    return sqlite3.connect('precatorios_estrategico.db', check_same_thread=False)

# Utilitários de grupos

def buscar_grupos(conexao):
    try:
        return pd.read_sql_query("SELECT id, nome FROM Grupos ORDER BY nome", conexao)
    except Exception:
        return pd.DataFrame(columns=['id', 'nome'])

def hash_senha(senha, salt):
    """Gera o hash de uma senha usando um salt."""
    return hashlib.sha256((senha + salt).encode('utf-8')).hexdigest()

# Importa a função de log do db.py, que já está estável
try:
    from modules.db import registrar_acao
except ImportError:
    # Fallback caso a função ainda não esteja no db.py principal
    def registrar_acao(conexao, tipo_acao, detalhes_humanos, dados_log={}):
        st.warning("Função 'registrar_acao' não encontrada no `modules.db`. Usando fallback.")
        return False

# --- Funções para a Seção 1: Gestão de Usuários ---
def buscar_todos_usuarios(conexao):
    try:
        return pd.read_sql_query("SELECT id, nome_usuario, perfil, grupo_id FROM Usuarios ORDER BY nome_usuario", conexao)
    except Exception as e:
        st.error(f"Erro ao buscar usuários: {e}"); return pd.DataFrame()

def criar_usuario(conexao, nome, senha, perfil, grupo_id=None):
    try:
        cursor = conexao.cursor()
        salt = os.urandom(16).hex()
        senha_hashed = hash_senha(senha, salt)
        cursor.execute(
            "INSERT INTO Usuarios (nome_usuario, senha_hash, senha_salt, perfil, grupo_id) VALUES (?, ?, ?, ?, ?)",
            (nome, senha_hashed, salt, perfil, grupo_id)
        )
        conexao.commit(); return True, "Usuário criado com sucesso!"
    except sqlite3.IntegrityError: return False, "Erro: Nome de usuário já existe."
    except Exception as e: return False, f"Erro inesperado: {e}"

def atualizar_grupo_usuario(conexao, user_id, novo_grupo_id):
    try:
        cursor = conexao.cursor(); cursor.execute("UPDATE Usuarios SET grupo_id = ? WHERE id = ?", (novo_grupo_id, user_id)); conexao.commit(); return True
    except Exception: return False

def deletar_usuario(conexao, user_id):
    try:
        cursor = conexao.cursor(); cursor.execute("DELETE FROM Usuarios WHERE id = ?", (user_id,)); conexao.commit(); return True
    except Exception: return False

def atualizar_perfil_usuario(conexao, user_id, novo_perfil):
    try:
        cursor = conexao.cursor(); cursor.execute("UPDATE Usuarios SET perfil = ? WHERE id = ?", (novo_perfil, user_id)); conexao.commit(); return True
    except Exception: return False

def redefinir_senha_usuario(conexao, user_id, nova_senha):
    try:
        cursor = conexao.cursor()
        novo_salt = os.urandom(16).hex()
        nova_senha_hashed = hash_senha(nova_senha, novo_salt)
        cursor.execute("UPDATE Usuarios SET senha_hash = ?, senha_salt = ? WHERE id = ?", (nova_senha_hashed, novo_salt, user_id))
        conexao.commit(); return True
    except Exception: return False

# --- Funções para a Seção 2: Auditoria ---
def buscar_nomes_usuarios_log(conexao):
    try:
        df = pd.read_sql_query("SELECT DISTINCT nome_usuario FROM HistoricoAcoes WHERE nome_usuario IS NOT NULL ORDER BY nome_usuario", conexao)
        return ["Todos"] + df['nome_usuario'].tolist()
    except Exception: return ["Todos"]

def buscar_logs(conexao, nome_usuario, tipo_acao, data_inicio, data_fim, termo_busca):
    try:
        query = "SELECT timestamp, nome_usuario, tipo_acao, detalhes_humanos, pagina_origem, chave_agrupamento_credor, id_credito, dados_alterados_json FROM HistoricoAcoes WHERE 1=1"
        params = []
        if nome_usuario != "Todos":
            query += " AND nome_usuario = ?"
            params.append(nome_usuario)
        if tipo_acao:
            placeholders = ','.join('?' for _ in tipo_acao)
            query += f" AND tipo_acao IN ({placeholders})"
            params.extend(tipo_acao)
        if data_inicio:
            query += " AND DATE(timestamp) >= ?"
            params.append(data_inicio)
        if data_fim:
            query += " AND DATE(timestamp) <= ?"
            params.append(data_fim)
        if termo_busca:
            query += " AND detalhes_humanos LIKE ?"
            params.append(f"%{termo_busca}%")
            
        query += " ORDER BY timestamp DESC"
        df = pd.read_sql_query(query, conexao, params=params)
        return df
    except Exception as e:
        st.error(f"Erro ao buscar logs: {e}"); return pd.DataFrame()

# --- LÓGICA PRINCIPAL DA PÁGINA ---
st.set_page_config(page_title="Central do Admin", layout="wide")
st.title("⚙️ Central do Administrador")

# Acesso liberado: páginas de Admin não exigem login

usuario_logado = st.session_state.get('username', 'N/A')
conexao = conectar_db()

# ==============================================================================
# SEÇÃO 1: GESTÃO DE USUÁRIOS
# ==============================================================================
st.header("🛡️ Gestão de Usuários")

with st.expander("➕ Criar Novo Usuário", expanded=False):
    with st.form("form_criar_usuario", clear_on_submit=True):
        st.subheader("Dados do Novo Usuário")
        novo_nome = st.text_input("Nome de Usuário")
        novo_perfil = st.selectbox("Perfil", ["Operador", "Admin"])
        # Seleção de Grupo (opcional)
        df_grupos = buscar_grupos(conexao)
        grupos_nomes = ["Sem grupo"] + df_grupos['nome'].tolist()
        grupos_ids = [None] + df_grupos['id'].tolist()
        idx_grupo = st.selectbox("Grupo", options=list(range(len(grupos_nomes))), format_func=lambda i: grupos_nomes[i])
        novo_grupo_id = grupos_ids[idx_grupo]
        nova_senha = st.text_input("Senha", type="password")
        confirmar_senha = st.text_input("Confirmar Senha", type="password")
        if st.form_submit_button("Criar Usuário"):
            if not all([novo_nome, nova_senha, confirmar_senha]): st.warning("Por favor, preencha todos os campos.")
            elif nova_senha != confirmar_senha: st.error("As senhas não coincidem.")
            else:
                sucesso, msg = criar_usuario(conexao, novo_nome, nova_senha, novo_perfil, novo_grupo_id)
                if sucesso:
                    st.success(msg)
                    registrar_acao(conexao, 'CRIACAO_USUARIO', f"Criou o usuário '{novo_nome}' com perfil '{novo_perfil}' e grupo '{grupos_nomes[idx_grupo]}'.", {'nome_usuario': usuario_logado})
                    if hasattr(st, "rerun"):
                        st.rerun()
                    else:
                        st.experimental_rerun()
                else: st.error(msg)

st.subheader("📋 Usuários Cadastrados")
df_usuarios = buscar_todos_usuarios(conexao)
# Mapa de grupos para exibição
_df_grupos_list = buscar_grupos(conexao)
mapa_grupos = {row['id']: row['nome'] for _, row in _df_grupos_list.iterrows()}
if df_usuarios.empty:
    st.info("Nenhum usuário cadastrado ainda.")
else:
    for _, row in df_usuarios.iterrows():
        user_id, user_nome, user_perfil, user_grupo_id = row['id'], row['nome_usuario'], row['perfil'], row.get('grupo_id')
        nome_grupo = mapa_grupos.get(user_grupo_id, "Sem grupo")
        with st.container(border=True):
            c1, c2, c3 = st.columns([2, 2, 1])
            c1.markdown(f"**Usuário:** `{user_nome}` (ID: {user_id})"); c2.markdown(f"**Perfil:** `{user_perfil}` | **Grupo:** `{nome_grupo}`")
            if user_nome != usuario_logado:
                with c3.popover("⚙️ Gerenciar"):
                    if st.button("🗑️ Deletar", key=f"del_{user_id}", type="primary"):
                        deletar_usuario(conexao, user_id)
                        registrar_acao(conexao, 'REMOCAO_USUARIO', f"Deletou o usuário '{user_nome}'.", {'nome_usuario': usuario_logado})
                        st.success(f"Usuário '{user_nome}' removido!")
                        if hasattr(st, "rerun"):
                            st.rerun()
                        else:
                            st.experimental_rerun()
                    novo_perfil = "Admin" if user_perfil == "Operador" else "Operador"
                    if st.button(f"Tornar '{novo_perfil}'", key=f"perfil_{user_id}"):
                        atualizar_perfil_usuario(conexao, user_id, novo_perfil)
                        registrar_acao(conexao, 'EDICAO_PERFIL_USUARIO', f"Alterou perfil de '{user_nome}' para '{novo_perfil}'.", {'nome_usuario': usuario_logado})
                        st.success(f"Perfil de '{user_nome}' atualizado!")
                        if hasattr(st, "rerun"):
                            st.rerun()
                        else:
                            st.experimental_rerun()
                    with st.form(f"form_reset_{user_id}"):
                        st.write("**Redefinir Senha**")
                        nova_senha_reset = st.text_input("Nova Senha", type="password", key=f"pwd_{user_id}")
                        if st.form_submit_button("Confirmar"):
                            if nova_senha_reset:
                                redefinir_senha_usuario(conexao, user_id, nova_senha_reset)
                                registrar_acao(conexao, 'RESET_SENHA_USUARIO', f"Redefiniu a senha de '{user_nome}'.", {'nome_usuario': usuario_logado})
                                st.success(f"Senha de '{user_nome}' redefinida!")
                                if hasattr(st, "rerun"):
                                    st.rerun()
                                else:
                                    st.experimental_rerun()
                            else: st.warning("Senha não pode ser vazia.")
                    with st.form(f"form_grupo_{user_id}"):
                        st.write("**Selecionar Grupo**")
                        grupos_nomes2 = ["Sem grupo"] + _df_grupos_list['nome'].tolist()
                        grupos_ids2 = [None] + _df_grupos_list['id'].tolist()
                        # seleção atual
                        idx_atual = grupos_ids2.index(user_grupo_id) if user_grupo_id in grupos_ids2 else 0
                        idx_sel = st.selectbox("Grupo", options=list(range(len(grupos_nomes2))), index=idx_atual, format_func=lambda i: grupos_nomes2[i], key=f"sel_grupo_{user_id}")
                        if st.form_submit_button("Atualizar Grupo"):
                            novo_grupo_id_sel = grupos_ids2[idx_sel]
                            atualizar_grupo_usuario(conexao, user_id, novo_grupo_id_sel)
                            registrar_acao(conexao, 'EDICAO_GRUPO_USUARIO', f"Alterou grupo de '{user_nome}' para '{grupos_nomes2[idx_sel]}'.", {'nome_usuario': usuario_logado})
                            st.success(f"Grupo de '{user_nome}' atualizado!")
                            if hasattr(st, "rerun"):
                                st.rerun()
                            else:
                                st.experimental_rerun()

# ==============================================================================
# LINHA DIVISÓRIA
# ==============================================================================
st.markdown("---")

# ==============================================================================
# SEÇÃO INTERMEDIÁRIA: CRIAR GRUPOS
# ==============================================================================
st.subheader("👥 Criar Grupos")
with st.form("form_criar_grupo", clear_on_submit=True):
    novo_nome_grupo = st.text_input("Nome do grupo")
    if st.form_submit_button("Criar Grupo"):
        if not novo_nome_grupo.strip():
            st.warning("Informe um nome para o grupo.")
        else:
            try:
                conexao.execute("INSERT INTO Grupos (nome) VALUES (?)", (novo_nome_grupo.strip(),))
                conexao.commit()
                st.success(f"Grupo '{novo_nome_grupo}' criado com sucesso!")
                registrar_acao(conexao, 'CRIACAO_GRUPO', f"Criou o grupo '{novo_nome_grupo}'.", {'nome_usuario': usuario_logado})
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()
            except Exception as e:
                st.error(f"Erro ao criar grupo: {e}")

# Exibe lista de grupos existentes
_df_grupos_view = buscar_grupos(conexao)
if not _df_grupos_view.empty:
    st.dataframe(_df_grupos_view, use_container_width=True, hide_index=True)
else:
    st.info("Nenhum grupo cadastrado ainda.")

# ==============================================================================
# SEÇÃO 2: AUDITORIA COMPLETA
# ==============================================================================
st.header("🕵️‍♂️ Auditoria Completa de Ações")

# --- Filtros de Auditoria ---
lista_usuarios_log = buscar_nomes_usuarios_log(conexao)
lista_tipos_acao = pd.read_sql_query("SELECT DISTINCT tipo_acao FROM HistoricoAcoes", conexao)['tipo_acao'].tolist()

filtros_col1, filtros_col2 = st.columns(2)
with filtros_col1:
    filtro_usuario = st.selectbox("Filtrar por Usuário", lista_usuarios_log)
    filtro_data_inicio = st.date_input("De:", None)
with filtros_col2:
    filtro_tipo_acao = st.multiselect("Filtrar por Tipo de Ação", lista_tipos_acao)
    filtro_data_fim = st.date_input("Até:", None)
filtro_termo = st.text_input("Buscar por termo nos detalhes do log:")

# --- Busca e Exibição dos Logs ---
df_logs = buscar_logs(conexao, filtro_usuario, filtro_tipo_acao, filtro_data_inicio, filtro_data_fim, filtro_termo)

st.write(f"**{len(df_logs)}** ações encontradas.")

if not df_logs.empty:
    st.dataframe(df_logs, use_container_width=True, hide_index=True)
    
    st.subheader("🔍 Detalhes da Ação Selecionada")
    selected_indices = st.multiselect("Selecione uma linha da tabela acima para ver os detalhes do JSON:", options=df_logs.index.tolist(), max_selections=1)
    
    if selected_indices:
        selected_row = df_logs.loc[selected_indices[0]]
        json_data_str = selected_row.get('dados_alterados_json')
        if json_data_str and json_data_str != 'null':
            try:
                json_data = json.loads(json_data_str)
                st.write(f"**Detalhes da Alteração (Ação ID: {selected_row.name})**")
                if 'de' in json_data and 'para' in json_data:
                    st.text_area("Valor Antigo (De):", value=str(json_data['de']), height=100, disabled=True)
                    st.text_area("Valor Novo (Para):", value=str(json_data['para']), height=100, disabled=True)
                else:
                    st.json(json_data)
            except json.JSONDecodeError:
                st.warning("O conteúdo do log não é um JSON válido.")
            except Exception as e:
                st.error(f"Erro ao processar detalhes: {e}")
        else:
            st.info("Nenhum detalhe JSON registrado para esta ação.")

conexao.close()
