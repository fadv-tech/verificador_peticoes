# ==============================================================================
# Painel.py (v2.0 - COM CONTROLE DE ACESSO POR ROLE)
# Arquivo principal de inicialização do Streamlit.
# Gerencia o login, o logout e a navegação principal.
# Controla acesso às páginas baseado no perfil do usuário.
# ==============================================================================

import os
os.environ["STREAMLIT_DISABLE_EMAIL_PROMPT"] = "1"

import streamlit as st
import hashlib
import os
from modules.db import conectar_db, buscar_usuario, registrar_acao, buscar_grupo_do_usuario

st.set_page_config(
    page_title="PRM - Gestão de Precatórios",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# CONFIGURAÇÃO DE PÁGINAS PERMITIDAS POR PERFIL
# ==============================================================================
PAGINAS_PERMITIDAS = {
    'Admin': 'all',  # Admin tem acesso a todas as páginas
    'Operador': [     # Usuário comum tem acesso limitado
        '1_Dashboard_Geral.py',
        '1_Dashboard_Geral_v2.py',
        '2_Mesa_de_Negociacao.py',
        '2_Mesa_de_Negociacao_v2.py',
        '3_Dossie_do_Credor.py',
        '4_Inclusao_Manual.py',
        '5_Consulta_Tecnica_Credito.py',
        '5_Consulta_Tecnica_Credito_v2.py'
    ]
}

def verificar_senha(senha_fornecida, salt, hash_armazenado):
    """Verifica se a senha fornecida corresponde ao hash armazenado."""
    return hashlib.sha256((senha_fornecida + salt).encode('utf-8')).hexdigest() == hash_armazenado

def ocultar_paginas_nao_permitidas():
    """Oculta páginas que o usuário não tem permissão de acessar"""
    if 'perfil' not in st.session_state:
        return
    
    perfil = st.session_state['perfil']
    
    # Se for Admin, não oculta nada
    if perfil == 'Admin':
        return
    
    # Para outros perfis, oculta páginas não permitidas
    paginas_permitidas = PAGINAS_PERMITIDAS.get(perfil, [])
    
    # CSS para ocultar páginas não permitidas
    hide_pages_css = """
    <style>
    """
    
    # Lista todas as páginas na pasta pages
    pages_dir = 'pages'
    if os.path.exists(pages_dir):
        for arquivo in os.listdir(pages_dir):
            if arquivo.endswith('.py'):
                # Se a página não está na lista de permitidas, oculta
                if arquivo not in paginas_permitidas:
                    # Remove extensão e número para criar o seletor
                    page_name = arquivo.replace('.py', '').replace('_', ' ')
                    hide_pages_css += f"""
                    [data-testid="stSidebarNav"] a[href*="{arquivo}"] {{
                        display: none;
                    }}
                    """
    
    hide_pages_css += """
    </style>
    """
    
    st.markdown(hide_pages_css, unsafe_allow_html=True)

# Inicializa o status de autenticação na sessão se não existir
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None

# Se o usuário não estiver logado, exibe a tela de login
if not st.session_state['authentication_status']:
    st.sidebar.title("Login")
    username = st.sidebar.text_input("Usuário")
    password = st.sidebar.text_input("Senha", type="password")

    if st.sidebar.button("Entrar", use_container_width=True, type="primary"):
        conexao = conectar_db()
        user_data = buscar_usuario(conexao, username)

        if user_data:
            user_id, user_name, user_profile, user_hash, user_salt = user_data
            if verificar_senha(password, user_salt, user_hash):
                st.session_state['authentication_status'] = True
                st.session_state['user_id'] = user_id
                st.session_state['username'] = user_name
                st.session_state['perfil'] = user_profile
                # Carrega o grupo do usuário para habilitar a visualização por grupo
                try:
                    st.session_state['grupo_id'] = buscar_grupo_do_usuario(conexao, user_id)
                except Exception:
                    st.session_state['grupo_id'] = None
                registrar_acao(conexao, 'LOGIN_SUCESSO', f"Usuário '{user_name}' logou com sucesso.", 
                             {'nome_usuario': user_name, 'perfil': user_profile})
                conexao.close()
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()
            else:
                st.sidebar.error("Usuário ou senha inválidos.")
                registrar_acao(conexao, 'LOGIN_FALHA', f"Tentativa de login falhou para o usuário '{username}'.",
                             {'nome_usuario': username})
                conexao.close()
        else:
            st.sidebar.error("Usuário ou senha inválidos.")
            if username:
                conexao = conectar_db()
                registrar_acao(conexao, 'LOGIN_FALHA', f"Tentativa de login para usuário inexistente: '{username}'.",
                             {'nome_usuario': username})
                conexao.close()
    
    st.title("⚖️ PRM - Gestão de Precatórios")
    st.info("Por favor, faça o login para acessar o sistema.")
    st.stop()

# Se o usuário estiver logado, exibe o menu e o botão de sair
st.sidebar.title(f"Bem-vindo, {st.session_state['username']}!")
st.sidebar.write(f"Perfil: **{st.session_state['perfil']}**")

# Aplica controle de acesso às páginas
ocultar_paginas_nao_permitidas()

if st.sidebar.button("Sair", use_container_width=True):
    conexao = conectar_db()
    registrar_acao(conexao, 'LOGOUT', f"Usuário '{st.session_state['username']}' saiu do sistema.",
                 {'nome_usuario': st.session_state['username']})
    conexao.close()
    # Limpa todos os dados da sessão para um logout completo
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

st.sidebar.divider()
st.sidebar.header("Navegação")

# Mostra informação sobre permissões
if st.session_state['perfil'] != 'Admin':
    st.sidebar.info("Você tem acesso limitado às páginas do sistema.")

st.header("Visão Geral do Sistema")
st.write("Selecione uma das opções no menu lateral para começar a trabalhar.")

# Mostra páginas disponíveis baseado no perfil
perfil = st.session_state['perfil']
if perfil == 'Admin':
    st.success("✓ Você tem acesso completo a todas as funcionalidades do sistema.")
else:
    st.info(f"Páginas disponíveis para o perfil '{perfil}':")
    paginas = PAGINAS_PERMITIDAS.get(perfil, [])
    for pagina in paginas:
        nome_pagina = pagina.replace('.py', '').replace('_', ' ')
        st.write(f"• {nome_pagina}")