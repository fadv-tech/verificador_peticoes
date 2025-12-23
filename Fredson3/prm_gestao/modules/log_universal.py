# modules/log_universal.py
# ==============================================================================
# MÓDULO DE LOG UNIVERSAL (v1.0)
#
# OBJETIVO:
# - Ponto ÚNICO de entrada para TODOS os registros de log do sistema.
# - Garante que todas as ações sejam registradas de forma padronizada e completa.
# - Abstrai a complexidade da inserção no banco de dados.
# ==============================================================================

import sqlite3
from datetime import datetime
import json
import streamlit as st

def registrar_acao(conexao, tipo_acao, detalhes_humanos, dados_adicionais={}):
    """
    Função Universal para registrar qualquer ação no sistema.

    Args:
        conexao: A conexão ativa com o banco de dados.
        tipo_acao (str): Categoria da ação (ex: 'LOGIN_SUCESSO', 'EDICAO_DADO').
        detalhes_humanos (str): Frase legível descrevendo a ação.
        dados_adicionais (dict): Dicionário com dados estruturados e de contexto.
            - 'pagina_origem': (Opcional) Script onde a ação ocorreu.
            - 'id_usuario': (Opcional) ID do usuário que fez a ação.
            - 'nome_usuario': (Opcional) Nome do usuário.
            - 'chave_agrupamento_credor': (Opcional) Chave do credor relacionado.
            - 'id_credito': (Opcional) ID do crédito específico.
            - 'dados_json': (Opcional) Dicionário com o "DE/PARA" da alteração.
    """
    try:
        cursor = conexao.cursor()
        
        # Pega os dados do dicionário, com valores padrão seguros
        id_usuario = dados_adicionais.get('id_usuario')
        nome_usuario = dados_adicionais.get('nome_usuario', st.session_state.get('username', 'N/A'))
        pagina_origem = dados_adicionais.get('pagina_origem')
        chave_credor = dados_adicionais.get('chave_agrupamento_credor')
        id_credito = dados_adicionais.get('id_credito')
        dados_json_str = json.dumps(dados_adicionais.get('dados_json')) if dados_adicionais.get('dados_json') else None

        cursor.execute(
            """INSERT INTO HistoricoAcoes 
               (timestamp, nome_usuario, id_usuario, tipo_acao, pagina_origem, chave_agrupamento_credor, id_credito, detalhes_humanos, dados_alterados_json) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (datetime.now(), nome_usuario, id_usuario, tipo_acao, pagina_origem, chave_credor, id_credito, detalhes_humanos, dados_json_str)
        )
        conexao.commit()
        return True
    except Exception as e:
        # Em uma aplicação real, isso poderia logar em um arquivo de texto para não poluir a UI
        st.error(f"Falha Crítica no Módulo de Log Universal: {e}")
        return False
