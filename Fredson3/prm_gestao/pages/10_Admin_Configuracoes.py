# ==============================================================================
# pages/3_Configuracoes_e_Devedores.py (v1.3 - VERSÃO FINAL COMPLETA)
#
# CORRIGIDO: Importa corretamente todas as funções do 'db.py' unificado.
# ==============================================================================
import streamlit as st
import pandas as pd
from modules.db import (
    conectar_db,
    buscar_regras_desagio_padrao,
    salvar_regras_desagio_padrao,
    buscar_devedores,
    salvar_devedor,
    deletar_devedor,
    buscar_nomes_devedores_unicos,
    registrar_acao
)

st.set_page_config(page_title="Configurações e Devedores", layout="wide")

# Acesso liberado: páginas de Admin não exigem login

st.title("⚙️ Configurações e Regras de Negócio")
st.markdown("Gerencie as regras de deságio padrão e as exceções por devedor.")

conexao = conectar_db()

# --- Seção de Regras Padrão ---
st.header("Regras de Deságio Padrão")
regras_padrao = buscar_regras_desagio_padrao(conexao)
min_padrao = regras_padrao.get('desagio_min_padrao', 20.0)
max_padrao = regras_padrao.get('desagio_max_padrao', 50.0)

with st.form("form_regras_padrao"):
    st.write("Defina os percentuais de deságio mínimo e máximo que serão aplicados a todos os devedores, exceto os que tiverem regras específicas.")
    col1, col2 = st.columns(2)
    novo_min_padrao = col1.number_input("Deságio Mínimo Padrão (%)", min_value=0.0, max_value=100.0, value=min_padrao, step=0.5)
    novo_max_padrao = col2.number_input("Deságio Máximo Padrão (%)", min_value=0.0, max_value=100.0, value=max_padrao, step=0.5)
    
    if st.form_submit_button("Salvar Regras Padrão", use_container_width=True, type="primary"):
        salvar_regras_desagio_padrao(conexao, novo_min_padrao, novo_max_padrao)
        registrar_acao(conexao, 'CONFIG_UPDATE', f"Regras de deságio padrão atualizadas para Min: {novo_min_padrao}%, Max: {novo_max_padrao}%.")
        st.success("Regras de deságio padrão salvas com sucesso!")
        if hasattr(st, "rerun"):
            st.rerun()
        else:
            st.experimental_rerun()

st.divider()

# --- Seção de Regras por Devedor ---
st.header("Regras Específicas por Devedor")
df_devedores = buscar_devedores(conexao)

if not df_devedores.empty:
    st.write("Devedores com regras de deságio específicas:")
    st.dataframe(df_devedores, use_container_width=True, hide_index=True)
else:
    st.info("Nenhuma regra específica por devedor foi cadastrada ainda.")

with st.expander("Adicionar ou Editar Regra para um Devedor"):
    with st.form("form_devedor"):
        nomes_devedores_db = buscar_nomes_devedores_unicos(conexao)
        nome_devedor = st.selectbox("Selecione ou Digite o Nome do Devedor", options=[""] + nomes_devedores_db, index=0)
        
        col1, col2 = st.columns(2)
        desagio_min_devedor = col1.number_input("Deságio Mínimo Específico (%)", min_value=0.0, max_value=100.0, value=25.0, step=0.5)
        desagio_max_devedor = col2.number_input("Deságio Máximo Específico (%)", min_value=0.0, max_value=100.0, value=55.0, step=0.5)
        
        if st.form_submit_button("Salvar Regra do Devedor", use_container_width=True):
            if nome_devedor:
                salvar_devedor(conexao, nome_devedor, desagio_min_devedor, desagio_max_devedor)
                registrar_acao(conexao, 'DEVEDOR_UPDATE', f"Regra para o devedor '{nome_devedor}' salva/atualizada.")
                st.success(f"Regra para o devedor '{nome_devedor}' salva com sucesso!")
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()

with st.expander("Remover Regra de um Devedor"):
    with st.form("form_delete_devedor"):
        devedores_com_regra = df_devedores['nome_devedor'].tolist()
        devedor_para_deletar = st.selectbox("Selecione o Devedor para Remover a Regra", options=[""] + devedores_com_regra, index=0)
        
        if st.form_submit_button("Remover Regra", type="primary"):
            if devedor_para_deletar:
                deletar_devedor(conexao, devedor_para_deletar)
                registrar_acao(conexao, 'DEVEDOR_DELETE', f"Regra para o devedor '{devedor_para_deletar}' removida.")
                st.warning(f"Regra para o devedor '{devedor_para_deletar}' foi removida.")
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()

conexao.close()
