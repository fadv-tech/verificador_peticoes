# ==============================================================================
# pages/3_Dossie_do_Credor.py (v9.3 - HISTÓRICO FILTRADO E LIMPO)
#
# OBJETIVO:
# - Filtra o histórico exibido para o operador, mostrando apenas ações
#   relevantes para a negociação e ocultando "logs de sistema".
# ==============================================================================

import streamlit as st
import pandas as pd
from datetime import datetime
import sqlite3

# --- 1. FUNÇÃO DE CONEXÃO (AUTOSSUFICIENTE) ---
def conectar_db():
    try:
        from modules.db import conectar_db as _conectar_db_mod
        return _conectar_db_mod()
    except Exception:
        import sqlite3
        return sqlite3.connect('precatorios_estrategico.db', check_same_thread=False)

# --- 2. IMPORTAÇÕES SEGURAS ---
try:
    from modules.db import (
        buscar_dados_completos_credor, buscar_creditos_por_credor, 
        salvar_dado_credor, atualizar_status_credito, salvar_anotacao_credito,
        atualizar_proposta_credito, registrar_acao, buscar_desagio_para_proposta
    )
except ImportError as e:
    st.error(f"ERRO CRÍTICO DE IMPORTAÇÃO: {e}."); st.stop()

# --- 3. FUNÇÕES DE APOIO (INTOCADAS) ---

def usuario_tem_permissao_credito(con, credito_id):
    perfil_atual = st.session_state.get('perfil')
    grupo_atual = st.session_state.get('grupo_id')
    if perfil_atual == 'Admin' or not grupo_atual:
        return True
    try:
        df_chk = pd.read_sql_query(
            "SELECT 1 FROM GruposCreditos WHERE grupo_id = ? AND credito_id = ?",
            con,
            params=(grupo_atual, int(credito_id))
        )
        return not df_chk.empty
    except Exception:
        return False


def registrar_e_executar_mudanca_status(con, user, chave_credor, credito_id, proc_num, novo_status, detalhes_adicionais=""):
    if not usuario_tem_permissao_credito(con, credito_id):
        st.error("🚫 Você não tem permissão para alterar este crédito.")
        return
    sucesso = atualizar_status_credito(con, credito_id, novo_status)
    if sucesso:
        detalhes_log = f"Status do crédito {credito_id} (Proc: {proc_num}) alterado para '{novo_status}'. {detalhes_adicionais}".strip()
        dados_log = {'pagina_origem': '3_Dossie_do_Credor.py', 'nome_usuario': user, 'chave_agrupamento_credor': chave_credor, 'id_credito': credito_id}
        registrar_acao(con, 'MUDANCA_STATUS_CREDITO', detalhes_log, dados_log)
        st.success(f"Status do crédito {proc_num} atualizado para '{novo_status}'!")
        if hasattr(st, "rerun"):
            st.rerun()
        else:
            st.experimental_rerun()
    else:
        st.error("Falha ao atualizar o status no banco de dados.")

# --- 4. LÓGICA PRINCIPAL ---
if 'chave_credor_selecionado' not in st.session_state or not st.session_state['chave_credor_selecionado']:
    st.error("Nenhum credor selecionado. Por favor, volte e selecione um credor."); st.stop()

chave_selecionada = st.session_state['chave_credor_selecionado']
usuario_logado = st.session_state.get('username', 'N/A')
conexao = conectar_db() 

# Log de visualização (continua sendo registrado, mas não será exibido aqui)
registrar_acao(conexao, 'VISUALIZACAO_Dossie', f"Acessou/Recarregou o dossiê do credor.", {'nome_usuario': usuario_logado, 'pagina_origem': '3_Dossie_do_Credor.py', 'chave_agrupamento_credor': chave_selecionada})

try:
    dados_credor = buscar_dados_completos_credor(conexao, chave_selecionada)
    df_creditos = buscar_creditos_por_credor(conexao, chave_selecionada)
except Exception as e:
    st.error(f"Ocorreu um erro ao buscar os dados do credor: {e}"); st.stop()

# Aplicar filtro por grupo para não-Admins
perfil_atual = st.session_state.get('perfil')
grupo_atual = st.session_state.get('grupo_id')
# Bloqueia não-admin sem grupo associado
if perfil_atual != 'Admin' and not grupo_atual:
    try:
        registrar_acao(conexao, 'ACESSO_NEGADO_SEM_GRUPO', "Usuário sem grupo tentou acessar dossiê do credor", {'nome_usuario': usuario_logado, 'pagina_origem': '3_Dossie_do_Credor.py', 'chave_agrupamento_credor': chave_selecionada})
    except Exception:
        pass
    st.error("Conteúdo indisponível.")
    st.stop()
ids_permitidos = None
if perfil_atual != 'Admin' and grupo_atual:
    try:
        ids_df = pd.read_sql_query(
            "SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?",
            conexao,
            params=(grupo_atual,)
        )
        ids_permitidos = set(ids_df['credito_id'].tolist())
        df_creditos = df_creditos[df_creditos['id'].isin(ids_permitidos)].copy()
    except Exception as e:
        st.warning(f"Não foi possível aplicar filtro de grupo: {e}")
# Bloqueio total de visualização quando não há créditos do grupo
if perfil_atual != 'Admin' and grupo_atual and df_creditos.empty:
    try:
        registrar_acao(conexao, 'ACESSO_NEGADO_DOSSIE', "Tentativa de acesso a dossiê sem créditos do grupo", {'nome_usuario': usuario_logado, 'pagina_origem': '3_Dossie_do_Credor.py', 'chave_agrupamento_credor': chave_selecionada})
    except Exception:
        pass
    st.error("Crédito não encontrado.")
    st.stop()

if not dados_credor:
    st.error(f"Não foi possível encontrar os dados para o credor: {chave_selecionada}"); st.stop()

# --- 5. RENDERIZAÇÃO DA PÁGINA ---
st.title(f"📂 Dossiê do Credor: {dados_credor.get('nome_principal', 'N/A')}")

if st.button("⬅️ Voltar"):
    registrar_acao(conexao, 'NAVEGACAO_SAIDA', "Clicou em 'Voltar' no Dossiê do Credor.", {'nome_usuario': usuario_logado, 'pagina_origem': '3_Dossie_do_Credor.py', 'chave_agrupamento_credor': chave_selecionada})
    st.switch_page("pages/2_Mesa_de_Negociacao.py")

st.divider()
col1, col2, col3 = st.columns(3)
col1.metric("Valor Total em Carteira", f"R$ {dados_credor.get('valor_total', 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
col2.metric("Quantidade de Créditos", dados_credor.get('qtd_creditos', 0))
col3.metric("Status do Relacionamento", dados_credor.get('status_relacionamento', 'Não Contatado'))
st.divider()
st.subheader("Dados de Contato e Anotações Gerais")
col_tel, col_email = st.columns(2)
with col_tel:
    with st.form("form_telefone"):
        novo_telefone = st.text_input("Telefone", value=dados_credor.get('telefone', ''))
        if st.form_submit_button("Salvar Telefone"):
            valor_antigo = dados_credor.get('telefone', '')
            if novo_telefone != valor_antigo:
                salvar_dado_credor(conexao, chave_selecionada, 'telefone', novo_telefone)
                log_detalhes = f"Alterou TELEFONE de '{valor_antigo}' para '{novo_telefone}'."
                dados_log = {'pagina_origem': '3_Dossie_do_Credor.py', 'nome_usuario': usuario_logado, 'chave_agrupamento_credor': chave_selecionada, 'dados_json': {'campo': 'telefone', 'de': valor_antigo, 'para': novo_telefone}}
                registrar_acao(conexao, 'EDICAO_DADO', log_detalhes, dados_log)
                st.success("Telefone atualizado!")
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()
with col_email:
    with st.form("form_email"):
        novo_email = st.text_input("Email", value=dados_credor.get('email', ''))
        if st.form_submit_button("Salvar Email"):
            valor_antigo = dados_credor.get('email', '')
            if novo_email != valor_antigo:
                salvar_dado_credor(conexao, chave_selecionada, 'email', novo_email)
                log_detalhes = f"Alterou EMAIL de '{valor_antigo}' para '{novo_email}'."
                dados_log = {'pagina_origem': '3_Dossie_do_Credor.py', 'nome_usuario': usuario_logado, 'chave_agrupamento_credor': chave_selecionada, 'dados_json': {'campo': 'email', 'de': valor_antigo, 'para': novo_email}}
                registrar_acao(conexao, 'EDICAO_DADO', log_detalhes, dados_log)
                st.success("Email atualizado!")
                if hasattr(st, "rerun"):
                    st.rerun()
                else:
                    st.experimental_rerun()
with st.form("form_anotacoes_gerais"):
    novas_anotacoes = st.text_area("Anotações Gerais", value=dados_credor.get('anotacoes_gerais', ''), height=125)
    if st.form_submit_button("Salvar Anotações Gerais"):
        valor_antigo = dados_credor.get('anotacoes_gerais', '')
        if novas_anotacoes != valor_antigo:
            salvar_dado_credor(conexao, chave_selecionada, 'anotacoes_gerais', novas_anotacoes)
            log_detalhes = f"Alterou Anotações Gerais de '{valor_antigo}' para '{novas_anotacoes}'"
            dados_log = {'pagina_origem': '3_Dossie_do_Credor.py', 'nome_usuario': usuario_logado, 'chave_agrupamento_credor': chave_selecionada, 'dados_json': {'campo': 'anotacoes_gerais', 'de': valor_antigo, 'para': novas_anotacoes}}
            registrar_acao(conexao, 'EDICAO_DADO', log_detalhes, dados_log)
            st.success("Anotações Gerais salvas!")
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()
st.divider()
st.subheader("⚙️ Mesa de Créditos Individuais")
if df_creditos.empty:
    st.warning("Este credor não possui créditos individuais registrados.")
else:
    colunas_grid = st.columns(2)
    for i, (_, credito) in enumerate(df_creditos.iterrows()):
        coluna_atual = colunas_grid[i % 2]
        with coluna_atual:
            with st.container(border=True):
                # ... (código interno do cartão de crédito idêntico)
                credito_id = credito['id']
                proc_num = credito['numero_processo']
                status_atual = credito['status_workflow']
                anotacao_atual = credito.get('rascunho_anotacao', '')
                st.markdown(f"##### Processo: `{proc_num}` (ID: {credito_id})")
                st.markdown(f"**Valor:** R$ {credito['valor_liquido_final']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                
                # Calcular proposta sugerida com deságio específico do devedor
                valor_liquido = credito['valor_liquido_final']
                nome_devedor = credito.get('requerido_1') or credito.get('requerido')
                
                # Buscar deságio (específico do devedor ou padrão)
                desagio_min, desagio_max, origem_desagio = buscar_desagio_para_proposta(conexao, nome_devedor)
                
                # Exibir proposta apenas se o deságio estiver configurado
                if desagio_min and desagio_max:
                    proposta_min = valor_liquido * (1 - desagio_max / 100)
                    proposta_max = valor_liquido * (1 - desagio_min / 100)
                    proposta_min_fmt = f"{proposta_min:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    proposta_max_fmt = f"{proposta_max:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    st.caption(f"💡 Proposta Sugerida: MÍNIMO {proposta_min_fmt} - MÁXIMO {proposta_max_fmt} ({desagio_min:.0f}%-{desagio_max:.0f}% | {origem_desagio})")
                else:
                    st.caption(f"⚠️ Deságio não configurado ({origem_desagio})")
                
                with st.form(f"form_anotacao_credito_{credito_id}"):
                    nova_anotacao_credito = st.text_area("Anotações deste Crédito", value=anotacao_atual, height=75, key=f"anot_cred_{credito_id}")
                    if st.form_submit_button("Salvar Anotação"):
                        if nova_anotacao_credito != anotacao_atual:
                            if not usuario_tem_permissao_credito(conexao, credito_id):
                                st.error("🚫 Você não tem permissão para editar este crédito.")
                            else:
                                salvar_anotacao_credito(conexao, credito_id, nova_anotacao_credito)
                                log_detalhes = f"Alterou anotação do crédito {credito_id} de '{anotacao_atual}' para '{nova_anotacao_credito}'"
                                dados_log = {'pagina_origem': '3_Dossie_do_Credor.py', 'nome_usuario': usuario_logado, 'chave_agrupamento_credor': chave_selecionada, 'id_credito': credito_id, 'dados_json': {'campo': 'rascunho_anotacao', 'de': anotacao_atual, 'para': nova_anotacao_credito}}
                                registrar_acao(conexao, 'EDICAO_ANOTACAO_CREDITO', log_detalhes, dados_log)
                                st.success("Anotação salva!")
                                if hasattr(st, "rerun"):
                                    st.rerun()
                                else:
                                    st.experimental_rerun()
                st.markdown(f"**Status:** `{status_atual}`")
                if status_atual in ["Novo", "Em Análise"]:
                    with st.form(f"form_proposta_{credito_id}"):
                        valor_proposta = st.number_input("Valor da Proposta (R$)", min_value=0.01, format="%.2f", key=f"valor_proposta_{credito_id}")
                        if st.form_submit_button("🚀 Enviar Proposta"):
                            if not usuario_tem_permissao_credito(conexao, credito_id):
                                st.error("🚫 Você não tem permissão para enviar proposta neste crédito.")
                            else:
                                sucesso = atualizar_proposta_credito(conexao, credito_id, "Proposta Pendente", valor_proposta)
                                if sucesso:
                                    valor_proposta_formatado = f"{valor_proposta:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                    detalhes_log = f"Proposta de R$ {valor_proposta_formatado} enviada para o crédito {credito_id}."
                                    dados_log = {'pagina_origem': '3_Dossie_do_Credor.py', 'nome_usuario': usuario_logado, 'chave_agrupamento_credor': chave_selecionada, 'id_credito': credito_id, 'dados_json': {'valor_proposta': valor_proposta}}
                                    registrar_acao(conexao, 'PROPOSTA_ENVIADA', detalhes_log, dados_log)
                                    st.success("Proposta registrada!")
                                    if hasattr(st, "rerun"):
                                        st.rerun()
                                    else:
                                        st.experimental_rerun()
                                else: st.error("Falha ao registrar a proposta.")
                elif status_atual == "Proposta Pendente":
                    valor_pendente = credito.get('valor_ultima_proposta')
                    if valor_pendente and valor_pendente > 0:
                        valor_formatado = f"{valor_pendente:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        st.info(f"**Proposta Pendente de R$ {valor_formatado}**")
                    else: st.warning("**Proposta Pendente (valor não registrado)**")
                    c1, c2 = st.columns(2)
                    if c1.button("✅ Aceita", key=f"aceita_{credito_id}", use_container_width=True):
                        registrar_e_executar_mudanca_status(conexao, usuario_logado, chave_selecionada, credito_id, proc_num, "Proposta Aceita", f"Proposta de R$ {valor_pendente or 0:,.2f} foi ACEITA.")
                    if c2.button("❌ Recusada", key=f"recusa_{credito_id}", use_container_width=True):
                        registrar_e_executar_mudanca_status(conexao, usuario_logado, chave_selecionada, credito_id, proc_num, "Recusado", f"Proposta de R$ {valor_pendente or 0:,.2f} foi RECUSADA.")
                elif status_atual == "Proposta Aceita":
                    st.info("✨ Proposta Aceita. Continue o processo na página Propostas Aceitas.")
                    if st.button("📋 Ir para Propostas Aceitas", key=f"propostas_aceitas_{credito_id}", use_container_width=True, type="primary"):
                        registrar_acao(conexao, 'NAVEGACAO_PARA_PROPOSTAS_ACEITAS', f"Clicou para ir para Propostas Aceitas.", {'nome_usuario': usuario_logado, 'pagina_origem': '3_Dossie_do_Credor.py', 'chave_agrupamento_credor': chave_selecionada, 'id_credito': credito_id})
                        st.switch_page("pages/6_Propostas_Aceitas.py")
                elif status_atual == "Adquirido": st.success("🎉 Crédito Adquirido!")
                elif status_atual == "Recusado":
                    st.error("Proposta Recusada.")
                    if st.button("🔄 Fazer Nova Proposta", key=f"renegocia_{credito_id}", use_container_width=True):
                        registrar_e_executar_mudanca_status(conexao, usuario_logado, chave_selecionada, credito_id, proc_num, "Em Análise", "Negociação reaberta após recusa.")
st.divider()
st.subheader("📜 Histórico de Ações do Credor")

expander_aberto = st.expander("Clique para ver o histórico de negociação")
if expander_aberto:
    try:
        registrar_acao(conexao, 'VISUALIZACAO_HISTORICO', "Expandiu o histórico de ações do credor.", {'nome_usuario': usuario_logado, 'pagina_origem': '3_Dossie_do_Credor.py', 'chave_agrupamento_credor': chave_selecionada})
    except Exception:
        pass
    with expander_aberto:
        try:
            # --- A MUDANÇA ESTÁ AQUI ---
            # Lista de tipos de ação que são RELEVANTES para o operador.
            tipos_relevantes = [
                'EDICAO_DADO', 'EDICAO_ANOTACAO_CREDITO', 'PROPOSTA_ENVIADA', 
                'MUDANCA_STATUS_CREDITO', 'RELACIONAMENTO_EM_MASSA', 'INCLUSAO_MANUAL_SUCESSO'
            ]
            placeholders = ', '.join('?' for _ in tipos_relevantes)
            # Quando não-admin, restringe histórico aos créditos do grupo
            if perfil_atual != 'Admin' and ids_permitidos:
                ids_list = list(ids_permitidos)
                ids_placeholders = ', '.join('?' for _ in ids_list)
                query = f"""
                    SELECT timestamp, nome_usuario, tipo_acao, detalhes_humanos 
                    FROM HistoricoAcoes 
                    WHERE 
                        chave_agrupamento_credor = ? AND
                        tipo_acao IN ({placeholders}) AND
                        id_credito IN ({ids_placeholders})
                    ORDER BY timestamp DESC
                """
                params = [chave_selecionada] + tipos_relevantes + ids_list
            else:
                query = f"""
                    SELECT timestamp, nome_usuario, tipo_acao, detalhes_humanos 
                    FROM HistoricoAcoes 
                    WHERE 
                        chave_agrupamento_credor = ? AND
                        tipo_acao IN ({placeholders})
                    ORDER BY timestamp DESC
                """
                params = [chave_selecionada] + tipos_relevantes

            df_log = pd.read_sql_query(query, conexao, params=params)
            if df_log.empty:
                st.info("Nenhuma ação de negociação registrada para este credor ainda.")
            else:
                st.dataframe(df_log, use_container_width=True, hide_index=True, column_config={'detalhes_humanos': 'Detalhes'})
        except Exception as e:
            st.warning(f"Não foi possível carregar o histórico de ações: {e}")

conexao.close()