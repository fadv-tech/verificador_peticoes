# ==============================================================================
# pages/4_Inclusao_Manual.py (v4.1 - INSTRUMENTADO COM LOGS)
#
# OBJETIVO:
# - Adiciona log de visualização, tentativa de envio inválido, sucesso e falha.
# - Em caso de falha, loga o JSON completo dos dados, como solicitado.
# ==============================================================================
import streamlit as st
from modules.db import conectar_db, criar_credito_manual, registrar_acao

# --- Configuração da Página e Controle de Acesso ---
st.set_page_config(page_title="Cadastro Manual de Crédito", layout="wide")

if 'authentication_status' not in st.session_state or not st.session_state['authentication_status']:
    st.warning("Por favor, faça o login para acessar o sistema.")
    st.stop()

if st.session_state.get("perfil") != "Admin":
    st.error("🚫 Acesso Negado. Esta página é restrita a Administradores.")
    st.stop()

usuario_logado = st.session_state.get('username', 'N/A')
conexao = conectar_db()

### LOG 1: VISUALIZAÇÃO DE PÁGINA ###
registrar_acao(conexao, 'VISUALIZACAO_PAGINA', "Acessou a página de Inclusão Manual.", {'nome_usuario': usuario_logado, 'pagina_origem': '4_Inclusao_Manual.py'})

# --- Interface Principal da Página ---
st.title("✍️ Cadastro Manual de Novo Crédito")
st.markdown("Utilize este formulário para inserir um novo crédito diretamente no sistema. Preencha os campos obrigatórios e os valores que já possuir.")
st.info("Campos marcados com `*` são obrigatórios.")

with st.form(key="form_inclusao_manual", clear_on_submit=True):
    st.subheader("Dados do Processo e das Partes")
    col1, col2 = st.columns(2)
    with col1:
        numero_processo = st.text_input("Número do Processo (CNJ) *")
        requerente = st.text_input("Nome do Credor (Requerente) *")
    with col2:
        requerido_1 = st.text_input("Nome do Devedor (Requerido) *")
        cpf_credor = st.text_input("CPF do Credor *")

    st.divider()
    st.subheader("Dados Financeiros (Opcional)")
    col_fin1, col_fin2, col_fin3 = st.columns(3)
    with col_fin1:
        valor_bruto_rpv_precatorio = st.number_input("Valor Bruto do Precatório/RPV", min_value=0.0, format="%.2f")
        honorarios_contratuais = st.number_input("Honorários Contratuais (%)", min_value=0.0, max_value=100.0, value=20.0, step=0.5)
    with col_fin2:
        imposto_de_renda_retido = st.number_input("Imposto de Renda Retido (%)", min_value=0.0, max_value=100.0, value=3.0, step=0.5)
        contribuicao_previdenciaria = st.number_input("Contribuição Previdenciária (PSS)", min_value=0.0, format="%.2f")
    with col_fin3:
        valor_liquido_final = st.number_input("Valor Líquido Final (se já calculado)", min_value=0.0, format="%.2f")

    submitted = st.form_submit_button("Cadastrar Novo Crédito", type="primary", use_container_width=True)

    if submitted:
        # Monta o dicionário de dados ANTES de qualquer validação, para o log de falha
        dados_credito = {
            'numero_processo': numero_processo.replace('.', '').replace('-', ''),
            'cpf_credor': cpf_credor,
            'requerente': requerente,
            'requerido_1': requerido_1,
            'valor_bruto_rpv_precatorio': valor_bruto_rpv_precatorio,
            'honorarios_contratuais': honorarios_contratuais,
            'imposto_de_renda_retido': imposto_de_renda_retido,
            'contribuicao_previdenciaria': contribuicao_previdenciaria,
            'valor_liquido_final': valor_liquido_final,
            'arquivo_de_origem': f"Inclusão Manual por {usuario_logado}"
        }

        if not all([numero_processo, requerente, requerido_1, cpf_credor]):
            st.error("Por favor, preencha todos os campos obrigatórios (*).")
            ### LOG 2: TENTATIVA DE ENVIO INVÁLIDO ###
            registrar_acao(conexao, 'FORMULARIO_INVALIDO', "Tentativa de cadastrar crédito com campos obrigatórios vazios.", {'nome_usuario': usuario_logado, 'pagina_origem': '4_Inclusao_Manual.py', 'dados_json': dados_credito})
        else:
            if valor_liquido_final == 0.0 and valor_bruto_rpv_precatorio > 0.0:
                valor_apos_honorarios = valor_bruto_rpv_precatorio * (1 - (honorarios_contratuais / 100.0))
                valor_apos_ir = valor_apos_honorarios * (1 - (imposto_de_renda_retido / 100.0))
                valor_liquido_final = valor_apos_ir - contribuicao_previdenciaria
                dados_credito['valor_liquido_final'] = valor_liquido_final # Atualiza o dicionário
                st.info(f"Valor líquido final estimado em: R$ {valor_liquido_final:,.2f}")

            with st.spinner("Salvando dados no sistema..."):
                sucesso, mensagem = criar_credito_manual(conexao, dados_credito)
                
                if sucesso:
                    novo_id = mensagem
                    st.success(f"Crédito cadastrado com sucesso! O novo crédito recebeu o ID: {novo_id}")
                    ### LOG 3: INCLUSÃO COM SUCESSO ###
                    detalhes_log = f"Crédito ID {novo_id} para o processo {numero_processo} criado manualmente."
                    dados_log = {'nome_usuario': usuario_logado, 'pagina_origem': '4_Inclusao_Manual.py', 'id_credito': novo_id, 'dados_json': dados_credito}
                    registrar_acao(conexao, 'INCLUSAO_MANUAL_SUCESSO', detalhes_log, dados_log)
                else:
                    st.error(f"Falha ao cadastrar o crédito: {mensagem}")
                    ### LOG 4: INCLUSÃO COM FALHA NO DB (COM JSON COMPLETO) ###
                    detalhes_log = f"Falha ao cadastrar crédito para o processo {numero_processo}. Erro: {mensagem}"
                    dados_log = {'nome_usuario': usuario_logado, 'pagina_origem': '4_Inclusao_Manual.py', 'dados_json': dados_credito}
                    registrar_acao(conexao, 'INCLUSAO_MANUAL_FALHA', detalhes_log, dados_log)

conexao.close()
