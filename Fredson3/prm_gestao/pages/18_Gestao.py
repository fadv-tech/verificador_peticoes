import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
from modules.db import registrar_acao
import re

st.set_page_config(page_title="18 – Gestão de Grupos", layout="wide")

# Controle de acesso temporariamente removido para testes
auth = True
perfil = "Admin"
username = "admin"

# Conecta direto ao banco de dados principal usando caminho absoluto
DB_PATH = Path(__file__).resolve().parent.parent / "precatorios_estrategico.db"
con = sqlite3.connect(str(DB_PATH), check_same_thread=False)

# Utilitário: extrai valores monetários de textos (logs), compatível com formato BR/US
def parse_currency_from_text(text):
    try:
        if not text:
            return None
        s = str(text)
        br_matches = re.findall(r'(?:R\$\s*)?([0-9\.]+,[0-9]{2})', s)
        us_matches = re.findall(r'(?:R\$\s*)?([0-9,]+\.[0-9]{2})', s)
        candidate = None
        if br_matches:
            candidate = br_matches[-1]
            candidate = candidate.replace('.', '').replace(',', '.')
            return float(candidate)
        if us_matches:
            candidate = us_matches[-1]
            candidate = candidate.replace(',', '')
            return float(candidate)
        return None
    except Exception:
        return None

st.title("Painel Ultra de Gestão de Grupos")
st.caption("Administra grupos, usuários, credores/créditos, pipeline e auditoria – tudo em um só lugar.")

@st.cache_data(ttl=120)
def listar_grupos():
    return pd.read_sql_query("SELECT id, nome FROM Grupos ORDER BY nome", con)

@st.cache_data(ttl=120)
def metricas_por_grupo():
    sql = """
    SELECT g.id, g.nome,
           COUNT(gc.credito_id) AS qtd_creditos,
           COALESCE(SUM(cp.valor_liquido_final),0) AS valor_total,
           COUNT(DISTINCT COALESCE(NULLIF(TRIM(cp.cpf_credor),''), 'S_CPF-' || UPPER(TRIM(cp.requerente)))) AS qtd_credores
    FROM Grupos g
    LEFT JOIN GruposCreditos gc ON gc.grupo_id = g.id
    LEFT JOIN CalculosPrecos cp ON cp.id = gc.credito_id
    GROUP BY g.id, g.nome
    ORDER BY g.nome
    """
    return pd.read_sql_query(sql, con)

@st.cache_data(ttl=120)
def listar_usuarios():
    sql = """
    SELECT u.id, u.nome_usuario, u.perfil, u.grupo_id, g.nome AS grupo
    FROM Usuarios u
    LEFT JOIN Grupos g ON g.id = u.grupo_id
    ORDER BY u.nome_usuario
    """
    return pd.read_sql_query(sql, con)

# (removido) Funções antigas baseadas em PropostasAceitas – não serão usadas nesta página

# ===================== TABS =====================
aba1, aba2, aba3, aba4, aba5, aba6, aba7, aba8, aba9 = st.tabs([
    "Visão Geral",
    "Grupos & Usuários",
    "Atribuição de Usuários",
    "Credores & Créditos",
    "Ações em Massa",
    "Auditoria",
    "Configuração Propostas Aceitas",
    "Gestão de Proposta Aceita",
    "Corrigir Créditos"
])

# -------- Visão Geral --------
with aba1:
    st.subheader("Métricas por Grupo")
    dfm = metricas_por_grupo()
    if dfm.empty:
        st.info("Nenhum grupo encontrado. Crie um na aba 'Grupos & Usuários'.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Grupos", len(dfm))
        c2.metric("Créditos vinculados", int(dfm["qtd_creditos"].sum()))
        c3.metric("Valor total", f"R$ {dfm['valor_total'].sum():,.2f}")
        st.dataframe(dfm, use_container_width=True)

# -------- Grupos & Usuários --------
with aba2:
    st.subheader("Gestão de Grupos")
    grupos = listar_grupos()
    colA, colB, colC = st.columns(3)
    # Criar grupo
    with colA:
        st.markdown("**Criar novo grupo**")
        with st.form("form_criar_grupo"):
            novo_nome = st.text_input("Nome do grupo")
            submit = st.form_submit_button("Criar")
            if submit and novo_nome.strip():
                con.execute("INSERT INTO Grupos (nome) VALUES (?)", (novo_nome.strip(),))
                con.commit()
                st.success(f"Grupo '{novo_nome}' criado.")
                registrar_acao(con, "criar_grupo", f"Criado grupo {novo_nome}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_nome": novo_nome}})
                st.cache_data.clear()
    # Renomear grupo
    with colB:
        st.markdown("**Renomear grupo**")
        gid = st.selectbox("Grupo", options=grupos["id"].tolist(), format_func=lambda x: grupos.set_index("id").loc[x, "nome"] if not grupos.empty else str(x))
        with st.form("form_renomear_grupo"):
            novo_nome2 = st.text_input("Novo nome")
            submit2 = st.form_submit_button("Renomear")
            if submit2 and novo_nome2.strip():
                con.execute("UPDATE Grupos SET nome = ? WHERE id = ?", (novo_nome2.strip(), gid))
                con.commit()
                st.success("Grupo renomeado.")
                registrar_acao(con, "renomear_grupo", f"Grupo {gid} -> {novo_nome2}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid}})
                st.cache_data.clear()
    # Excluir grupo
    with colC:
        st.markdown("**Excluir grupo**")
        gid2 = st.selectbox("Grupo a excluir", options=grupos["id"].tolist(), format_func=lambda x: grupos.set_index("id").loc[x, "nome"] if not grupos.empty else str(x), key="gid_del")
        desvincular = st.checkbox("Desvincular créditos antes de excluir")
        if st.button("Excluir grupo", type="primary"):
            if desvincular:
                con.execute("DELETE FROM GruposCreditos WHERE grupo_id = ?", (gid2,))
            con.execute("DELETE FROM Grupos WHERE id = ?", (gid2,))
            con.commit()
            st.success("Grupo excluído.")
            registrar_acao(con, "excluir_grupo", f"Excluído grupo {gid2}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid2, "desvinculou": desvincular}})
            st.cache_data.clear()

# -------- Atribuição de Usuários --------
with aba3:
    st.subheader("Atribuição de Usuários ao Grupo")
    st.caption("Gerencie a atribuição de usuários aos grupos de forma centralizada")
    
    grupos2 = listar_grupos()
    if grupos2.empty:
        st.info("Crie ao menos um grupo para atribuir usuários.")
    else:
        alvo_gid = st.selectbox("Selecione o Grupo", options=grupos2["id"].tolist(), format_func=lambda x: grupos2.set_index("id").loc[x, "nome"])
        
        # Exibir informações do grupo selecionado
        grupo_info = grupos2[grupos2["id"] == alvo_gid].iloc[0]
        st.markdown(f"**Grupo Selecionado:** {grupo_info['nome']}")
        
        # Listar usuários
        dfu = listar_usuarios()
        if dfu.empty:
            st.info("Nenhum usuário encontrado no sistema.")
        else:
            st.markdown("### Usuários Disponíveis")
            st.caption("Selecione os usuários que deseja atribuir a este grupo")
            
            # Criar uma cópia do DataFrame para edição
            dfu_edit = dfu.copy()
            dfu_edit["Selecionar"] = False
            
            # Marcar usuários que já pertencem ao grupo
            dfu_edit.loc[dfu_edit["grupo_id"] == alvo_gid, "Selecionar"] = True
            
            # Criar o editor de dados
            edited = st.data_editor(
                dfu_edit,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "Selecionar": st.column_config.CheckboxColumn(
                        "Selecionar",
                        help="Marque para atribuir ao grupo"
                    ),
                    "nome_usuario": st.column_config.TextColumn(
                        "Usuário",
                        help="Nome do usuário"
                    ),
                    "perfil": st.column_config.TextColumn(
                        "Perfil",
                        help="Perfil do usuário"
                    ),
                    "grupo": st.column_config.TextColumn(
                        "Grupo Atual",
                        help="Grupo atual do usuário"
                    )
                },
                hide_index=True
            )
            
            # Botão para salvar alterações
            if st.button("Salvar Atribuições", type="primary"):
                selecionados = edited.loc[edited["Selecionar"] == True, "id"].tolist()
                
                # Primeiro, remove todos os usuários do grupo
                con.execute("UPDATE Usuarios SET grupo_id = NULL WHERE grupo_id = ?", (alvo_gid,))
                
                # Depois, atribui os usuários selecionados
                for uid in selecionados:
                    con.execute("UPDATE Usuarios SET grupo_id = ? WHERE id = ?", (alvo_gid, uid))
                
                con.commit()
                st.success(f"{len(selecionados)} usuário(s) atribuídos ao grupo.")
                registrar_acao(
                    con,
                    "atribuir_usuarios_grupo",
                    f"Grupo {alvo_gid}: {len(selecionados)} usuários",
                    {
                        "nome_usuario": username,
                        "pagina_origem": "18_Gestao.py",
                        "dados_json": {
                            "grupo_id": alvo_gid,
                            "qtd_usuarios": len(selecionados)
                        }
                    }
                )
                st.cache_data.clear()

# -------- Credores & Créditos --------
with aba4:
    st.subheader("Vínculo de Credores/Créditos ao Grupo")
    grupos3 = listar_grupos()
    if grupos3.empty:
        st.info("Crie um grupo primeiro.")
    else:
        gid = st.selectbox("Grupo", options=grupos3["id"].tolist(), format_func=lambda x: grupos3.set_index("id").loc[x, "nome"], key="gid_link")
        termo = st.text_input("Busca (nome do credor, CPF/CNPJ ou nº processo)")
        df_res = pd.DataFrame()
        if termo.strip():
            like = f"%{termo.strip().upper()}%"
            sql = """
            WITH base AS (
              SELECT cp.id,
                     COALESCE(NULLIF(TRIM(cp.cpf_credor),''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) AS chave,
                     UPPER(TRIM(cp.requerente)) AS credor,
                     TRIM(cp.cpf_credor) AS documento,
                     COALESCE(cp.valor_liquido_final,0) AS valor,
                     TRIM(cp.numero_processo) AS processo
              FROM CalculosPrecos cp
              WHERE UPPER(TRIM(cp.requerente)) LIKE ? OR TRIM(cp.cpf_credor) LIKE ? OR TRIM(cp.numero_processo) LIKE ?
            )
            SELECT chave, MAX(credor) AS credor, MAX(documento) AS documento,
                   COUNT(*) AS qtd_creditos, SUM(valor) AS valor_total
            FROM base
            GROUP BY chave
            ORDER BY valor_total DESC
            """
            df_res = pd.read_sql_query(sql, con, params=(like, like.replace("%", ""), like.replace("%", "")))
        if df_res.empty:
            st.info("Digite um termo de busca para listar credores.")
        else:
            df_res["Selecionar"] = False
            edited2 = st.data_editor(df_res, use_container_width=True)
            escolhidos = edited2.loc[edited2["Selecionar"] == True, "chave"].tolist()
            colx, coly = st.columns(2)
            with colx:
                if st.button("Vincular selecionados ao grupo", type="primary"):
                    count = 0
                    for chave in escolhidos:
                        con.execute(
                            "INSERT OR IGNORE INTO GruposCreditos (grupo_id, credito_id) "
                            "SELECT ?, id FROM CalculosPrecos WHERE COALESCE(NULLIF(TRIM(cpf_credor),''), 'S_CPF-' || UPPER(TRIM(requerente))) = ?",
                            (gid, chave)
                        )
                        count += con.total_changes
                    con.commit()
                    st.success(f"Vinculados {len(escolhidos)} credor(es) ao grupo.")
                    registrar_acao(con, "vincular_credores_grupo", f"Grupo {gid}: {len(escolhidos)} chaves", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid, "qtd_chaves": len(escolhidos)}})
                    st.cache_data.clear()
            with coly:
                if st.button("Desvincular selecionados do grupo"):
                    for chave in escolhidos:
                        con.execute(
                            "DELETE FROM GruposCreditos WHERE grupo_id = ? AND credito_id IN "
                            "(SELECT id FROM CalculosPrecos WHERE COALESCE(NULLIF(TRIM(cpf_credor),''), 'S_CPF-' || UPPER(TRIM(requerente))) = ?)",
                            (gid, chave)
                        )
                    con.commit()
                    st.success(f"Desvinculados {len(escolhidos)} credor(es) do grupo.")
                    registrar_acao(con, "desvincular_credores_grupo", f"Grupo {gid}: {len(escolhidos)} chaves", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid, "qtd_chaves": len(escolhidos)}})
                    st.cache_data.clear()
        st.divider()
        st.markdown("**Vínculo por Processo (CNJ)**")
        proc = st.text_input("Número do processo")
        colp, colq = st.columns(2)
        with colp:
            if st.button("Vincular processo") and proc.strip():
                con.execute("INSERT OR IGNORE INTO GruposCreditos (grupo_id, credito_id) SELECT ?, id FROM CalculosPrecos WHERE TRIM(numero_processo) = ?", (gid, proc.strip()))
                con.commit()
                st.success("Processo vinculado ao grupo.")
                registrar_acao(con, "vincular_processo_grupo", f"Grupo {gid}: processo {proc}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid, "processo": proc}})
                st.cache_data.clear()
        with colq:
            if st.button("Desvincular processo") and proc.strip():
                con.execute("DELETE FROM GruposCreditos WHERE grupo_id = ? AND credito_id IN (SELECT id FROM CalculosPrecos WHERE TRIM(numero_processo) = ?)", (gid, proc.strip()))
                con.commit()
                st.success("Processo desvinculado do grupo.")
                registrar_acao(con, "desvincular_processo_grupo", f"Grupo {gid}: processo {proc}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid, "processo": proc}})
                st.cache_data.clear()

# -------- Ações em Massa --------
with aba4:
    st.subheader("Ações em Massa por Grupo")
    grupos4 = listar_grupos()
    if grupos4.empty:
        st.info("Crie um grupo primeiro.")
    else:
        gid = st.selectbox("Grupo", options=grupos4["id"].tolist(), format_func=lambda x: grupos4.set_index("id").loc[x, "nome"], key="gid_bulk")
        st.markdown("**Gestão de Relacionamento (GestaoCredores)**")
        status_rel = st.selectbox("Status", ["Não Contatado", "Contato Iniciado", "Em Negociação", "Proposta Enviada", "Proposta Aceita", "Rejeitado"])
        if st.button("Aplicar status em todos os credores do grupo"):
            keys_sql = """
            SELECT DISTINCT COALESCE(NULLIF(TRIM(cp.cpf_credor),''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) AS chave
            FROM CalculosPrecos cp
            JOIN GruposCreditos gc ON gc.credito_id = cp.id
            WHERE gc.grupo_id = ?
            """
            dfk = pd.read_sql_query(keys_sql, con, params=(gid,))
            for chave in dfk["chave"].tolist():
                con.execute("INSERT OR IGNORE INTO GestaoCredores (chave_agrupamento, status_relacionamento) VALUES (?, ?)", (chave, status_rel))
                con.execute("UPDATE GestaoCredores SET status_relacionamento = ? WHERE chave_agrupamento = ?", (status_rel, chave))
            con.commit()
            st.success("Status aplicado em massa.")
            registrar_acao(con, "bulk_status_gestaocredores", f"Grupo {gid}: {status_rel}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid, "status_relacionamento": status_rel}})
        st.divider()
        st.markdown("**Pipeline de Créditos (GestaoCreditos)**")
        status_wf = st.selectbox("Etapa", ["Novo", "Contato Iniciado", "Em Análise", "Em Negociação", "Proposta Enviada", "Proposta Aceita", "Rejeitado"], key="wf")
        if st.button("Aplicar etapa em todos os créditos do grupo"):
            ids_sql = """
            SELECT cp.id AS credito_id
            FROM CalculosPrecos cp
            JOIN GruposCreditos gc ON gc.credito_id = cp.id
            WHERE gc.grupo_id = ?
            """
            dfi = pd.read_sql_query(ids_sql, con, params=(gid,))
            for cid in dfi["credito_id"].tolist():
                con.execute("INSERT OR IGNORE INTO GestaoCreditos (id, status_workflow) VALUES (?, ?)", (cid, status_wf))
                con.execute("UPDATE GestaoCreditos SET status_workflow = ? WHERE id = ?", (status_wf, cid))
            con.commit()
            st.success("Pipeline aplicado em massa.")
            registrar_acao(con, "bulk_status_gestaocreditos", f"Grupo {gid}: {status_wf}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"grupo_id": gid, "status_workflow": status_wf}})

# -------- Auditoria --------
with aba5:
    st.subheader("Auditoria e Logs por Grupo")
    grupos5 = listar_grupos()
    if grupos5.empty:
        st.info("Crie um grupo primeiro.")
    else:
        gid = st.selectbox("Grupo", options=grupos5["id"].tolist(), format_func=lambda x: grupos5.set_index("id").loc[x, "nome"], key="gid_audit")
        st.markdown("Logs relacionados a créditos do grupo")
        sql_logs_cred = """
        SELECT * FROM HistoricoAcoes
        WHERE id_credito IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)
        ORDER BY timestamp DESC
        LIMIT 500
        """
        df_logs_cred = pd.read_sql_query(sql_logs_cred, con, params=(gid,))
        st.dataframe(df_logs_cred, use_container_width=True)
        st.markdown("Logs realizados por usuários do grupo")
        sql_logs_user = """
        SELECT * FROM HistoricoAcoes
        WHERE nome_usuario IN (SELECT nome_usuario FROM Usuarios WHERE grupo_id = ?)
        ORDER BY timestamp DESC
        LIMIT 500
        """
        df_logs_user = pd.read_sql_query(sql_logs_user, con, params=(gid,))
        st.dataframe(df_logs_user, use_container_width=True)

# -------- Configuração Propostas Aceitas --------
with aba6:
    st.subheader("Configuração Propostas Aceitas")
    st.caption("Defina quais itens são obrigatórios para bloquear avanço de operadores na página '6 – Propostas Aceitas'.")

    # Garantir tabela de requisitos
    con.execute("""
    CREATE TABLE IF NOT EXISTS ConfigPropostasAceitasRequisitos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        etapa TEXT NOT NULL,
        item_codigo TEXT NOT NULL,
        item_nome TEXT NOT NULL,
        obrigatorio INTEGER DEFAULT 1,
        ativo INTEGER DEFAULT 1,
        updated_at TEXT,
        updated_by TEXT,
        UNIQUE(etapa, item_codigo)
    )
    """)
    con.commit()

    # Visualização e edição
    etapas = pd.read_sql_query("SELECT DISTINCT etapa FROM ConfigPropostasAceitasRequisitos ORDER BY etapa", con)
    etapa_sel = st.selectbox("Etapa", options=etapas["etapa"].tolist())
    if etapa_sel:
        df = pd.read_sql_query("SELECT etapa, item_codigo, item_nome, obrigatorio, ativo FROM ConfigPropostasAceitasRequisitos WHERE etapa = ? ORDER BY item_nome", con, params=(etapa_sel,))
        if df.empty:
            st.info("Nenhum item configurado para esta etapa. Itens padrão são criados automaticamente na página 6 ao primeiro acesso.")
        else:
            st.markdown("Marque 'obrigatório' para bloquear avanço de operadores quando o item não estiver cumprido.")
            df_edit = st.data_editor(df, use_container_width=True)
            if st.button("Salvar alterações", type="primary", key="salvar_alteracoes_creditos_editor"):
                now = pd.Timestamp.now().isoformat()
                # Capturar mudanças para log detalhado
                mudancas = []
                df_old = df.set_index("item_codigo")
                for _, r in df_edit.iterrows():
                    con.execute(
                        "UPDATE ConfigPropostasAceitasRequisitos SET obrigatorio = ?, ativo = ?, updated_at = ?, updated_by = ? WHERE etapa = ? AND item_codigo = ?",
                        (int(r["obrigatorio"]), int(r["ativo"]), now, username, r["etapa"], r["item_codigo"]) 
                    )
                    if r["item_codigo"] in df_old.index:
                        old_obrig = int(df_old.loc[r["item_codigo"], "obrigatorio"]) if pd.notna(df_old.loc[r["item_codigo"], "obrigatorio"]) else None
                        old_ativo = int(df_old.loc[r["item_codigo"], "ativo"]) if pd.notna(df_old.loc[r["item_codigo"], "ativo"]) else None
                        new_obrig = int(r["obrigatorio"]) if pd.notna(r["obrigatorio"]) else None
                        new_ativ = int(r["ativo"]) if pd.notna(r["ativo"]) else None
                        if old_obrig != new_obrig or old_ativo != new_ativ:
                            mudancas.append({
                                "item_codigo": r["item_codigo"],
                                "item_nome": r["item_nome"],
                                "obrigatorio": {"old": old_obrig, "new": new_obrig},
                                "ativo": {"old": old_ativo, "new": new_ativ}
                            })
                con.commit()
                st.success("Requisitos atualizados.")
                registrar_acao(con, "atualizar_requisitos_propostas_aceitas", f"Etapa {etapa_sel}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"etapa": etapa_sel, "mudancas": mudancas, "mudancas_count": len(mudancas)}})
                st.cache_data.clear()

# -------- Gestão de Proposta Aceita --------
with aba8:
    st.subheader("Gestão de Proposta Aceita")
    st.caption("Editor geral dos dados de Aceite das operações já existentes (página 6).")

    # Lista operações controladas exclusivamente pelo status de gestão
    df_ops = pd.read_sql_query(
        """
        SELECT 
            o.id AS operacao_id,
            o.credito_id,
            o.numero_processo,
            COALESCE(a.credor_nome, cp.requerente) AS credor_nome,
            cp.valor_liquido_final AS valor_original,
            a.valor_compra,
            a.data_hora_aceite,
            a.forma_pagamento,
            a.observacoes,
            gc.status_workflow,
            gc.data_ultima_atualizacao
        FROM Operacoes o
        LEFT JOIN AceitesOperacao a ON a.operacao_id = o.id
        LEFT JOIN CalculosPrecos cp ON cp.numero_processo = o.numero_processo
        LEFT JOIN GestaoCreditos gc ON gc.id = o.credito_id
        WHERE gc.status_workflow = 'Proposta Aceita'
        ORDER BY COALESCE(a.data_hora_aceite, gc.data_ultima_atualizacao) DESC, o.id DESC
        """,
        con,
    )

    # Enriquecer dados faltantes de aceite (data e valor) a partir dos logs, se necessário
    if not df_ops.empty:
        def enrich_row(row):
            try:
                cid = row.get('credito_id')
                # Preencher data do aceite via logs se ausente
                data_aceite = row.get('data_hora_aceite')
                if (data_aceite is None) or (str(data_aceite).strip() == ''):
                    if pd.notnull(cid):
                        q = (
                            "SELECT timestamp, detalhes_humanos, nome_usuario FROM HistoricoAcoes "
                            "WHERE id_credito = ? AND (tipo_acao = 'PROPOSTA_ACEITA' "
                            "OR (tipo_acao = 'MUDANCA_STATUS_CREDITO' AND detalhes_humanos LIKE '%ACEITA%')) "
                            "ORDER BY timestamp DESC LIMIT 1"
                        )
                        r = pd.read_sql_query(q, con, params=(int(cid),))
                        if not r.empty:
                            row['data_hora_aceite'] = r.iloc[0]['timestamp']
                # Preencher valor da proposta aceita via logs se ausente/zero
                valor = row.get('valor_compra')
                try:
                    valor_zero = float(valor or 0) == 0
                except Exception:
                    valor_zero = True
                if (valor is None) or valor_zero:
                    if pd.notnull(cid):
                        qv = (
                            "SELECT detalhes_humanos FROM HistoricoAcoes "
                            "WHERE id_credito = ? AND tipo_acao IN ('PROPOSTA_ACEITA','PROPOSTA_ENVIADA') "
                            "ORDER BY timestamp DESC LIMIT 1"
                        )
                        rv = pd.read_sql_query(qv, con, params=(int(cid),))
                        if not rv.empty:
                            v = parse_currency_from_text(rv.iloc[0]['detalhes_humanos'])
                            if v is not None:
                                row['valor_compra'] = v
                return row
            except Exception:
                return row
        df_ops = df_ops.apply(enrich_row, axis=1)
        # Evitar rótulos estranhos no select por conta de linhas duplicadas
        # Mantém apenas uma linha por operação (ordenadas pelo ORDER BY acima)
        try:
            df_ops = df_ops.drop_duplicates(subset=["operacao_id"], keep="first")
        except Exception:
            pass

    if df_ops.empty:
        st.info("Nenhuma operação com Aceite encontrado.")
    else:
        # Função segura para formatar rótulos, mesmo se houver Series por acidente
        def _fmt_op(x):
            idx = df_ops.set_index("operacao_id")
            num = idx.loc[x, "numero_processo"]
            cred = idx.loc[x, "credor_nome"]
            if isinstance(num, pd.Series):
                num = num.iloc[0]
            if isinstance(cred, pd.Series):
                cred = cred.iloc[0]
            return f"Op {x} – {num} – {cred}"

        op_sel = st.selectbox(
            "Selecione a operação",
            options=df_ops["operacao_id"].tolist(),
            format_func=_fmt_op,
        )

        row = df_ops.set_index("operacao_id").loc[op_sel]

        # --- Status do Crédito (primeiro campo) ---
        st.markdown("### Status do Crédito")
        # Carregar opções existentes de status para evitar divergências com o restante do sistema
        try:
            df_status_opts = pd.read_sql_query(
                "SELECT DISTINCT TRIM(status_workflow) AS status FROM GestaoCreditos WHERE TRIM(COALESCE(status_workflow,'')) != '' ORDER BY 1",
                con,
            )
            status_opcoes = [s for s in df_status_opts["status"].dropna().tolist() if s]
        except Exception:
            status_opcoes = []
        # Garantir presença de 'Novo' como opção inicial
        if "Novo" not in status_opcoes:
            status_opcoes = ["Novo"] + [s for s in status_opcoes if s != "Novo"]

        status_atual = str(row.get("status_workflow") or "Novo")
        try:
            idx_status = status_opcoes.index(status_atual) if status_atual in status_opcoes else 0
        except Exception:
            idx_status = 0
        novo_status = st.selectbox("Status", options=status_opcoes or [status_atual], index=idx_status)
        # Botão de salvar status logo abaixo
        if st.button("Salvar Status", key=f"salvar_status_{int(op_sel)}"):
            cid = row.get("credito_id")
            try:
                cid_int = int(cid)
            except Exception:
                cid_int = None

            if cid_int is None:
                st.error("Não foi possível identificar o crédito desta operação para atualizar o Status.")
            else:
                try:
                    agora_iso = pd.Timestamp.now().isoformat()
                    # Garantir existência de registro em GestaoCreditos e atualizar status
                    con.execute("INSERT OR IGNORE INTO GestaoCreditos (id) VALUES (?)", (cid_int,))
                    con.execute(
                        "UPDATE GestaoCreditos SET status_workflow = ?, data_ultima_atualizacao = ? WHERE id = ?",
                        (str(novo_status), agora_iso, cid_int),
                    )
                    con.commit()
                    st.success("Status do crédito atualizado.")
                    # Log da alteração de status
                    try:
                        registrar_acao(
                            con,
                            "MUDANCA_STATUS_CREDITO",
                            f"Crédito {cid_int}: {status_atual} -> {novo_status}",
                            {
                                "nome_usuario": username,
                                "pagina_origem": "18_Gestao.py",
                                "id_credito": cid_int,
                                "dados_json": {
                                    "operacao_id": int(op_sel),
                                    "de": status_atual,
                                    "para": novo_status,
                                },
                            },
                        )
                    except Exception:
                        pass
                    try:
                        st.cache_data.clear()
                    except Exception:
                        pass
                    try:
                        st.cache_data.clear()
                    except Exception:
                        pass
                    try:
                        st.cache_data.clear()
                    except Exception:
                        pass
                except Exception as e:
                    st.error(f"Falha ao resetar crédito: {e}")

        st.markdown("### Informações da Proposta (somente leitura)")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.text_input("Número do Processo", value=str(row.get("numero_processo") or ""), disabled=True)
        with c2:
            st.text_input("Nome do Credor", value=str(row.get("credor_nome") or ""), disabled=True)
        with c3:
            try:
                st.text_input("Valor Original", value=f"R$ {float(row.get('valor_original') or 0):,.2f}", disabled=True)
            except Exception:
                st.text_input("Valor Original", value=str(row.get("valor_original") or ""), disabled=True)

        c4, c5 = st.columns(2)
        with c4:
            try:
                st.text_input("Valor de compra", value=f"R$ {float(row.get('valor_compra') or 0):,.2f}", disabled=True)
            except Exception:
                st.text_input("Valor de compra", value=str(row.get("valor_compra") or ""), disabled=True)
        with c5:
            st.text_input("Data/Hora do aceite", value=str(row.get("data_hora_aceite") or ""), disabled=True)

        st.markdown("### Campos Editáveis (AceitesOperacao)")
        forma = st.text_input("Forma de pagamento", value=str(row.get("forma_pagamento") or ""))
        obs = st.text_area("Observações", value=str(row.get("observacoes") or ""))

        if st.button("Salvar alterações do Aceite", type="primary"):
            con.execute(
                """
                INSERT OR REPLACE INTO AceitesOperacao 
                    (operacao_id, data_hora_aceite, credor_nome, valor_compra, forma_pagamento, observacoes)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    int(op_sel),
                    str(row.get("data_hora_aceite") or ""),
                    str(row.get("credor_nome") or ""),
                    float(row.get("valor_compra") or 0),
                    forma,
                    obs,
                ),
            )
            con.commit()
            st.success("Aceite atualizado com sucesso.")
            registrar_acao(
                con,
                "editar_aceite_operacao",
                f"Op {int(op_sel)} – atualização de aceite",
                {
                    "nome_usuario": username,
                    "pagina_origem": "18_Gestao.py",
                    "dados_json": {
                        "operacao_id": int(op_sel),
                        "forma_pagamento": forma,
                        "tem_observacoes": bool(obs),
                    },
                },
            )
            try:
                st.cache_data.clear()
            except Exception:
                pass

        st.divider()
        st.markdown("### Resetar crédito para NÃO COMPRADO")
        if st.button("Devolver crédito para NÃO COMPRADO (reset)"):
            try:
                # Identificar crédito vinculado
                cid = row.get("credito_id")
                try:
                    cid_int = int(cid)
                except Exception:
                    cid_int = None

                if cid_int is None:
                    st.error("Não foi possível identificar o crédito desta operação para reset.")
                else:
                    # Remove o registro de aceite da operação
                    con.execute("DELETE FROM AceitesOperacao WHERE operacao_id = ?", (int(op_sel),))

                    # Reseta o status de gestão do crédito para o estado inicial
                    now_iso = pd.Timestamp.now().isoformat()
                    con.execute(
                        "UPDATE GestaoCreditos SET status_workflow = 'Novo', data_ultima_atualizacao = ? WHERE id = ?",
                        (now_iso, cid_int),
                    )

                    con.commit()
                    st.success("Crédito devolvido para NÃO COMPRADO e resetado com sucesso.")

                    # Loga ação de reset para exclusão futura das listas de aceites
                    registrar_acao(
                        con,
                        "RESET_PARA_NAO_COMPRADO",
                        f"Operação {int(op_sel)} resetada; crédito {cid_int} devolvido para não comprado",
                        {
                            "nome_usuario": username,
                            "pagina_origem": "18_Gestao.py",
                            "dados_json": {
                                "operacao_id": int(op_sel),
                                "credito_id": cid_int,
                            },
                        },
                    )
                    st.cache_data.clear()
            except Exception as e:
                st.error(f"Falha ao resetar crédito: {e}")

# -------- Corrigir Créditos --------
with aba9:
    st.subheader("Corrigir Créditos")
    st.caption("Editor completo dos dados do crédito (CalculosPrecos) e status (GestaoCreditos).")

    # Descobrir dinamicamente colunas da tabela CalculosPrecos
    try:
        df_cols = pd.read_sql_query("PRAGMA table_info(CalculosPrecos)", con)
        cp_cols = df_cols["name"].tolist()
    except Exception:
        cp_cols = [
            "id",
            "numero_processo",
            "cpf_credor",
            "requerente",
            "requerido",
            "valor_liquido_final",
            "arquivo_de_origem",
        ]

    # Filtros inspirados na Consulta Técnica Crédito v2
    colf1, colf2 = st.columns([1, 2])
    with colf1:
        modo = st.radio(
            "Pesquisar por",
            options=["CPF/CNPJ", "Processo", "Nome"],
            horizontal=True,
        )
    with colf2:
        termo = st.text_input("Digite o termo de busca")

    # Execução da busca
    df_creditos = pd.DataFrame()
    params = None
    if termo.strip():
        try:
            if modo == "CPF/CNPJ":
                # Busca direta por documento (remover espaços para melhorar o match)
                termo_doc = termo.strip()
                df_creditos = pd.read_sql_query(
                    """
                    SELECT cp.*, gc.status_workflow, gc.data_ultima_atualizacao
                    FROM CalculosPrecos cp
                    LEFT JOIN GestaoCreditos gc ON gc.id = cp.id
                    WHERE TRIM(COALESCE(cp.cpf_credor,'')) LIKE ?
                    ORDER BY cp.id DESC
                    """,
                    con,
                    params=(f"%{termo_doc}%",),
                )
            elif modo == "Processo":
                # Processo geralmente é exato
                termo_proc = termo.strip()
                df_creditos = pd.read_sql_query(
                    """
                    SELECT cp.*, gc.status_workflow, gc.data_ultima_atualizacao
                    FROM CalculosPrecos cp
                    LEFT JOIN GestaoCreditos gc ON gc.id = cp.id
                    WHERE TRIM(COALESCE(cp.numero_processo,'')) = ?
                    ORDER BY cp.id DESC
                    """,
                    con,
                    params=(termo_proc,),
                )
            else:  # Nome
                like = f"%{termo.strip().upper()}%"
                df_creditos = pd.read_sql_query(
                    """
                    SELECT cp.*, gc.status_workflow, gc.data_ultima_atualizacao
                    FROM CalculosPrecos cp
                    LEFT JOIN GestaoCreditos gc ON gc.id = cp.id
                    WHERE UPPER(TRIM(COALESCE(cp.requerente,''))) LIKE ?
                    ORDER BY cp.id DESC
                    """,
                    con,
                    params=(like,),
                )
        except Exception as e:
            st.error(f"Falha na busca: {e}")

    if df_creditos.empty:
        st.info("Digite um termo e execute a busca para listar créditos.")
    else:
        st.markdown("### Créditos encontrados (edição direta)")

        # Manter cópia original para detecção de mudanças
        df_original = df_creditos.copy()

        # Editor com todas as colunas da CalculosPrecos + status_workflow
        # Configuração básica para alguns campos comuns
        column_config = {}
        if "valor_liquido_final" in df_creditos.columns:
            column_config["valor_liquido_final"] = st.column_config.NumberColumn(
                "valor_liquido_final",
                help="Valor líquido final do crédito",
                step=0.01,
                format="%.2f",
            )
        if "status_workflow" in df_creditos.columns:
            # Carregar opções existentes de status
            try:
                df_status_opts = pd.read_sql_query(
                    "SELECT DISTINCT TRIM(status_workflow) AS status FROM GestaoCreditos WHERE TRIM(COALESCE(status_workflow,'')) != '' ORDER BY 1",
                    con,
                )
                status_opcoes = [s for s in df_status_opts["status"].dropna().tolist() if s]
                if "Novo" not in status_opcoes:
                    status_opcoes = ["Novo"] + [s for s in status_opcoes if s != "Novo"]
            except Exception:
                status_opcoes = ["Novo", "Contato Iniciado", "Em Análise", "Em Negociação", "Proposta Enviada", "Proposta Aceita", "Rejeitado"]
            column_config["status_workflow"] = st.column_config.SelectboxColumn(
                "status_workflow",
                options=status_opcoes,
                help="Etapa do crédito no pipeline",
            )

        edited_df = st.data_editor(
            df_creditos,
            use_container_width=True,
            num_rows="static",
            hide_index=True,
            column_config=column_config,
            key="editor_corrigir_creditos",
        )

        if st.button("Salvar alterações", type="primary", key="salvar_alteracoes_creditos_correcao"):
            total_updates_cp = 0
            total_updates_status = 0
            mudancas_totais = []
            now_iso = pd.Timestamp.now().isoformat()

            # Índice original por id para comparação
            try:
                orig_idx = df_original.set_index("id")
                edit_idx = edited_df.set_index("id")
            except Exception:
                st.error("Esperado campo 'id' nas linhas dos créditos para salvar.")
                st.stop()

            for cid, row in edit_idx.iterrows():
                # Comparar valores por coluna
                mudancas = {}
                if cid in orig_idx.index:
                    for col in edited_df.columns:
                        try:
                            ov = orig_idx.loc[cid, col]
                        except Exception:
                            ov = None
                        nv = row.get(col)
                        # Normalizar None/NaN para comparação
                        ov_norm = None if pd.isna(ov) else ov
                        nv_norm = None if pd.isna(nv) else nv
                        if ov_norm != nv_norm:
                            mudancas[col] = {"old": ov_norm, "new": nv_norm}
                else:
                    # Linha nova não deve ocorrer em num_rows='static'
                    continue

                if not mudancas:
                    continue

                # Atualizar CalculosPrecos para colunas pertinentes
                cols_to_update = [c for c in cp_cols if c in mudancas and c != "id"]
                if cols_to_update:
                    set_clause = ", ".join([f"{c} = ?" for c in cols_to_update])
                    values = [row.get(c) for c in cols_to_update]
                    try:
                        con.execute(
                            f"UPDATE CalculosPrecos SET {set_clause} WHERE id = ?",
                            tuple(values + [int(cid)]),
                        )
                        total_updates_cp += 1
                    except Exception as e:
                        st.error(f"Falha ao atualizar CalculosPrecos (id={cid}): {e}")

                # Atualizar status em GestaoCreditos, se alterado
                if "status_workflow" in mudancas:
                    try:
                        novo_status = str(row.get("status_workflow") or "Novo")
                        con.execute("INSERT OR IGNORE INTO GestaoCreditos (id, status_workflow) VALUES (?, ?)", (int(cid), novo_status))
                        con.execute(
                            "UPDATE GestaoCreditos SET status_workflow = ?, data_ultima_atualizacao = ? WHERE id = ?",
                            (novo_status, now_iso, int(cid)),
                        )
                        total_updates_status += 1
                    except Exception as e:
                        st.error(f"Falha ao atualizar GestaoCreditos (id={cid}): {e}")

                # Logar correção
                try:
                    registrar_acao(
                        con,
                        "CORRECAO_CREDITO",
                        f"Crédito {int(cid)} corrigido ({len(mudancas)} campo(s))",
                        {
                            "nome_usuario": username,
                            "pagina_origem": "18_Gestao.py",
                            "id_credito": int(cid),
                            "dados_json": {
                                "mudancas": mudancas,
                                "mudancas_count": len(mudancas),
                            },
                        },
                    )
                except Exception:
                    pass
                mudancas_totais.append({"id": int(cid), "mudancas": mudancas})

            con.commit()
            st.success(
                f"Alterações salvas: {total_updates_cp} crédito(s) atualizados e {total_updates_status} status(es) alterados."
            )
            try:
                st.cache_data.clear()
            except Exception:
                pass

        # ---- Editor completo do Processo de Compra (Operação & Aceite) ----
        st.divider()
        st.markdown("### Editar Processo de Compra (Operação & Aceite)")

        # Helper para formatar seleção de crédito
        def _fmt_credito(cid: int) -> str:
            try:
                r = df_creditos.set_index("id").loc[cid]
                nome = str(r.get("requerente") or "?")
                proc = str(r.get("numero_processo") or "?")
                val = float(r.get("valor_liquido_final") or 0)
                return f"ID {cid} · {nome} · {proc} · R$ {val:,.2f}"
            except Exception:
                return str(cid)

        cid_sel = st.selectbox(
            "Selecione o crédito",
            options=df_creditos["id"].tolist(),
            format_func=_fmt_credito,
            key="cid_proc_compra",
        )

        # Carregar dados auxiliares
        df_grupos = listar_grupos()
        try:
            df_usuarios = listar_usuarios()
        except Exception:
            df_usuarios = pd.DataFrame(columns=["nome_usuario"])  # fallback

        # Descobrir vínculo de grupo atual do crédito
        try:
            rv_grp = pd.read_sql_query(
                "SELECT grupo_id FROM GruposCreditos WHERE credito_id = ? ORDER BY rowid DESC LIMIT 1",
                con,
                params=(int(cid_sel),),
            )
            grupo_atual = int(rv_grp.iloc[0]["grupo_id"]) if not rv_grp.empty and pd.notna(rv_grp.iloc[0]["grupo_id"]) else None
        except Exception:
            grupo_atual = None

        # Buscar operação vinculada (última)
        rv_op = pd.read_sql_query(
            "SELECT * FROM Operacoes WHERE credito_id = ? ORDER BY id DESC LIMIT 1",
            con,
            params=(int(cid_sel),),
        )
        op_exists = not rv_op.empty
        op_id = int(rv_op.iloc[0]["id"]) if op_exists else None

        # Opções de status de operação
        status_operacao_opcoes = [
            "Aceite",
            "Em Documentação",
            "Contrato",
            "Redação",
            "Due Diligence",
            "Assinatura",
            "Homologação",
            "Pagamento",
            "Encerrado",
        ]

        st.markdown("#### Operação vinculada ao crédito")
        col_o1, col_o2, col_o3 = st.columns([1, 1, 2])
        with col_o1:
            st.text_input("Operação (ID)", value=str(op_id or ""), disabled=True)
        with col_o2:
            # Selecionar grupo
            grupos_opts = df_grupos["id"].tolist() if not df_grupos.empty else []
            fmt_grupo = (lambda gid: df_grupos.set_index("id").loc[gid, "nome"] if not df_grupos.empty and gid in df_grupos["id"].tolist() else str(gid))
            idx_gid = grupos_opts.index(grupo_atual) if grupo_atual in grupos_opts else 0 if grupos_opts else None
            gid_sel = st.selectbox("Grupo", options=grupos_opts or [None], index=idx_gid if idx_gid is not None else 0, format_func=fmt_grupo)
        with col_o3:
            # Selecionar operador
            usuarios_opts = df_usuarios["nome_usuario"].dropna().tolist()
            try:
                operador_atual = str(rv_op.iloc[0].get("operador_username") or "") if op_exists else str(username or "")
            except Exception:
                operador_atual = str(username or "")
            try:
                idx_op = usuarios_opts.index(operador_atual) if operador_atual in usuarios_opts else 0
            except Exception:
                idx_op = 0
            operador_sel = st.selectbox("Operador", options=usuarios_opts or [operador_atual], index=idx_op)

        # Campos principais da operação
        r_cp = df_creditos.set_index("id").loc[cid_sel]
        numero_processo_val = st.text_input("Número do Processo (Operação)", value=str(rv_op.iloc[0].get("numero_processo") if op_exists else (r_cp.get("numero_processo") or "")))
        status_atual_val = st.selectbox(
            "Status da Operação",
            options=status_operacao_opcoes,
            index=(status_operacao_opcoes.index(str(rv_op.iloc[0].get("status_atual") or "Aceite")) if op_exists else status_operacao_opcoes.index("Aceite")),
        )

        if st.button("Salvar Operação", type="primary", key=f"salvar_operacao_credito_{int(cid_sel)}"):
            try:
                agora_iso = pd.Timestamp.now().isoformat()
                # Criar se não existe
                if not op_exists:
                    # Inferir tipo_pessoa a partir do documento
                    doc = str(r_cp.get("cpf_credor") or "").strip()
                    tipo_pessoa = "Física" if re.sub(r"\D", "", doc).__len__() == 11 else "Jurídica"
                    tipo_credito = "Precatório"
                    con.execute(
                        "INSERT INTO Operacoes (credito_id, grupo_id, operador_username, tipo_pessoa, tipo_credito, numero_processo, status_atual, criado_em) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (int(cid_sel), gid_sel, operador_sel, tipo_pessoa, tipo_credito, numero_processo_val.strip(), status_atual_val.strip(), agora_iso),
                    )
                    con.commit()
                    # Recarregar
                    rv_op = pd.read_sql_query(
                        "SELECT * FROM Operacoes WHERE credito_id = ? ORDER BY id DESC LIMIT 1",
                        con,
                        params=(int(cid_sel),),
                    )
                    op_id = int(rv_op.iloc[0]["id"]) if not rv_op.empty else None
                    op_exists = not rv_op.empty
                    st.success(f"Operação criada (ID {op_id}).")
                    try:
                        registrar_acao(con, "CRIAR_OPERACAO", f"Crédito {int(cid_sel)} -> Operação {op_id}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"credito_id": int(cid_sel), "operacao_id": op_id}})
                    except Exception:
                        pass
                else:
                    # Atualizar campos principais
                    con.execute(
                        "UPDATE Operacoes SET grupo_id = ?, operador_username = ?, numero_processo = ?, status_atual = ? WHERE id = ?",
                        (gid_sel, operador_sel, numero_processo_val.strip(), status_atual_val.strip(), int(op_id)),
                    )
                    con.commit()
                    st.success("Operação atualizada.")
                    try:
                        registrar_acao(con, "ATUALIZAR_OPERACAO", f"Operação {int(op_id)} atualizada", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                    except Exception:
                        pass
                try:
                    st.cache_data.clear()
                except Exception:
                    pass
            except Exception as e:
                st.error(f"Falha ao salvar operação: {e}")

        # Editor de dados de Aceite
        st.markdown("#### Dados de Aceite da Operação")
        if not op_exists:
            st.info("Crie/salve a operação acima para editar os dados de aceite.")
        else:
            rv_ac = pd.read_sql_query(
                "SELECT * FROM AceitesOperacao WHERE operacao_id = ?",
                con,
                params=(int(op_id),),
            )
            # Valores atuais ou defaults
            credor_nome_val = st.text_input("Nome do Credor (aceite)", value=str(rv_ac.iloc[0].get("credor_nome") if not rv_ac.empty else (r_cp.get("requerente") or "")), key=f"aceite_credor_nome_{int(op_id)}")
            try:
                valor_compra_val = float(rv_ac.iloc[0].get("valor_compra") if not rv_ac.empty else 0)
            except Exception:
                valor_compra_val = 0.0
            valor_compra_val = st.number_input("Valor de Compra (aceite)", value=valor_compra_val, min_value=0.0, step=100.0, key=f"aceite_valor_compra_{int(op_id)}")
            data_hora_aceite_val = st.text_input("Data/Hora do Aceite (ISO)", value=str(rv_ac.iloc[0].get("data_hora_aceite") if not rv_ac.empty else ""), key=f"aceite_datahora_{int(op_id)}")
            forma_pagamento_val = st.text_input("Forma de Pagamento", value=str(rv_ac.iloc[0].get("forma_pagamento") if not rv_ac.empty else ""), key=f"aceite_forma_pagamento_{int(op_id)}")
            observacoes_val = st.text_area("Observações", value=str(rv_ac.iloc[0].get("observacoes") if not rv_ac.empty else ""), key=f"aceite_observacoes_{int(op_id)}")

            if st.button("Salvar Dados de Aceite", type="primary", key=f"salvar_aceite_credito_{int(cid_sel)}"):
                try:
                    if rv_ac.empty:
                        con.execute(
                            "INSERT INTO AceitesOperacao (operacao_id, valor_compra, data_hora_aceite, forma_pagamento, observacoes, credor_nome) VALUES (?, ?, ?, ?, ?, ?)",
                            (int(op_id), float(valor_compra_val or 0), str(data_hora_aceite_val or ""), str(forma_pagamento_val or ""), str(observacoes_val or ""), str(credor_nome_val or "")),
                        )
                    else:
                        con.execute(
                            "UPDATE AceitesOperacao SET valor_compra = ?, data_hora_aceite = ?, forma_pagamento = ?, observacoes = ?, credor_nome = ? WHERE operacao_id = ?",
                            (float(valor_compra_val or 0), str(data_hora_aceite_val or ""), str(forma_pagamento_val or ""), str(observacoes_val or ""), str(credor_nome_val or ""), int(op_id)),
                        )
                    con.commit()
                    st.success("Dados de aceite salvos.")
                    try:
                        registrar_acao(con, "SALVAR_ACEITE", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id), "credito_id": int(cid_sel)}})
                    except Exception:
                        pass
                    try:
                        st.cache_data.clear()
                    except Exception:
                        pass
                except Exception as e:
                    st.error(f"Falha ao salvar dados de aceite: {e}")

            # Etapas complementares da operação
            st.markdown("#### Etapas da Operação")
            tab_aceite, tab_docs, tab_contrato, tab_redacao, tab_dd, tab_assin, tab_homol, tab_pag = st.tabs([
                "Aceite", "Documentos", "Contrato", "Redação", "Due Diligence", "Assinatura", "Homologação", "Pagamento"
            ])

            # Reexibir resumo do aceite na primeira aba para contexto
            with tab_aceite:
                st.write("Resumo do aceite já editado acima.")
                st.text_input("Operação", value=str(op_id), disabled=True, key=f"aceite_resumo_operacao_{int(op_id)}")
                st.text_input("Credor", value=str(credor_nome_val or ""), disabled=True, key=f"aceite_resumo_credor_{int(op_id)}")
                st.text_input("Valor de compra", value=f"R$ {float(valor_compra_val or 0):,.2f}", disabled=True, key=f"aceite_resumo_valor_{int(op_id)}")
                st.text_input("Data/Hora", value=str(data_hora_aceite_val or ""), disabled=True, key=f"aceite_resumo_data_{int(op_id)}")

            # Documentos
            with tab_docs:
                st.markdown("Checklist e documentos da operação")
                # Determinar tipo_pessoa
                try:
                    rv_tp = pd.read_sql_query("SELECT tipo_pessoa FROM Operacoes WHERE id = ?", con, params=(int(op_id),))
                    tipo_pessoa = str(rv_tp.iloc[0][0] or "") if not rv_tp.empty else ""
                except Exception:
                    tipo_pessoa = ""

                # Gerar checklist padrão
                if st.button("Gerar checklist padrão", key=f"gerar_checklist_{int(op_id)}"):
                    try:
                        itens_pf = [
                            ("PF", "RG/CNH/CPF"),
                            ("PF", "Comprovante de endereço"),
                            ("PF", "Estado civil/Regime de bens"),
                            ("PF", "Anuência do cônjuge"),
                            ("PF", "Dados bancários"),
                        ]
                        itens_pj = [
                            ("PJ", "Contrato/Estatuto + última alteração"),
                            ("PJ", "Documento(s) do representante"),
                            ("PJ", "Procuração/Ata de poderes"),
                            ("PJ", "Comprovante de endereço"),
                            ("PJ", "Dados bancários"),
                        ]
                        itens_cr = [
                            ("CREDITO", "Número do processo e tribunal"),
                            ("CREDITO", "Peças essenciais (sentença/acórdão/etc.)"),
                            ("CREDITO", "Comprovante de precatório/RPV"),
                        ]
                        lista = []
                        if tipo_pessoa == "Física" or tipo_pessoa == "PF":
                            lista += itens_pf
                        if tipo_pessoa == "Jurídica" or tipo_pessoa == "PJ":
                            lista += itens_pj
                        lista += itens_cr
                        for cat, nome in lista:
                            con.execute(
                                "INSERT OR IGNORE INTO DocumentosOperacao (operacao_id, categoria, item_nome, status, obs) VALUES (?, ?, ?, 'Pendente', '')",
                                (int(op_id), cat, nome),
                            )
                        con.commit()
                        st.success("Checklist padrão gerado.")
                        try:
                            registrar_acao(con, "GERAR_CHECKLIST_DOCS", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao gerar checklist: {e}")

                # Editor de documentos
                df_docs = pd.read_sql_query(
                    "SELECT id, categoria, item_nome, status, obs FROM DocumentosOperacao WHERE operacao_id = ? ORDER BY id",
                    con,
                    params=(int(op_id),),
                )
                status_opts = ["Pendente", "Em Análise", "Validado", "Recusado"]
                edited_docs = st.data_editor(
                    df_docs,
                    use_container_width=True,
                    num_rows="dynamic",
                    hide_index=True,
                    column_config={
                        "status": st.column_config.SelectboxColumn("status", options=status_opts),
                        "obs": st.column_config.TextColumn("obs"),
                    },
                    key=f"editor_docs_{int(op_id)}",
                )
                if st.button("Salvar documentos", type="primary", key=f"salvar_docs_{int(op_id)}"):
                    try:
                        # Distinguir updates e inserts
                        orig_idx = df_docs.set_index("id") if not df_docs.empty else pd.DataFrame()
                        for _, r in edited_docs.iterrows():
                            rid = r.get("id")
                            cat = str(r.get("categoria") or "")
                            nome = str(r.get("item_nome") or "")
                            status = str(r.get("status") or "Pendente")
                            obs = str(r.get("obs") or "")
                            if pd.isna(rid) or rid is None or str(rid).strip() == "":
                                con.execute(
                                    "INSERT INTO DocumentosOperacao (operacao_id, categoria, item_nome, status, obs) VALUES (?, ?, ?, ?, ?)",
                                    (int(op_id), cat, nome, status, obs),
                                )
                            else:
                                con.execute(
                                    "UPDATE DocumentosOperacao SET categoria = ?, item_nome = ?, status = ?, obs = ? WHERE id = ? AND operacao_id = ?",
                                    (cat, nome, status, obs, int(rid), int(op_id)),
                                )
                        con.commit()
                        st.success("Documentos salvos.")
                        try:
                            registrar_acao(con, "SALVAR_DOCUMENTOS", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar documentos: {e}")

            # Contrato
            with tab_contrato:
                df_con = pd.read_sql_query("SELECT * FROM ContratosOperacao WHERE operacao_id = ?", con, params=(int(op_id),))
                tipo_contrato = st.text_input("Tipo de contrato", value=str(df_con.iloc[0].get("tipo_contrato") if not df_con.empty else ""), key=f"contrato_tipo_{int(op_id)}")
                cond_pag = st.text_input("Condição de pagamento", value=str(df_con.iloc[0].get("condicao_pagamento") if not df_con.empty else ""), key=f"contrato_cond_{int(op_id)}")
                prazos = st.text_area("Prazos", value=str(df_con.iloc[0].get("prazos") if not df_con.empty else ""), key=f"contrato_prazos_{int(op_id)}")
                if st.button("Salvar contrato", type="primary", key=f"salvar_contrato_{int(op_id)}"):
                    try:
                        if df_con.empty:
                            con.execute(
                                "INSERT INTO ContratosOperacao (operacao_id, tipo_contrato, condicao_pagamento, prazos) VALUES (?, ?, ?, ?)",
                                (int(op_id), tipo_contrato, cond_pag, prazos),
                            )
                        else:
                            con.execute(
                                "UPDATE ContratosOperacao SET tipo_contrato = ?, condicao_pagamento = ?, prazos = ? WHERE operacao_id = ?",
                                (tipo_contrato, cond_pag, prazos, int(op_id)),
                            )
                        con.commit()
                        st.success("Contrato salvo.")
                        try:
                            registrar_acao(con, "SALVAR_CONTRATO", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar contrato: {e}")

            # Redação
            with tab_redacao:
                df_red = pd.read_sql_query("SELECT * FROM RedacoesOperacao WHERE operacao_id = ?", con, params=(int(op_id),))
                status_red = st.text_input("Status da redação", value=str(df_red.iloc[0].get("status_redacao") if not df_red.empty else ""), key=f"redacao_status_{int(op_id)}")
                arquivo_link = st.text_input("Link do arquivo", value=str(df_red.iloc[0].get("arquivo_link") if not df_red.empty else ""), key=f"redacao_arquivo_{int(op_id)}")
                if st.button("Salvar redação", type="primary", key=f"salvar_redacao_{int(op_id)}"):
                    try:
                        if df_red.empty:
                            con.execute(
                                "INSERT INTO RedacoesOperacao (operacao_id, status_redacao, arquivo_link) VALUES (?, ?, ?)",
                                (int(op_id), status_red, arquivo_link),
                            )
                        else:
                            con.execute(
                                "UPDATE RedacoesOperacao SET status_redacao = ?, arquivo_link = ? WHERE operacao_id = ?",
                                (status_red, arquivo_link, int(op_id)),
                            )
                        con.commit()
                        st.success("Redação salva.")
                        try:
                            registrar_acao(con, "SALVAR_REDACAO", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar redação: {e}")

            # Due Diligence
            with tab_dd:
                df_dd = pd.read_sql_query("SELECT * FROM DueDiligencias WHERE operacao_id = ?", con, params=(int(op_id),))
                dd_tipo = st.text_input("Tipo (Simplificada/Completa)", value=str(df_dd.iloc[0].get("tipo") if not df_dd.empty else ""), key=f"dd_tipo_{int(op_id)}")
                dd_result = st.text_input("Resultado", value=str(df_dd.iloc[0].get("resultado") if not df_dd.empty else ""), key=f"dd_result_{int(op_id)}")
                dd_parecer = st.text_area("Parecer", value=str(df_dd.iloc[0].get("parecer") if not df_dd.empty else ""), key=f"dd_parecer_{int(op_id)}")
                if st.button("Salvar due diligence", type="primary", key=f"salvar_dd_{int(op_id)}"):
                    try:
                        if df_dd.empty:
                            con.execute(
                                "INSERT INTO DueDiligencias (operacao_id, tipo, resultado, parecer) VALUES (?, ?, ?, ?)",
                                (int(op_id), dd_tipo, dd_result, dd_parecer),
                            )
                        else:
                            con.execute(
                                "UPDATE DueDiligencias SET tipo = ?, resultado = ?, parecer = ? WHERE operacao_id = ?",
                                (dd_tipo, dd_result, dd_parecer, int(op_id)),
                            )
                        con.commit()
                        st.success("Due diligence salva.")
                        try:
                            registrar_acao(con, "SALVAR_DUE_DILIGENCE", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar due diligence: {e}")

            # Assinatura
            with tab_assin:
                df_ass = pd.read_sql_query("SELECT * FROM AssinaturasOperacao WHERE operacao_id = ?", con, params=(int(op_id),))
                status_ass = st.text_input("Status da assinatura", value=str(df_ass.iloc[0].get("status_assinatura") if not df_ass.empty else ""), key=f"assin_status_{int(op_id)}")
                data_ass = st.text_input("Data da assinatura", value=str(df_ass.iloc[0].get("data_assinatura") if not df_ass.empty else ""), key=f"assin_data_{int(op_id)}")
                assinantes = st.text_area("Assinantes", value=str(df_ass.iloc[0].get("assinantes") if not df_ass.empty else ""), key=f"assin_assinantes_{int(op_id)}")
                if st.button("Salvar assinatura", type="primary", key=f"salvar_assin_{int(op_id)}"):
                    try:
                        if df_ass.empty:
                            con.execute(
                                "INSERT INTO AssinaturasOperacao (operacao_id, status_assinatura, data_assinatura, assinantes) VALUES (?, ?, ?, ?)",
                                (int(op_id), status_ass, data_ass, assinantes),
                            )
                        else:
                            con.execute(
                                "UPDATE AssinaturasOperacao SET status_assinatura = ?, data_assinatura = ?, assinantes = ? WHERE operacao_id = ?",
                                (status_ass, data_ass, assinantes, int(op_id)),
                            )
                        con.commit()
                        st.success("Assinatura salva.")
                        try:
                            registrar_acao(con, "SALVAR_ASSINATURA", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar assinatura: {e}")

            # Homologação
            with tab_homol:
                df_hom = pd.read_sql_query("SELECT * FROM HomologacoesOperacao WHERE operacao_id = ?", con, params=(int(op_id),))
                data_pet = st.text_input("Data da petição", value=str(df_hom.iloc[0].get("data_peticao") if not df_hom.empty else ""), key=f"homol_peticao_{int(op_id)}")
                status_hom = st.text_input("Status da homologação", value=str(df_hom.iloc[0].get("status_homologacao") if not df_hom.empty else ""), key=f"homol_status_{int(op_id)}")
                data_hom = st.text_input("Data da homologação", value=str(df_hom.iloc[0].get("data_homologacao") if not df_hom.empty else ""), key=f"homol_data_{int(op_id)}")
                if st.button("Salvar homologação", type="primary", key=f"salvar_homol_{int(op_id)}"):
                    try:
                        if df_hom.empty:
                            con.execute(
                                "INSERT INTO HomologacoesOperacao (operacao_id, data_peticao, status_homologacao, data_homologacao) VALUES (?, ?, ?, ?)",
                                (int(op_id), data_pet, status_hom, data_hom),
                            )
                        else:
                            con.execute(
                                "UPDATE HomologacoesOperacao SET data_peticao = ?, status_homologacao = ?, data_homologacao = ? WHERE operacao_id = ?",
                                (data_pet, status_hom, data_hom, int(op_id)),
                            )
                        con.commit()
                        st.success("Homologação salva.")
                        try:
                            registrar_acao(con, "SALVAR_HOMOLOGACAO", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar homologação: {e}")

            # Pagamento
            with tab_pag:
                df_pag = pd.read_sql_query("SELECT * FROM PagamentosOperacao WHERE operacao_id = ?", con, params=(int(op_id),))
                data_pag = st.text_input("Data do pagamento", value=str(df_pag.iloc[0].get("data_pagamento") if not df_pag.empty else ""), key=f"pag_data_{int(op_id)}")
                try:
                    valor_pago = float(df_pag.iloc[0].get("valor_pago") if not df_pag.empty else 0)
                except Exception:
                    valor_pago = 0.0
                valor_pago = st.number_input("Valor pago", value=valor_pago, min_value=0.0, step=100.0, key=f"pag_valor_{int(op_id)}")
                metodo_pag = st.text_input("Método", value=str(df_pag.iloc[0].get("metodo") if not df_pag.empty else ""), key=f"pag_metodo_{int(op_id)}")
                if st.button("Salvar pagamento", type="primary", key=f"salvar_pag_{int(op_id)}"):
                    try:
                        if df_pag.empty:
                            con.execute(
                                "INSERT INTO PagamentosOperacao (operacao_id, data_pagamento, valor_pago, metodo) VALUES (?, ?, ?, ?)",
                                (int(op_id), data_pag, float(valor_pago or 0), metodo_pag),
                            )
                        else:
                            con.execute(
                                "UPDATE PagamentosOperacao SET data_pagamento = ?, valor_pago = ?, metodo = ? WHERE operacao_id = ?",
                                (data_pag, float(valor_pago or 0), metodo_pag, int(op_id)),
                            )
                        con.commit()
                        st.success("Pagamento salvo.")
                        try:
                            registrar_acao(con, "SALVAR_PAGAMENTO", f"Operação {int(op_id)}", {"nome_usuario": username, "pagina_origem": "18_Gestao.py", "dados_json": {"operacao_id": int(op_id)}})
                        except Exception:
                            pass
                    except Exception as e:
                        st.error(f"Falha ao salvar pagamento: {e}")
            try:
                registrar_acao(
                    con,
                    "CORRECAO_CREDITO_MASSA",
                    f"Correção em massa concluída ({len(mudancas_totais)} linha(s))",
                    {
                        "nome_usuario": username,
                        "pagina_origem": "18_Gestao.py",
                        "dados_json": {
                            "linhas": mudancas_totais,
                            "linhas_count": len(mudancas_totais),
                        },
                    },
                )
            except Exception:
                pass
            try:
                st.cache_data.clear()
            except Exception:
                pass