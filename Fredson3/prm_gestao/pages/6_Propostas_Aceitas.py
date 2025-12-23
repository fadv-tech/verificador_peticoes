import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import sys
import os
from pathlib import Path
import re

# Adiciona o diretório pai ao PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from modules.db import conectar_db, registrar_acao, atualizar_status_credito

st.set_page_config(page_title="6 – Propostas Aceitas", layout="wide")

# --- Controle de acesso ---
auth = st.session_state.get("authentication_status")
perfil = st.session_state.get("perfil")
username = st.session_state.get("username")
grupo_id = st.session_state.get("grupo_id")
if not auth:
    st.warning("Faça login para acessar.")
    st.stop()

con = conectar_db()
st.title("Propostas Aceitas – Pós-aceite e Formalização")
st.caption("Fluxo completo após aceitação: documentos, contrato, redação, due diligence, assinatura, homologação, pagamento e encerramento.")

# --- Tabelas auxiliares (autocontidas) ---

def init_tables(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS Operacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        credito_id INTEGER,
        grupo_id INTEGER,
        operador_username TEXT,
        tipo_pessoa TEXT,
        tipo_credito TEXT,
        numero_processo TEXT,
        status_atual TEXT,
        criado_em TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS AceitesOperacao (
        operacao_id INTEGER PRIMARY KEY,
        data_hora_aceite TEXT,
        credor_nome TEXT,
        valor_compra REAL,
        forma_pagamento TEXT,
        observacoes TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS DocumentosOperacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operacao_id INTEGER,
        categoria TEXT,
        item_codigo TEXT,
        item_nome TEXT,
        status TEXT,
        obs TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS ContratosOperacao (
        operacao_id INTEGER PRIMARY KEY,
        tipo_contrato TEXT,
        condicao_pagamento TEXT,
        prazos TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS RedacoesOperacao (
        operacao_id INTEGER PRIMARY KEY,
        status_redacao TEXT,
        arquivo_link TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS DueDiligencias (
        operacao_id INTEGER PRIMARY KEY,
        tipo TEXT,
        resultado TEXT,
        parecer TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS AssinaturasOperacao (
        operacao_id INTEGER PRIMARY KEY,
        status_assinatura TEXT,
        data_assinatura TEXT,
        assinantes TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS HomologacoesOperacao (
        operacao_id INTEGER PRIMARY KEY,
        data_peticao TEXT,
        status_homologacao TEXT,
        data_homologacao TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS PagamentosOperacao (
        operacao_id INTEGER PRIMARY KEY,
        data_pagamento TEXT,
        valor_pago REAL,
        metodo TEXT
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS EncerramentosOperacao (
        operacao_id INTEGER PRIMARY KEY,
        status_final TEXT,
        data_encerramento TEXT,
        resumo TEXT
    )
    """)
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

DEFAULTS = [
    ("Aceite", "ACEITE_DATA_HORA", "Data/Hora do aceite"),
    ("Aceite", "ACEITE_CREDOR_NOME", "Nome do credor"),
    ("Aceite", "ACEITE_VALOR", "Valor de compra"),
    ("Aceite", "ACEITE_FORMA_PAGAMENTO", "Forma de pagamento"),
    ("Aceite", "ACEITE_TIPO_CREDITO", "Tipo de crédito"),
    ("Aceite", "ACEITE_NUMERO_PROCESSO", "Número do processo"),
    ("Aceite", "ACEITE_OPERADOR", "Operador responsável"),
    ("Documentos PF", "PF_RG_CNH_CPF", "RG/CNH/CPF"),
    ("Documentos PF", "PF_COMPROVANTE_ENDERECO", "Comprovante de endereço"),
    ("Documentos PF", "PF_ESTADO_CIVIL_REGIME", "Estado civil/Regime de bens"),
    ("Documentos PF", "PF_ANUENCIA_CONJUGE", "Anuência do cônjuge"),
    ("Documentos PF", "PF_DADOS_BANCARIOS", "Dados bancários"),
    ("Documentos PJ", "PJ_CONTRATO_ESTATUTO", "Contrato/Estatuto + última alteração"),
    ("Documentos PJ", "PJ_DOC_REPRESENTANTE", "Documento(s) do representante"),
    ("Documentos PJ", "PJ_PODERES", "Procuração/Ata de poderes"),
    ("Documentos PJ", "PJ_COMPROVANTE_ENDERECO", "Comprovante de endereço"),
    ("Documentos PJ", "PJ_DADOS_BANCARIOS", "Dados bancários"),
    ("Documentos Crédito", "CR_NUMERO_PROCESSO_TRIBUNAL", "Número do processo e tribunal"),
    ("Documentos Crédito", "CR_PECAS_ESSENCIAIS", "Peças essenciais (sentença/acórdão/etc.)"),
    ("Documentos Crédito", "CR_COMPROVANTE_PRECAT_RPV", "Comprovante de precatório/RPV"),
    ("Contrato", "CON_TIPO_CONTRATO", "Tipo de contrato"),
    ("Contrato", "CON_CONDICAO_PAGAMENTO", "Condição de pagamento"),
    ("Contrato", "CON_PRAZOS", "Prazos principais"),
    ("Redação", "RED_PRONTO", "Redação pronta"),
    ("Due Diligence", "DD_TIPO", "Tipo (Simplificada/Completa)"),
    ("Due Diligence", "DD_RESULTADO", "Resultado/Parecer"),
    ("Assinatura", "ASS_ASSINADO", "Assinado"),
    ("Homologação", "HOM_PETICAO", "Petição protocolada"),
    ("Homologação", "HOM_HOMOLOGADO", "Homologado"),
    ("Pagamento", "PAG_REALIZADO", "Pagamento realizado"),
    ("Encerramento", "ENC_ENCERRADO", "Encerrado")
]

def seed_config_if_empty(con):
    df = pd.read_sql_query("SELECT COUNT(*) AS c FROM ConfigPropostasAceitasRequisitos", con)
    if int(df.iloc[0]["c"]) == 0:
        now = datetime.now().isoformat()
        for etapa, codigo, nome in DEFAULTS:
            con.execute(
                "INSERT OR IGNORE INTO ConfigPropostasAceitasRequisitos (etapa, item_codigo, item_nome, obrigatorio, ativo, updated_at, updated_by) VALUES (?, ?, ?, 1, 1, ?, ?)",
                (etapa, codigo, nome, now, username or "system")
            )
        con.commit()

init_tables(con)
seed_config_if_empty(con)

@st.cache_data(ttl=60)
def get_config(con):
    return pd.read_sql_query("SELECT etapa, item_codigo, item_nome, obrigatorio, ativo FROM ConfigPropostasAceitasRequisitos", con)

# Utilitário simples para extrair valores monetários de descrições de log
def parse_currency_from_text(text):
    try:
        if not text:
            return None
        s = str(text)
        # Padrão BR: "R$ 12.345,67" ou "12.345,67"
        br_matches = re.findall(r'(?:R\$\s*)?([0-9\.]+,[0-9]{2})', s)
        # Padrão US: "R$ 11,000.00" ou "11,000.00"
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

def get_aceite_prefill(con, operacao_id: int):
    try:
        base = pd.read_sql_query(
            """
            SELECT o.id as operacao_id, o.credito_id, o.numero_processo,
                   a.data_hora_aceite, a.credor_nome as credor_nome_aceite, a.valor_compra as valor_compra_aceite,
                   gc.valor_ultima_proposta,
                   cp.requerente, ed.polo_ativo_credor
            FROM Operacoes o
            LEFT JOIN AceitesOperacao a ON a.operacao_id = o.id
            LEFT JOIN GestaoCreditos gc ON gc.id = o.credito_id
            LEFT JOIN CalculosPrecos cp ON cp.numero_processo = o.numero_processo
            LEFT JOIN ExtracaoDiario ed ON ed.numero_processo_cnj = o.numero_processo
            WHERE o.id = ?
            """,
            con,
            params=(operacao_id,),
        )
        if base.empty:
            return {}
        row = base.iloc[0].to_dict()
        data_hora = row.get("data_hora_aceite")
        aceite_por = None  # Não usar logs; manter vazio se não houver na tabela de aceite

        credor = row.get("credor_nome_aceite") or row.get("requerente") or row.get("polo_ativo_credor")

        valor = row.get("valor_compra_aceite")
        try:
            valor_num = float(valor) if valor is not None else 0.0
        except Exception:
            valor_num = 0.0
        if valor is None or valor_num == 0.0:
            val2 = row.get("valor_ultima_proposta")
            try:
                val2_num = float(val2) if val2 is not None else 0.0
            except Exception:
                val2_num = 0.0
            if val2 is not None and val2_num != 0.0:
                valor = val2

        return {
            "data_hora": data_hora,
            "credor": credor,
            "valor": valor,
            "aceite_por": aceite_por,
        }
    except Exception:
        return {}

# --- Migração: mover créditos 'Em Documentação' para 'Proposta Aceita' ---
def migrar_em_documentacao_para_proposta_aceita(con, grupo_id, username, perfil):
    try:
        ts = datetime.now().isoformat()
        params_sel = []
        where = "TRIM(COALESCE(status_workflow,'')) = 'Em Documentação'"
        if perfil != 'Admin' and grupo_id:
            where += " AND id IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)"
            params_sel.append(int(grupo_id))

        # Quantos serão migrados?
        q_count = "SELECT COUNT(1) FROM GestaoCreditos WHERE " + where
        before_df = pd.read_sql_query(q_count, con, params=params_sel)
        before_count = int(before_df.iloc[0][0]) if not before_df.empty else 0

        if before_count == 0:
            registrar_acao(con, "migracao_status_em_documentacao", "Nenhum crédito para migrar", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "dados_json": {"grupo_id": grupo_id, "perfil": perfil, "migrados": 0}})
            return 0

        # Executa migração
        params_upd = [ts]
        sql_upd = "UPDATE GestaoCreditos SET status_workflow = 'Proposta Aceita', data_ultima_atualizacao = ? WHERE " + where
        if perfil != 'Admin' and grupo_id:
            params_upd.append(int(grupo_id))
        con.execute(sql_upd, params_upd)
        con.commit()

        registrar_acao(con, "migracao_status_em_documentacao", f"Migrados {before_count} crédito(s) para Proposta Aceita", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "dados_json": {"grupo_id": grupo_id, "perfil": perfil, "migrados": before_count}})
        return before_count
    except Exception as e:
        registrar_acao(con, "erro_migracao_status", f"Erro migrando status 'Em Documentação': {e}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py"})
        return 0

# --- Varredura: incluir créditos 'Proposta Aceita' como operações na página 6 ---
def varrer_creditos_em_documentacao(con, grupo_id, username, perfil):
    try:
        params = []
        sql = (
            "SELECT gc.id AS credito_id, cp.numero_processo, TRIM(COALESCE(cp.cpf_credor,'')) AS cpf_credor "
            "FROM GestaoCreditos gc JOIN CalculosPrecos cp ON cp.id = gc.id "
            "WHERE TRIM(COALESCE(gc.status_workflow,'')) = 'Proposta Aceita'"
        )
        if perfil != 'Admin' and grupo_id:
            sql += " AND gc.id IN (SELECT credito_id FROM GruposCreditos WHERE grupo_id = ?)"
            params.append(int(grupo_id))
        sql += " AND gc.id NOT IN (SELECT credito_id FROM Operacoes WHERE credito_id IS NOT NULL)"
        df = pd.read_sql_query(sql, con, params=params)
        created_ops = []
        for _, row in df.iterrows():
            cred_id = int(row['credito_id'])
            num_proc = (row['numero_processo'] or '').strip() or None
            doc = (row['cpf_credor'] or '').strip()
            doc_digits = ''.join([c for c in doc if c.isdigit()])
            tipo_pessoa = 'PJ' if len(doc_digits) > 11 else 'PF'
            cur = con.cursor()
            cur.execute(
                "INSERT INTO Operacoes (credito_id, grupo_id, operador_username, tipo_pessoa, tipo_credito, numero_processo, status_atual, criado_em) VALUES (?, ?, ?, ?, ?, ?, 'Aceite', ?)",
                (cred_id, grupo_id, username, tipo_pessoa, "Outro", num_proc, datetime.now().isoformat())
            )
            created_ops.append(cur.lastrowid)
        con.commit()
        registrar_acao(
            con,
            "varredura_proposta_aceita",
            f"Varreu créditos 'Proposta Aceita' e incluiu {len(created_ops)} operação(ões).",
            {
                "nome_usuario": username,
                "pagina_origem": "6_Propostas_Aceitas.py",
                "dados_json": {
                    "grupo_id": grupo_id,
                    "qtd_creditos_encontrados": int(len(df)),
                    "qtd_operacoes_criadas": int(len(created_ops)),
                    "operacoes_criadas_ids": created_ops
                }
            }
        )
        return {"found": int(len(df)), "created": int(len(created_ops)), "ops_ids": created_ops}
    except Exception as e:
        registrar_acao(con, "erro_varredura_proposta_aceita", f"Erro na varredura: {e}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py"})
        return {"found": 0, "created": 0, "ops_ids": []}

# Executa a migração e varredura automáticas
st.toast("Migrando créditos em documentação para proposta aceita...")
qtd_migrados = migrar_em_documentacao_para_proposta_aceita(con, grupo_id, username, perfil)
st.toast("Migração concluída!")
st.toast("Varrendo créditos com status 'Proposta Aceita'...")
varrer_creditos_em_documentacao(con, grupo_id, username, perfil)
st.toast("Varredura concluída!")

def check_item(con, operacao_id, codigo):
    if codigo == "ACEITE_DATA_HORA":
        df = pd.read_sql_query("SELECT data_hora_aceite FROM AceitesOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "ACEITE_CREDOR_NOME":
        df = pd.read_sql_query("SELECT credor_nome FROM AceitesOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "ACEITE_VALOR":
        df = pd.read_sql_query("SELECT valor_compra FROM AceitesOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        try:
            return not df.empty and float(df.iloc[0][0] or 0) > 0
        except Exception:
            return False
    if codigo == "ACEITE_FORMA_PAGAMENTO":
        df = pd.read_sql_query("SELECT forma_pagamento FROM AceitesOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "ACEITE_TIPO_CREDITO":
        df = pd.read_sql_query("SELECT tipo_credito FROM Operacoes WHERE id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "ACEITE_NUMERO_PROCESSO":
        df = pd.read_sql_query("SELECT numero_processo FROM Operacoes WHERE id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "ACEITE_OPERADOR":
        df = pd.read_sql_query("SELECT operador_username FROM Operacoes WHERE id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    # Documentos (status precisa ser 'Validado')
    docmap = {
        "PF_RG_CNH_CPF": ("PF", "RG/CNH/CPF"),
        "PF_COMPROVANTE_ENDERECO": ("PF", "Comprovante de endereço"),
        "PF_ESTADO_CIVIL_REGIME": ("PF", "Estado civil/Regime de bens"),
        "PF_ANUENCIA_CONJUGE": ("PF", "Anuência do cônjuge"),
        "PF_DADOS_BANCARIOS": ("PF", "Dados bancários"),
        "PJ_CONTRATO_ESTATUTO": ("PJ", "Contrato/Estatuto + última alteração"),
        "PJ_DOC_REPRESENTANTE": ("PJ", "Documento(s) do representante"),
        "PJ_PODERES": ("PJ", "Procuração/Ata de poderes"),
        "PJ_COMPROVANTE_ENDERECO": ("PJ", "Comprovante de endereço"),
        "PJ_DADOS_BANCARIOS": ("PJ", "Dados bancários"),
        "CR_NUMERO_PROCESSO_TRIBUNAL": ("CREDITO", "Número do processo e tribunal"),
        "CR_PECAS_ESSENCIAIS": ("CREDITO", "Peças essenciais (sentença/acórdão/etc.)"),
        "CR_COMPROVANTE_PRECAT_RPV": ("CREDITO", "Comprovante de precatório/RPV")
    }
    if codigo in docmap:
        cat, nome = docmap[codigo]
        df = pd.read_sql_query(
            "SELECT status FROM DocumentosOperacao WHERE operacao_id = ? AND categoria = ? AND item_nome = ?",
            con, params=(operacao_id, cat, nome)
        )
        return not df.empty and df.iloc[0][0] == "Validado"
    if codigo in ["CON_TIPO_CONTRATO", "CON_CONDICAO_PAGAMENTO", "CON_PRAZOS"]:
        col = {"CON_TIPO_CONTRATO": "tipo_contrato", "CON_CONDICAO_PAGAMENTO": "condicao_pagamento", "CON_PRAZOS": "prazos"}[codigo]
        df = pd.read_sql_query(f"SELECT {col} FROM ContratosOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "RED_PRONTO":
        df = pd.read_sql_query("SELECT status_redacao FROM RedacoesOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip().lower() == "pronto"
    if codigo in ["DD_TIPO", "DD_RESULTADO"]:
        col = {"DD_TIPO": "tipo", "DD_RESULTADO": "resultado"}[codigo]
        df = pd.read_sql_query(f"SELECT {col} FROM DueDiligencias WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "ASS_ASSINADO":
        df = pd.read_sql_query("SELECT status_assinatura FROM AssinaturasOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip().lower() == "assinado"
    if codigo in ["HOM_PETICAO", "HOM_HOMOLOGADO"]:
        col = {"HOM_PETICAO": "data_peticao", "HOM_HOMOLOGADO": "status_homologacao"}[codigo]
        df = pd.read_sql_query(f"SELECT {col} FROM HomologacoesOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    if codigo == "PAG_REALIZADO":
        df = pd.read_sql_query("SELECT valor_pago FROM PagamentosOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        try:
            return not df.empty and float(df.iloc[0][0] or 0) > 0
        except Exception:
            return False
    if codigo == "ENC_ENCERRADO":
        df = pd.read_sql_query("SELECT status_final FROM EncerramentosOperacao WHERE operacao_id = ?", con, params=(operacao_id,))
        return not df.empty and str(df.iloc[0][0] or "").strip() != ""
    return True

def verificar_requisitos(con, operacao_id, etapa):
    cfg = get_config(con)
    pend = []
    reqs = cfg[(cfg["etapa"] == etapa) & (cfg["obrigatorio"] == 1) & (cfg["ativo"] == 1)]
    for _, row in reqs.iterrows():
        ok = check_item(con, operacao_id, row["item_codigo"])
        if not ok:
            pend.append(row["item_nome"])
    return len(pend) == 0, pend

@st.cache_data(ttl=60)
def listar_operacoes(grupo_id, username):
    # Filtrar somente operações com crédito em status 'Proposta Aceita'
    params = []
    sql = (
        "SELECT o.id, o.credito_id, o.grupo_id, o.operador_username, o.tipo_pessoa, o.tipo_credito, o.numero_processo, o.status_atual, o.criado_em "
        "FROM Operacoes o "
        "LEFT JOIN GestaoCreditos gc ON gc.id = o.credito_id "
    )
    where_clauses = ["TRIM(COALESCE(gc.status_workflow,'')) = 'Proposta Aceita'"]
    if perfil != "Admin":
        where_clauses.append("o.grupo_id = ?")
        where_clauses.append("o.operador_username = ?")
        params = [grupo_id or -1, username or ""]
    else:
        if grupo_id:
            where_clauses.append("o.grupo_id = ?")
            params = [grupo_id]
    sql += " WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY o.id DESC"
    return pd.read_sql_query(sql, con, params=params)

# --- UI: Tabela de Gestão ---
st.header("Gestão de Propostas Aceitas")

# Função para obter dados das operações com informações relevantes
@st.cache_data(ttl=60)
def get_operacoes_detalhadas(grupo_id, username):
    sql = """
    SELECT 
        o.id,
        o.credito_id,
        o.numero_processo,
        o.status_atual,
        o.criado_em,
        a.data_hora_aceite,
        a.credor_nome AS credor_nome_aceite,
        COALESCE(a.credor_nome, cp.requerente, ed.polo_ativo_credor) AS credor_nome,
        a.valor_compra AS valor_compra_aceite,
        COALESCE(a.valor_compra, gc.valor_ultima_proposta) AS valor_compra,
        COALESCE(po.valor_pago_total, 0) AS valor_pago,
        cp.cpf_credor,
        COALESCE(cp.requerido, ed.polo_passivo) AS devedor_nome,
        cp.valor_liquido_final,
        cp.valor_total_condenacao,
        cp.arquivo_de_origem,
        gc.status_workflow,
        gc.data_ultima_atualizacao,
        JULIANDAY('now') - JULIANDAY(o.criado_em) AS dias_em_operacao
    FROM Operacoes o
    LEFT JOIN AceitesOperacao a ON o.id = a.operacao_id
    LEFT JOIN GestaoCreditos gc ON gc.id = o.credito_id
    LEFT JOIN CalculosPrecos cp ON cp.numero_processo = o.numero_processo
    LEFT JOIN (
        SELECT operacao_id, SUM(valor_pago) AS valor_pago_total FROM PagamentosOperacao GROUP BY operacao_id
    ) po ON po.operacao_id = o.id
    LEFT JOIN ExtracaoDiario ed ON ed.numero_processo_cnj = o.numero_processo
    """
    params = []
    where_clauses = []
    if perfil != "Admin":
        where_clauses.append("o.grupo_id = ?")
        where_clauses.append("o.operador_username = ?")
        params = [grupo_id or -1, username or ""]
    else:
        if grupo_id:
            where_clauses.append("o.grupo_id = ?")
            params = [grupo_id]

    # Controlar exclusivamente por status do crédito
    where_clauses.append("gc.status_workflow = 'Proposta Aceita'")

    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY o.criado_em DESC"
    
    df = pd.read_sql_query(sql, con, params=params)

    # Deduplicação por operação (um registro por o.id),
    # priorizando linhas que possuem dados explícitos do aceite
    if not df.empty:
        df['dt_aceite_sort'] = pd.to_datetime(df['data_hora_aceite'], errors='coerce')
        prefer_cols = []
        if 'valor_compra_aceite' in df.columns:
            prefer_cols.append(df['valor_compra_aceite'].notnull())
        if 'credor_nome_aceite' in df.columns:
            prefer_cols.append(df['credor_nome_aceite'].notnull())
        if prefer_cols:
            df['prefer'] = sum(prefer_cols).astype(int)
        else:
            df['prefer'] = 0
        df = df.sort_values(by=['prefer', 'dt_aceite_sort'], ascending=[False, False])
        df = df.drop_duplicates(subset=['id'], keep='first')
        df = df.drop(columns=[c for c in ['dt_aceite_sort', 'valor_compra_aceite', 'credor_nome_aceite'] if c in df.columns])

    # Remover qualquer fallback baseado em logs; usar somente dados das tabelas relacionadas ao crédito/aceite

    # Formatando e enriquecendo colunas
    try:
        df['dias_em_operacao'] = df['dias_em_operacao'].round(0).astype(int)
    except Exception:
        df['dias_em_operacao'] = 0
    for col_val in ['valor_compra', 'valor_pago', 'valor_liquido_final', 'valor_total_condenacao']:
        if col_val in df.columns:
            df[col_val] = df[col_val].fillna(0).apply(lambda x: f"R$ {x:,.2f}")
    if 'data_hora_aceite' in df.columns:
        df['data_hora_aceite'] = pd.to_datetime(df['data_hora_aceite'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    df['criado_em'] = pd.to_datetime(df['criado_em'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')
    if 'data_ultima_atualizacao' in df.columns:
        df['data_ultima_atualizacao'] = pd.to_datetime(df['data_ultima_atualizacao'], errors='coerce').dt.strftime('%d/%m/%Y %H:%M')

    # Renomeando colunas para melhor visualização
    df = df.rename(columns={
        'id': 'ID',
        'credito_id': 'ID Crédito',
        'numero_processo': 'Processo',
        'status_atual': 'Status',
        'criado_em': 'Data Criação',
        'data_hora_aceite': 'Data Aceite',
        'aceite_por': 'Aceite por',
        'credor_nome': 'Credor',
        'devedor_nome': 'Devedor',
        'cpf_credor': 'CPF/CNPJ',
        'valor_compra': 'Valor Compra',
        'valor_pago': 'Valor Pago',
        'valor_liquido_final': 'Valor Líquido',
        'valor_total_condenacao': 'Valor Total',
        'arquivo_de_origem': 'Arquivo Origem',
        'status_workflow': 'Workflow',
        'data_ultima_atualizacao': 'Última Atualização',
        'dias_em_operacao': 'Dias em Operação'
    })
    
    return df

# Obtendo e exibindo a tabela de gestão
df_gestao = get_operacoes_detalhadas(grupo_id, username)

# Filtros
col1, col2, col3 = st.columns(3)
with col1:
    filtro_status = st.multiselect(
        "Filtrar por Status",
        options=sorted(df_gestao['Status'].unique()),
        default=[]
    )
with col2:
    filtro_dias = st.slider(
        "Filtrar por Dias em Operação",
        min_value=0,
        max_value=max(df_gestao['Dias em Operação']),
        value=(0, max(df_gestao['Dias em Operação']))
    )
with col3:
    busca = st.text_input("Buscar por Processo, Credor ou Devedor", "")

# Aplicando filtros
df_filtrado = df_gestao.copy()
if filtro_status:
    df_filtrado = df_filtrado[df_filtrado['Status'].isin(filtro_status)]
df_filtrado = df_filtrado[
    (df_filtrado['Dias em Operação'] >= filtro_dias[0]) &
    (df_filtrado['Dias em Operação'] <= filtro_dias[1])
]
if busca:
    mask = (
        df_filtrado['Processo'].str.contains(busca, case=False, na=False) |
        df_filtrado['Credor'].str.contains(busca, case=False, na=False) |
        (df_filtrado['Devedor'].str.contains(busca, case=False, na=False) if 'Devedor' in df_filtrado.columns else False)
    )
    df_filtrado = df_filtrado[mask]

# Exibindo estatísticas
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total de Operações", len(df_filtrado))
with col2:
    media_dias = df_filtrado['Dias em Operação'].mean()
    st.metric("Média de Dias", f"{media_dias:.1f}")
with col3:
    ops_atrasadas = len(df_filtrado[df_filtrado['Dias em Operação'] > 30])
    st.metric("Operações > 30 dias", ops_atrasadas)
with col4:
    status_counts = df_filtrado['Status'].value_counts()
    status_mais_comum = status_counts.index[0] if not status_counts.empty else "N/A"
    st.metric("Status Mais Comum", status_mais_comum)

# Exibindo a tabela com dados filtrados
st.dataframe(
    df_filtrado,
    use_container_width=True,
    hide_index=True,
    column_config={
        'ID': st.column_config.NumberColumn(format="%d"),
        'ID Crédito': st.column_config.NumberColumn(format="%d"),
        'Dias em Operação': st.column_config.NumberColumn(
            format="%d",
            help="Dias desde a criação da operação"
        )
    }
)

st.divider()

# --- UI: criar/selecionar operação ---
colA, colB = st.columns([1,2])
with colA:
    st.subheader("Criar operação")
    with st.form("form_criar_op"):
        credito_id = st.text_input("ID do crédito (se conhecido)")
        numero_processo = st.text_input("Número do processo (CNJ)")
        tipo_pessoa = st.selectbox("Tipo de pessoa", ["PF", "PJ"])
        tipo_credito = st.selectbox("Tipo de crédito", ["RPV", "Precatório", "Outro"])
        submit = st.form_submit_button("Criar operação")
        if submit:
            con.execute(
                "INSERT INTO Operacoes (credito_id, grupo_id, operador_username, tipo_pessoa, tipo_credito, numero_processo, status_atual, criado_em) VALUES (?, ?, ?, ?, ?, ?, 'Aceite', ?)",
                (
                    int(credito_id) if credito_id.strip().isdigit() else None,
                    grupo_id,
                    username,
                    tipo_pessoa,
                    tipo_credito,
                    numero_processo.strip() or None,
                    datetime.now().isoformat()
                )
            )
            con.commit()
            st.success("Operação criada.")
            st.cache_data.clear()
            registrar_acao(con, "criar_operacao", f"Operador {username}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py"})

with colB:
    st.subheader("Operações")
    dfops = listar_operacoes(grupo_id, username)
    if dfops.empty:
        st.info("Nenhuma operação encontrada para seu contexto.")
    else:
        op_sel = st.selectbox("Selecione a operação", options=dfops["id"].tolist(), format_func=lambda x: f"Op {x} – {dfops.set_index('id').loc[x, 'status_atual']} – Proc {dfops.set_index('id').loc[x, 'numero_processo']}")
        st.markdown("Status atual: **{}**".format(dfops.set_index("id").loc[op_sel, "status_atual"]))
        try:
            cid_sel = dfops.set_index('id').loc[op_sel, 'credito_id']
        except Exception:
            cid_sel = None

        aba1, aba2, aba3, aba4, aba5, aba6, aba7, aba8 = st.tabs([
            "Aceite", "Documentos", "Contrato", "Redação", "Due Diligence", "Assinatura", "Homologação", "Pagamento"
        ])

        # --- Aceite ---
        with aba1:
            df = pd.read_sql_query("SELECT forma_pagamento, observacoes FROM AceitesOperacao WHERE operacao_id = ?", con, params=(op_sel,))
            prefill = get_aceite_prefill(con, op_sel)
            data_hora_prefill = prefill.get("data_hora") or ""
            credor_prefill = prefill.get("credor") or ""
            valor_prefill = prefill.get("valor")
            valor_display = f"R$ {float(valor_prefill):,.2f}" if valor_prefill is not None else ""
            aceite_por = prefill.get("aceite_por") or ""

            st.text_input("Data/Hora do aceite", value=str(data_hora_prefill), disabled=True)
            st.text_input("Aceite por", value=aceite_por, disabled=True)
            st.text_input("Nome do credor", value=credor_prefill, disabled=True)
            st.text_input("Valor de compra", value=valor_display, disabled=True)

            forma = st.text_input("Forma de pagamento", value=(df.iloc[0]["forma_pagamento"] if not df.empty else ""))
            obs = st.text_area("Observações", value=(df.iloc[0]["observacoes"] if not df.empty else ""))
            if st.button("Salvar aceite"):
                con.execute(
                    "INSERT OR REPLACE INTO AceitesOperacao (operacao_id, data_hora_aceite, credor_nome, valor_compra, forma_pagamento, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
                    (op_sel, data_hora_prefill, credor_prefill, float(valor_prefill or 0), forma, obs)
                )
                con.commit()
                try:
                    st.cache_data.clear()
                except Exception:
                    pass
                st.success("Aceite salvo.")
                registrar_acao(
                    con,
                    "salvar_aceite",
                    f"Op {op_sel}",
                    {
                        "nome_usuario": username,
                        "pagina_origem": "6_Propostas_Aceitas.py",
                        "id_credito": cid_sel,
                        "dados_json": {
                            "operacao_id": op_sel,
                            "data_hora_aceite": data_hora_prefill,
                            "credor_nome": credor_prefill,
                            "valor_compra": float(valor_prefill or 0),
                            "forma_pagamento": forma,
                            "observacoes": obs,
                        },
                    },
                )

            colx, coly = st.columns(2)
            with colx:
                if st.button("Avançar para Documentos", type="primary"):
                    ok, pend = verificar_requisitos(con, op_sel, "Aceite")
                    if perfil != "Admin" and not ok:
                        st.error("Pendências obrigatórias: " + ", ".join(pend))
                        registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Aceite", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Aceite", "etapa_destino": "Em Documentação", "pendencias": pend}})
                    else:
                        con.execute("UPDATE Operacoes SET status_atual = 'Em Documentação' WHERE id = ?", (op_sel,))
                        con.commit()
                        st.success("Etapa avançada para Em Documentação.")
                        registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "de": "Aceite", "para": "Em Documentação"}})

        # --- Documentos ---
        with aba2:
            st.markdown("Checklist de documentos")
            tipo_pessoa = pd.read_sql_query("SELECT tipo_pessoa FROM Operacoes WHERE id = ?", con, params=(op_sel,)).iloc[0][0]
            if st.button("Gerar checklist padrão"):
                items = []
                if tipo_pessoa == "PF":
                    items += [
                        ("PF", "RG/CNH/CPF"), ("PF", "Comprovante de endereço"), ("PF", "Estado civil/Regime de bens"), ("PF", "Anuência do cônjuge"), ("PF", "Dados bancários")
                    ]
                else:
                    items += [
                        ("PJ", "Contrato/Estatuto + última alteração"), ("PJ", "Documento(s) do representante"), ("PJ", "Procuração/Ata de poderes"), ("PJ", "Comprovante de endereço"), ("PJ", "Dados bancários")
                    ]
                items += [
                    ("CREDITO", "Número do processo e tribunal"), ("CREDITO", "Peças essenciais (sentença/acórdão/etc.)"), ("CREDITO", "Comprovante de precatório/RPV")
                ]
                items_count = len(items)
                for cat, nome in items:
                    con.execute("INSERT OR IGNORE INTO DocumentosOperacao (operacao_id, categoria, item_codigo, item_nome, status) VALUES (?, ?, ?, ?, 'Pendente')", (op_sel, cat, f"AUTO_{cat}_{nome}", nome))
                con.commit()
                st.success("Checklist gerado.")
                registrar_acao(con, "gerar_checklist_documentos", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "tipo_pessoa": tipo_pessoa, "items_count": items_count}})
            dfchk = pd.read_sql_query("SELECT id, categoria, item_nome, status, obs FROM DocumentosOperacao WHERE operacao_id = ? ORDER BY categoria, id", con, params=(op_sel,))
            if dfchk.empty:
                st.info("Gere a checklist padrão ou adicione itens manualmente.")
            else:
                edited = st.data_editor(dfchk, use_container_width=True, num_rows="dynamic")
                if st.button("Salvar alterações de documentos"):
                    orig_by_id = {int(r["id"]): {"status": r["status"], "obs": r["obs"]} for _, r in dfchk.iterrows()}
                    changes = []
                    for _, r in edited.iterrows():
                        con.execute("UPDATE DocumentosOperacao SET status = ?, obs = ? WHERE id = ?", (r["status"], r["obs"], int(r["id"])) )
                        oid = int(r["id"])
                        ch = {}
                        if str(r["status"]) != str(orig_by_id.get(oid, {}).get("status")):
                            ch["status"] = {"old": orig_by_id.get(oid, {}).get("status"), "new": r["status"]}
                        if str(r["obs"]) != str(orig_by_id.get(oid, {}).get("obs")):
                            ch["obs"] = {"old": orig_by_id.get(oid, {}).get("obs"), "new": r["obs"]}
                        if ch:
                            ch["id"] = oid
                            ch["item_nome"] = r["item_nome"]
                            ch["categoria"] = r["categoria"]
                            changes.append(ch)
                    con.commit()
                    st.success("Checklist atualizada.")
                    registrar_acao(con, "salvar_documentos", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "n_changes": len(changes), "changes": changes}})
            if st.button("Avançar para Contrato", type="primary"):
                ok, pend = verificar_requisitos(con, op_sel, "Documentos PF" if tipo_pessoa == "PF" else "Documentos PJ")
                ok2, pend2 = verificar_requisitos(con, op_sel, "Documentos Crédito")
                all_ok = ok and ok2
                if perfil != "Admin" and not all_ok:
                    st.error("Pendências obrigatórias: " + ", ".join(pend + pend2))
                    registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Documentos", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "tipo_pessoa": tipo_pessoa, "etapa_origem": "Documentos", "etapa_destino": "Contrato", "pendencias": pend + pend2}})
                else:
                    con.execute("UPDATE Operacoes SET status_atual = 'Contrato' WHERE id = ?", (op_sel,))
                    con.commit()
                    st.success("Etapa avançada para Contrato.")
                    registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "de": "Documentos", "para": "Contrato"}})

        # --- Contrato ---
        with aba3:
            dfc = pd.read_sql_query("SELECT * FROM ContratosOperacao WHERE operacao_id = ?", con, params=(op_sel,))
            tipo_ctr = st.text_input("Tipo de contrato", value=(dfc.iloc[0]["tipo_contrato"] if not dfc.empty else ""))
            cond = st.text_input("Condição de pagamento", value=(dfc.iloc[0]["condicao_pagamento"] if not dfc.empty else ""))
            praz = st.text_input("Prazos principais", value=(dfc.iloc[0]["prazos"] if not dfc.empty else ""))
            if st.button("Salvar contrato"):
                con.execute("INSERT OR REPLACE INTO ContratosOperacao (operacao_id, tipo_contrato, condicao_pagamento, prazos) VALUES (?, ?, ?, ?)", (op_sel, tipo_ctr, cond, praz))
                con.commit()
                st.success("Contrato salvo.")
                registrar_acao(con, "salvar_contrato", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "tipo_contrato": tipo_ctr, "condicao_pagamento": cond, "prazos": praz}})
            if st.button("Avançar para Redação", type="primary"):
                ok, pend = verificar_requisitos(con, op_sel, "Contrato")
                if perfil != "Admin" and not ok:
                    st.error("Pendências obrigatórias: " + ", ".join(pend))
                    registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Contrato", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Contrato", "etapa_destino": "Redação", "pendencias": pend}})
                else:
                    con.execute("UPDATE Operacoes SET status_atual = 'Redação' WHERE id = ?", (op_sel,))
                    con.commit()
                    st.success("Etapa avançada para Redação.")
                    registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "de": "Contrato", "para": "Redação"}})

        # --- Redação ---
        with aba4:
            dfr = pd.read_sql_query("SELECT * FROM RedacoesOperacao WHERE operacao_id = ?", con, params=(op_sel,))
            status_r = st.selectbox("Status da redação", ["Rascunho", "Em Revisão", "Pronto"], index=( ["Rascunho","Em Revisão","Pronto"].index(dfr.iloc[0]["status_redacao"]) if not dfr.empty and dfr.iloc[0]["status_redacao"] in ["Rascunho","Em Revisão","Pronto"] else 0 ))
            link = st.text_input("Link/arquivo (opcional)", value=(dfr.iloc[0]["arquivo_link"] if not dfr.empty else ""))
            if st.button("Salvar redação"):
                con.execute("INSERT OR REPLACE INTO RedacoesOperacao (operacao_id, status_redacao, arquivo_link) VALUES (?, ?, ?)", (op_sel, status_r, link))
                con.commit()
                st.success("Redação salva.")
                registrar_acao(con, "salvar_redacao", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "status_redacao": status_r, "arquivo_link": link}})
            if st.button("Avançar para Due Diligence", type="primary"):
                ok, pend = verificar_requisitos(con, op_sel, "Redação")
                if perfil != "Admin" and not ok:
                    st.error("Pendências obrigatórias: " + ", ".join(pend))
                    registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Redação", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Redação", "etapa_destino": "Due Diligence", "pendencias": pend}})
                else:
                    con.execute("UPDATE Operacoes SET status_atual = 'Due Diligence' WHERE id = ?", (op_sel,))
                    con.commit()
                    st.success("Etapa avançada para Due Diligence.")
                    registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Due Diligence", "etapa_destino": "Due Diligence", "pendencias": pend}})

        # --- Due Diligence ---
        with aba5:
            dfd = pd.read_sql_query("SELECT * FROM DueDiligencias WHERE operacao_id = ?", con, params=(op_sel,))
            tipo = st.selectbox("Tipo", ["Simplificada", "Completa"], index=( ["Simplificada","Completa"].index(dfd.iloc[0]["tipo"]) if not dfd.empty and dfd.iloc[0]["tipo"] in ["Simplificada","Completa"] else 0 ))
            resultado = st.text_input("Resultado/Parecer", value=(dfd.iloc[0]["resultado"] if not dfd.empty else ""))
            parecer = st.text_area("Parecer detalhado", value=(dfd.iloc[0]["parecer"] if not dfd.empty else ""))
            if st.button("Salvar due diligence"):
                con.execute("INSERT OR REPLACE INTO DueDiligencias (operacao_id, tipo, resultado, parecer) VALUES (?, ?, ?, ?)", (op_sel, tipo, resultado, parecer))
                con.commit()
                st.success("Due diligence salva.")
                registrar_acao(con, "salvar_diligencia", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "tipo": tipo, "resultado_len": len(resultado or ""), "parecer_len": len(parecer or "")}})
            if st.button("Avançar para Assinatura", type="primary"):
                ok, pend = verificar_requisitos(con, op_sel, "Due Diligence")
                if perfil != "Admin" and not ok:
                    st.error("Pendências obrigatórias: " + ", ".join(pend))
                    registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Due Diligence", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Due Diligence", "etapa_destino": "Assinatura", "pendencias": pend}})
                else:
                    con.execute("UPDATE Operacoes SET status_atual = 'Assinatura' WHERE id = ?", (op_sel,))
                    con.commit()
                    st.success("Etapa avançada para Assinatura.")
                    registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Due Diligence", "etapa_destino": "Assinatura", "pendencias": pend}})

        # --- Assinatura ---
        with aba6:
            dfa = pd.read_sql_query("SELECT * FROM AssinaturasOperacao WHERE operacao_id = ?", con, params=(op_sel,))
            status_a = st.selectbox("Status da assinatura", ["Pendente", "Em Coleta", "Assinado"], index=( ["Pendente","Em Coleta","Assinado"].index(dfa.iloc[0]["status_assinatura"]) if not dfa.empty and dfa.iloc[0]["status_assinatura"] in ["Pendente","Em Coleta","Assinado"] else 0 ))
            data_a = st.text_input("Data de assinatura", value=(dfa.iloc[0]["data_assinatura"] if not dfa.empty else ""))
            assinantes = st.text_area("Assinantes (nomes)", value=(dfa.iloc[0]["assinantes"] if not dfa.empty else ""))
            if st.button("Salvar assinatura"):
                con.execute("INSERT OR REPLACE INTO AssinaturasOperacao (operacao_id, status_assinatura, data_assinatura, assinantes) VALUES (?, ?, ?, ?)", (op_sel, status_a, data_a, assinantes))
                con.commit()
                st.success("Assinatura salva.")
                registrar_acao(con, "salvar_assinatura", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "status_assinatura": status_a, "data_assinatura": data_a, "assinantes_len": len(assinantes or "")}})
            if st.button("Avançar para Homologação", type="primary"):
                ok, pend = verificar_requisitos(con, op_sel, "Assinatura")
                if perfil != "Admin" and not ok:
                    st.error("Pendências obrigatórias: " + ", ".join(pend))
                    registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Assinatura", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Assinatura", "etapa_destino": "Homologação", "pendencias": pend}})
                else:
                    con.execute("UPDATE Operacoes SET status_atual = 'Homologação' WHERE id = ?", (op_sel,))
                    con.commit()
                    st.success("Etapa avançada para Homologação.")
                    registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "de": "Assinatura", "para": "Homologação"}})

        # --- Homologação ---
        with aba7:
            dfh = pd.read_sql_query("SELECT * FROM HomologacoesOperacao WHERE operacao_id = ?", con, params=(op_sel,))
            data_p = st.text_input("Data da petição", value=(dfh.iloc[0]["data_peticao"] if not dfh.empty else ""))
            status_h = st.text_input("Status da homologação", value=(dfh.iloc[0]["status_homologacao"] if not dfh.empty else ""))
            data_h = st.text_input("Data da homologação", value=(dfh.iloc[0]["data_homologacao"] if not dfh.empty else ""))
            if st.button("Salvar homologação"):
                con.execute("INSERT OR REPLACE INTO HomologacoesOperacao (operacao_id, data_peticao, status_homologacao, data_homologacao) VALUES (?, ?, ?, ?)", (op_sel, data_p, status_h, data_h))
                con.commit()
                st.success("Homologação salva.")
                registrar_acao(con, "salvar_homologacao", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "data_peticao": data_p, "status_homologacao": status_h, "data_homologacao": data_h}})
            if st.button("Avançar para Pagamento", type="primary"):
                ok, pend = verificar_requisitos(con, op_sel, "Homologação")
                if perfil != "Admin" and not ok:
                    st.error("Pendências obrigatórias: " + ", ".join(pend))
                    registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Homologação", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Homologação", "etapa_destino": "Pagamento", "pendencias": pend}})
                else:
                    con.execute("UPDATE Operacoes SET status_atual = 'Pagamento' WHERE id = ?", (op_sel,))
                    con.commit()
                    st.success("Etapa avançada para Pagamento.")
                    registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "de": "Homologação", "para": "Pagamento"}})

        # --- Pagamento ---
        with aba8:
            dfp = pd.read_sql_query("SELECT * FROM PagamentosOperacao WHERE operacao_id = ?", con, params=(op_sel,))
            data_pg = st.text_input("Data do pagamento", value=(dfp.iloc[0]["data_pagamento"] if not dfp.empty else ""))
            valor_pg = st.text_input("Valor pago", value=(str(dfp.iloc[0]["valor_pago"]) if not dfp.empty and dfp.iloc[0]["valor_pago"] else ""))
            metodo = st.text_input("Método de pagamento", value=(dfp.iloc[0]["metodo"] if not dfp.empty else ""))
            if st.button("Salvar pagamento"):
                con.execute("INSERT OR REPLACE INTO PagamentosOperacao (operacao_id, data_pagamento, valor_pago, metodo) VALUES (?, ?, ?, ?)", (op_sel, data_pg, float(valor_pg or 0), metodo))
                con.commit()
                st.success("Pagamento salvo.")
                registrar_acao(con, "salvar_pagamento", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "data_pagamento": data_pg, "valor_pago": float(valor_pg or 0), "metodo": metodo}})
            colp, colq = st.columns(2)
            with colp:
                if st.button("Avançar para Encerramento", type="primary"):
                    ok, pend = verificar_requisitos(con, op_sel, "Pagamento")
                    if perfil != "Admin" and not ok:
                        st.error("Pendências obrigatórias: " + ", ".join(pend))
                        registrar_acao(con, "bloqueio_avanco_etapa", f"Op {op_sel} – Pagamento", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "etapa_origem": "Pagamento", "etapa_destino": "Encerrado", "pendencias": pend}})
                    else:
                        con.execute("UPDATE Operacoes SET status_atual = 'Encerrado' WHERE id = ?", (op_sel,))
                        con.commit()
                        st.success("Etapa avançada para Encerrado.")
                        registrar_acao(con, "avancar_etapa", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "de": "Pagamento", "para": "Encerrado"}})
            with colq:
                dfe = pd.read_sql_query("SELECT * FROM EncerramentosOperacao WHERE operacao_id = ?", con, params=(op_sel,))
                status_f = st.text_input("Status final", value=(dfe.iloc[0]["status_final"] if not dfe.empty else ""))
                data_e = st.text_input("Data de encerramento", value=(dfe.iloc[0]["data_encerramento"] if not dfe.empty else ""))
                resumo = st.text_area("Resumo da operação", value=(dfe.iloc[0]["resumo"] if not dfe.empty else ""))
                if st.button("Salvar encerramento"):
                    con.execute("INSERT OR REPLACE INTO EncerramentosOperacao (operacao_id, status_final, data_encerramento, resumo) VALUES (?, ?, ?, ?)", (op_sel, status_f, data_e, resumo))
                    con.commit()
                    st.success("Encerramento salvo.")
                    registrar_acao(con, "salvar_encerramento", f"Op {op_sel}", {"nome_usuario": username, "pagina_origem": "6_Propostas_Aceitas.py", "id_credito": cid_sel, "dados_json": {"operacao_id": op_sel, "status_final": status_f, "data_encerramento": data_e, "resumo_len": len(resumo or "")}})