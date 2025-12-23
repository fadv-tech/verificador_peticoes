# ==============================================================================
# modules/db.py (v37.0 - VERSÃO DA RESTAURAÇÃO COMPLETA E SEM ATALHOS)
#
# OBJETIVO:
# - É o código COMPLETO, sem "..." ou omissões.
# - RESTAURA o db.py original que funcionava com TODAS as páginas.
# - ADICIONA as funções 'atualizar_proposta_credito' e a nova 'buscar_creditos_por_credor'.
# - MOVE 'registrar_acao' de volta para cá, consertando o ImportError em todo o sistema.
# ==============================================================================
import sqlite3
import pandas as pd
from datetime import datetime
import json

# --- FUNÇÃO DE CONEXÃO ORIGINAL ---
def conectar_db():
    """A função original que conecta ao banco principal."""
    conexao = sqlite3.connect('precatorios_estrategico.db', check_same_thread=False)
    try:
        # Garantir que a tabela de usuários exista para evitar erros de login/gestão
        garantir_esquema_usuarios(conexao)
        # Garantir esquema de grupos sem impactar funcionalidades existentes
        garantir_esquema_grupos(conexao)
    except Exception as e:
        # Falhas de migração não devem quebrar conexão
        print(f"Aviso: falha ao garantir esquema de grupos: {e}")
    return conexao

# --- FUNÇÃO DE LOG RESTAURADA ---
def registrar_acao(conexao, tipo_acao, detalhes_humanos, dados_log={}):
    """Grava uma ação na tabela de histórico. VOLTOU PARA CÁ PARA CONSERTAR TUDO."""
    try:
        cursor = conexao.cursor()
        query = "INSERT INTO HistoricoAcoes (timestamp, nome_usuario, tipo_acao, detalhes_humanos, pagina_origem, chave_agrupamento_credor, id_credito, dados_alterados_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        params = (
            datetime.now(),
            dados_log.get('nome_usuario', 'N/A'),
            tipo_acao,
            detalhes_humanos,
            dados_log.get('pagina_origem'),
            dados_log.get('chave_agrupamento_credor'),
            dados_log.get('id_credito'),
            json.dumps(dados_log.get('dados_json')) if dados_log.get('dados_json') else None
        )
        cursor.execute(query, params)
        conexao.commit()
        return True
    except Exception as e:
        print(f"ERRO AO REGISTRAR AÇÃO (v2): {e}")
        try:
            cursor.execute("INSERT INTO HistoricoAcoes (timestamp, nome_usuario, tipo_acao, detalhes) VALUES (?, ?, ?, ?)",
                           (datetime.now(), dados_log.get('nome_usuario', 'N/A'), tipo_acao, detalhes_humanos))
            conexao.commit()
        except Exception as e2:
            print(f"ERRO no fallback de log (v1): {e2}")
        return False

# --- FUNÇÕES DE LEITURA (QUERIES) ---

def buscar_credores_consolidados(conexao):
    query = "SELECT COALESCE(NULLIF(TRIM(cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(requerente))) as chave_agrupamento, MIN(requerente) as nome_principal, MIN(cpf_credor) as documento, COUNT(id) as qtd_creditos, SUM(valor_liquido_final) as valor_total, GROUP_CONCAT(DISTINCT requerido_1) as devedores FROM CalculosPrecos WHERE requerente IS NOT NULL AND requerente != '' GROUP BY chave_agrupamento;"
    df = pd.read_sql_query(query, conexao)
    try:
        df_status = pd.read_sql_query("SELECT chave_agrupamento, status_relacionamento FROM GestaoCredores", conexao)
        df = pd.merge(df, df_status, on='chave_agrupamento', how='left')
        df['status_relacionamento'].fillna('Não Contatado', inplace=True)
    except pd.errors.DatabaseError:
        df['status_relacionamento'] = 'Não Contatado (Erro DB)'
    return df

def buscar_dados_completos_credor(conexao, chave_agrupamento):
    query_principal = "SELECT MIN(requerente) as nome_principal, COUNT(id) as qtd_creditos, SUM(valor_liquido_final) as valor_total FROM CalculosPrecos WHERE COALESCE(NULLIF(TRIM(cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(requerente))) = ?"
    df_principal = pd.read_sql_query(query_principal, conexao, params=(chave_agrupamento,))
    dados_credor = df_principal.iloc[0].to_dict() if not df_principal.empty else {}
    try:
        cursor = conexao.cursor()
        cursor.execute("SELECT status_relacionamento, telefone, email, anotacoes_gerais FROM GestaoCredores WHERE chave_agrupamento = ?", (chave_agrupamento,))
        resultado_gestao = cursor.fetchone()
        if resultado_gestao:
            dados_credor.update({'status_relacionamento': resultado_gestao[0] or "Não Contatado", 'telefone': resultado_gestao[1] or "", 'email': resultado_gestao[2] or "", 'anotacoes_gerais': resultado_gestao[3] or ""})
        else:
            dados_credor.setdefault('status_relacionamento', "Não Contatado")
    except sqlite3.OperationalError:
        dados_credor.setdefault('status_relacionamento', "Não Contatado (Erro DB)")
    return dados_credor

def buscar_dossie_completo(conexao, credito_id):
    if not credito_id or pd.isna(credito_id): return None
    query = "SELECT cp.*, gc.status_workflow, gc.rascunho_anotacao, d.desagio_min_percentual, d.desagio_max_percentual, (SELECT valor FROM Configuracoes WHERE chave = 'desagio_min_padrao') as desagio_min_padrao, (SELECT valor FROM Configuracoes WHERE chave = 'desagio_max_padrao') as desagio_max_padrao FROM CalculosPrecos cp LEFT JOIN GestaoCreditos gc ON cp.id = gc.id LEFT JOIN Devedores d ON UPPER(cp.requerido_1) = d.nome_devedor WHERE cp.id = ?;"
    df = pd.read_sql_query(query, conexao, params=(int(credito_id),))
    if not df.empty:
        dossie = df.iloc[0].to_dict()
        valor_liquido = dossie.get('valor_liquido_final', 0); desagio_min = dossie.get('desagio_min_percentual') or dossie.get('desagio_min_padrao'); desagio_max = dossie.get('desagio_max_percentual') or dossie.get('desagio_max_padrao')
        if valor_liquido and desagio_min and desagio_max:
            dossie['valor_min_compra'] = valor_liquido * (1 - (float(desagio_max) / 100.0)); dossie['valor_max_compra'] = valor_liquido * (1 - (float(desagio_min) / 100.0))
        else:
            dossie['valor_min_compra'] = None; dossie['valor_max_compra'] = None
        return dossie
    return None

def buscar_usuario(conexao, nome_usuario):
    cursor = conexao.cursor(); cursor.execute("SELECT id, nome_usuario, perfil, senha_hash, senha_salt FROM Usuarios WHERE nome_usuario = ?", (nome_usuario,)); return cursor.fetchone()

def buscar_regras_desagio_padrao(conexao):
    regras = {}; df = pd.read_sql_query("SELECT chave, valor FROM Configuracoes WHERE chave IN ('desagio_min_padrao', 'desagio_max_padrao')", conexao)
    for _, row in df.iterrows(): regras[row['chave']] = float(row['valor'])
    return regras if regras else {'desagio_min_padrao': 20.0, 'desagio_max_padrao': 50.0}

def buscar_devedores(conexao):
    return pd.read_sql_query("SELECT nome_devedor, desagio_min_percentual, desagio_max_percentual FROM Devedores ORDER BY nome_devedor", conexao)

def buscar_nomes_devedores_unicos(conexao):
    df = pd.read_sql_query("SELECT DISTINCT requerido_1 FROM CalculosPrecos WHERE requerido_1 IS NOT NULL AND requerido_1 != '' ORDER BY requerido_1", conexao)
    return df['requerido_1'].tolist()

def buscar_resumo_credores(conexao):
    query = "SELECT cpf_credor AS documento, MAX(requerente) AS nome, COUNT(id) AS qtd_creditos, SUM(valor_liquido_final) AS valor_total FROM CalculosPrecos WHERE cpf_credor IS NOT NULL AND cpf_credor != '' GROUP BY cpf_credor ORDER BY MAX(requerente);"
    return pd.read_sql_query(query, conexao)

def buscar_creditos_por_cpf(conexao, cpf):
    query = "SELECT id, numero_processo, requerente, requerido_1, valor_liquido_final FROM CalculosPrecos WHERE cpf_credor = ?"
    return pd.read_sql_query(query, conexao, params=(cpf,))

# --- FUNÇÕES DE ESCRITA ---

def atualizar_status_relacionamento_massa(conexao, credores_para_mover, novo_status):
    try:
        cursor = conexao.cursor()
        for _, credor in credores_para_mover.iterrows():
            chave = credor['chave_agrupamento']
            cursor.execute("INSERT OR IGNORE INTO GestaoCredores (chave_agrupamento) VALUES (?)", (chave,))
            cursor.execute("UPDATE GestaoCredores SET status_relacionamento = ? WHERE chave_agrupamento = ?", (novo_status, chave))
        conexao.commit(); return True, "Status atualizado com sucesso."
    except Exception as e: return False, f"Erro na atualização em massa: {e}"

def salvar_dado_credor(conexao, chave_agrupamento, campo, novo_valor):
    try:
        cursor = conexao.cursor()
        cursor.execute("INSERT OR IGNORE INTO GestaoCredores (chave_agrupamento) VALUES (?)", (chave_agrupamento,))
        cursor.execute(f"UPDATE GestaoCredores SET {campo} = ? WHERE chave_agrupamento = ?", (novo_valor, chave_agrupamento))
        conexao.commit(); return True
    except Exception as e: print(f"Erro ao salvar dado do credor: {e}"); return False

def salvar_regras_desagio_padrao(conexao, min_p, max_p):
    cursor = conexao.cursor()
    cursor.execute("INSERT OR REPLACE INTO Configuracoes (chave, valor) VALUES ('desagio_min_padrao', ?)", (str(min_p),))
    cursor.execute("INSERT OR REPLACE INTO Configuracoes (chave, valor) VALUES ('desagio_max_padrao', ?)", (str(max_p),))
    conexao.commit()

def salvar_devedor(conexao, nome, min_p, max_p):
    cursor = conexao.cursor(); cursor.execute("INSERT OR REPLACE INTO Devedores (nome_devedor, desagio_min_percentual, desagio_max_percentual) VALUES (?, ?, ?)", (nome.upper(), min_p, max_p)); conexao.commit()

def deletar_devedor(conexao, nome):
    cursor = conexao.cursor(); cursor.execute("DELETE FROM Devedores WHERE nome_devedor = ?", (nome,)); conexao.commit()

def criar_credito_manual(conexao, dados_credito):
    try:
        cursor = conexao.cursor()
        cursor.execute("INSERT INTO CalculosPrecos (numero_processo, cpf_credor, requerente, requerido_1, valor_liquido_final, arquivo_de_origem) VALUES (:numero_processo, :cpf_credor, :requerente, :requerido_1, :valor_liquido_final, :arquivo_de_origem)", dados_credito)
        novo_id = cursor.lastrowid; conexao.commit(); return True, novo_id
    except Exception as e: return False, str(e)

def atualizar_status_credito(conexao, credito_id, novo_status):
    try:
        cursor = conexao.cursor()
        cursor.execute("INSERT OR IGNORE INTO GestaoCreditos (id) VALUES (?)", (credito_id,))
        cursor.execute("UPDATE GestaoCreditos SET status_workflow = ?, data_ultima_atualizacao = ? WHERE id = ?", (novo_status, datetime.now(), credito_id))
        conexao.commit(); return True
    except Exception: return False

def salvar_anotacao_credito(conexao, credito_id, nova_anotacao):
    try:
        cursor = conexao.cursor()
        cursor.execute("INSERT OR IGNORE INTO GestaoCreditos (id) VALUES (?)", (int(credito_id),))
        cursor.execute("UPDATE GestaoCreditos SET rascunho_anotacao = ? WHERE id = ?", (nova_anotacao, int(credito_id)))
        conexao.commit(); return True
    except Exception: return False

# --- FUNÇÕES NOVAS (ADICIONADAS SEM QUEBRAR NADA) ---

def atualizar_proposta_credito(conexao, credito_id, novo_status, valor_proposta):
    """Adicionada para o Dossiê do Credor."""
    try:
        cursor = conexao.cursor()
        cursor.execute("INSERT OR IGNORE INTO GestaoCreditos (id) VALUES (?)", (credito_id,))
        cursor.execute("UPDATE GestaoCreditos SET status_workflow = ?, valor_ultima_proposta = ?, data_ultima_atualizacao = ? WHERE id = ?", (novo_status, valor_proposta, datetime.now(), credito_id))
        conexao.commit(); return True
    except Exception as e: print(f"ERRO AO ATUALIZAR PROPOSTA: {e}"); return False

def buscar_creditos_por_credor(conexao, chave_agrupamento):
    """Versão para o Dossiê do Credor, que busca o valor da proposta."""
    query = """
        SELECT
            cp.id, cp.numero_processo, cp.valor_liquido_final,
            cp.requerido_1,
            COALESCE(gc.status_workflow, 'Novo') as status_workflow,
            gc.rascunho_anotacao,
            gc.valor_ultima_proposta
        FROM CalculosPrecos cp
        LEFT JOIN GestaoCreditos gc ON cp.id = gc.id
        WHERE COALESCE(NULLIF(TRIM(cp.cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) = ?
    """
    return pd.read_sql_query(query, conexao, params=(chave_agrupamento,))


def buscar_desagio_para_proposta(conexao, nome_devedor):
    """
    Busca deságio para cálculo de proposta com hierarquia:
    1. Deságio específico do devedor (tabela Devedores)
    2. Deságio padrão (tabela Configuracoes)
    3. Fallback fixo (47%-52%)
    
    Args:
        conexao: Conexão SQLite
        nome_devedor: Nome do devedor (requerido_1 do processo)
    
    Returns:
        tuple: (desagio_min, desagio_max, origem)
        Exemplo: (47.0, 52.0, 'padrão')
    """
    try:
        cursor = conexao.cursor()
        
        # 1. Tentar buscar deságio específico do devedor
        if nome_devedor:
            # Normalizar o nome do devedor removendo acentos e convertendo para maiúsculas
            import unicodedata
            nome_normalizado = unicodedata.normalize('NFKD', nome_devedor.strip().upper())
            nome_normalizado = ''.join([c for c in nome_normalizado if not unicodedata.combining(c)])
            
            # Buscar todos os devedores e comparar após normalização
            cursor.execute("""
                SELECT nome_devedor, desagio_min_percentual, desagio_max_percentual
                FROM Devedores
                WHERE desagio_min_percentual > 0
                  AND desagio_max_percentual > 0
            """)
            
            for row in cursor.fetchall():
                nome_banco = row[0]
                nome_banco_normalizado = unicodedata.normalize('NFKD', nome_banco.strip().upper())
                nome_banco_normalizado = ''.join([c for c in nome_banco_normalizado if not unicodedata.combining(c)])
                
                if nome_normalizado == nome_banco_normalizado:
                    return (
                        float(row[1]),
                        float(row[2]),
                        'específico'
                    )
        
        # 2. Buscar deságio padrão das Configurações
        cursor.execute("SELECT valor FROM Configuracoes WHERE chave = 'desagio_min_padrao'")
        desagio_min = cursor.fetchone()
        
        cursor.execute("SELECT valor FROM Configuracoes WHERE chave = 'desagio_max_padrao'")
        desagio_max = cursor.fetchone()
        
        if desagio_min and desagio_max:
            try:
                min_val = float(desagio_min[0])
                max_val = float(desagio_max[0])
                if min_val > 0 and max_val > 0:
                    return (min_val, max_val, 'padrão')
            except (ValueError, TypeError):
                pass  # Continua para fallback
        
        # 3. Fallback fixo
        return (47.0, 52.0, 'fixo')
        
    except Exception as e:
        print(f"Erro ao buscar deságio: {e}")
        return (47.0, 52.0, 'fixo (erro)')

# =============================
# ESQUEMA DE GRUPOS (MIGRAÇÕES)
# =============================

def garantir_esquema_grupos(conexao):
    """Cria tabelas e índices de grupos se não existirem; adiciona coluna em Usuarios.
    Não altera dados existentes; executa idempotente.
    """
    cursor = conexao.cursor()
    # Tabela de grupos
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Grupos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE NOT NULL,
            descricao TEXT
        )
        """
    )
    # Tabela de vínculo crédito-grupo
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS GruposCreditos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            grupo_id INTEGER NOT NULL,
            credito_id INTEGER NOT NULL,
            UNIQUE(grupo_id, credito_id)
        )
        """
    )
    # Índices para performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gruposcreditos_grupo ON GruposCreditos (grupo_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gruposcreditos_credito ON GruposCreditos (credito_id)")
    # Adicionar coluna grupo_id em Usuarios, se não existir
    try:
        cursor.execute("ALTER TABLE Usuarios ADD COLUMN grupo_id INTEGER")
    except Exception:
        pass  # coluna já existe
    conexao.commit()

# =============================
# ESQUEMA BÁSICO: USUÁRIOS
# =============================
def garantir_esquema_usuarios(conexao):
    """Cria a tabela Usuarios se não existir, com colunas esperadas pelo sistema.
    Inclui a coluna grupo_id para compatibilidade com funcionalidades de grupos.
    """
    cursor = conexao.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_usuario TEXT NOT NULL UNIQUE,
            perfil TEXT NOT NULL,
            senha_hash TEXT NOT NULL,
            senha_salt TEXT NOT NULL,
            grupo_id INTEGER
        )
        """
    )
    # Índice único já coberto pela constraint UNIQUE(nome_usuario); manter idempotência
    conexao.commit()

# =============================
# FUNÇÕES DE GRUPOS (CRUD)
# =============================

def buscar_grupos(conexao):
    return pd.read_sql_query("SELECT id, nome, descricao FROM Grupos ORDER BY nome", conexao)


def criar_grupo(conexao, nome, descricao=None):
    try:
        cursor = conexao.cursor()
        cursor.execute("INSERT INTO Grupos (nome, descricao) VALUES (?, ?)", (nome.strip(), descricao))
        conexao.commit()
        return True, cursor.lastrowid
    except Exception as e:
        return False, str(e)


def atualizar_grupo(conexao, grupo_id, nome=None, descricao=None):
    try:
        cursor = conexao.cursor()
        if nome is not None:
            cursor.execute("UPDATE Grupos SET nome = ? WHERE id = ?", (nome.strip(), grupo_id))
        if descricao is not None:
            cursor.execute("UPDATE Grupos SET descricao = ? WHERE id = ?", (descricao, grupo_id))
        conexao.commit()
        return True
    except Exception as e:
        return False


def deletar_grupo(conexao, grupo_id):
    try:
        cursor = conexao.cursor()
        cursor.execute("DELETE FROM Grupos WHERE id = ?", (grupo_id,))
        cursor.execute("DELETE FROM GruposCreditos WHERE grupo_id = ?", (grupo_id,))
        conexao.commit()
        return True
    except Exception:
        return False


def buscar_grupo_do_usuario(conexao, user_id):
    cursor = conexao.cursor()
    cursor.execute("SELECT grupo_id FROM Usuarios WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else None


def atribuir_grupo_usuario(conexao, user_id, grupo_id):
    try:
        cursor = conexao.cursor()
        cursor.execute("UPDATE Usuarios SET grupo_id = ? WHERE id = ?", (grupo_id, user_id))
        conexao.commit()
        return True
    except Exception:
        return False

# =============================
# Vínculo de Créditos aos Grupos
# =============================

def vincular_credito_a_grupo(conexao, grupo_id, credito_id):
    try:
        cursor = conexao.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO GruposCreditos (grupo_id, credito_id) VALUES (?, ?)",
            (grupo_id, int(credito_id))
        )
        conexao.commit()
        return True
    except Exception:
        return False


def desvincular_credito_de_grupo(conexao, grupo_id, credito_id):
    try:
        cursor = conexao.cursor()
        cursor.execute(
            "DELETE FROM GruposCreditos WHERE grupo_id = ? AND credito_id = ?",
            (grupo_id, int(credito_id))
        )
        conexao.commit()
        return True
    except Exception:
        return False


def vincular_chave_agrupamento_a_grupo(conexao, grupo_id, chave_agrupamento):
    """Vincula TODOS os créditos da chave_agrupamento ao grupo."""
    try:
        cursor = conexao.cursor()
        cursor.execute(
            """
            SELECT id FROM CalculosPrecos
            WHERE COALESCE(NULLIF(TRIM(cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(requerente))) = ?
            """,
            (chave_agrupamento,)
        )
        ids = [row[0] for row in cursor.fetchall()]
        for cid in ids:
            cursor.execute(
                "INSERT OR IGNORE INTO GruposCreditos (grupo_id, credito_id) VALUES (?, ?)",
                (grupo_id, cid)
            )
        conexao.commit()
        return True, len(ids)
    except Exception as e:
        return False, str(e)


def usuario_pode_ver_chave(conexao, user_id, chave_agrupamento):
    """Retorna True se o usuário for Admin ou houver qualquer crédito da chave vinculado ao grupo do usuário."""
    cursor = conexao.cursor()
    # Checar perfil
    cursor.execute("SELECT perfil, grupo_id FROM Usuarios WHERE id = ?", (user_id,))
    row = cursor.fetchone()
    if not row:
        return False
    perfil, grupo_id = row
    if perfil == 'Admin':
        return True
    if not grupo_id:
        return False
    cursor.execute(
        """
        SELECT 1 FROM CalculosPrecos cp
        JOIN GruposCreditos gc ON gc.credito_id = cp.id
        WHERE gc.grupo_id = ? AND COALESCE(NULLIF(TRIM(cp.cpf_credor), ''), 'S_CPF-' || UPPER(TRIM(cp.requerente))) = ?
        LIMIT 1
        """,
        (grupo_id, chave_agrupamento)
    )
    return cursor.fetchone() is not None