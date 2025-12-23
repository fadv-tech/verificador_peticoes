import sqlite3
import os
import re

# SQL para precatorios_estrategico.db
SQL_PRECATORIOS_ESTRATEGICO = """
PRAGMA foreign_keys = ON;
PRAGMA case_sensitive_like = OFF;

CREATE TABLE AceitesOperacao (
        operacao_id INTEGER PRIMARY KEY,
        data_hora_aceite TEXT,
        credor_nome TEXT,
        valor_compra REAL,
        forma_pagamento TEXT,
        observacoes TEXT
    );

CREATE TABLE AssinaturasOperacao (
        operacao_id INTEGER PRIMARY KEY,
        status_assinatura TEXT,
        data_assinatura TEXT,
        assinantes TEXT
    );

CREATE TABLE CalculosPrecos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, numero_processo TEXT NOT NULL, cpf_credor TEXT NOT NULL,
                    requerente TEXT, requerido_1 TEXT, valor_total_condenacao REAL, amortizacoes_realizadas REAL,
                    restituicao_de_custas REAL, valor_bruto_rpv_precatorio REAL, parcela_isenta_juros REAL,
                    contribuicao_previdenciaria REAL, imposto_de_renda_retido REAL, honorarios_contratuais REAL,
                    valor_liquido_final REAL, arquivo_de_origem TEXT, requerido TEXT, UNIQUE(numero_processo, cpf_credor)
                );

CREATE TABLE ConfigPropostasAceitasRequisitos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        etapa TEXT NOT NULL,
        item_codigo TEXT NOT NULL,
        item_nome TEXT NOT NULL,
        obrigatorio INTEGER DEFAULT 1,
        ativo INTEGER DEFAULT 1,
        updated_at TEXT,
        updated_by TEXT,
        UNIQUE(etapa, item_codigo)
    );

CREATE TABLE Configuracoes (
        chave TEXT PRIMARY KEY,
        valor TEXT
    );

CREATE TABLE ContratosOperacao (
        operacao_id INTEGER PRIMARY KEY,
        tipo_contrato TEXT,
        condicao_pagamento TEXT,
        prazos TEXT
    );

CREATE TABLE DadosProcesso (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_processo TEXT NOT NULL,
            chave TEXT NOT NULL,
            valor TEXT,
            data_extracao TEXT,
            fonte TEXT,
            FOREIGN KEY (numero_processo) REFERENCES CalculosPrecos(numero_processo)
        );

CREATE TABLE Decisoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                processo_id INTEGER,
                movimentacao_id_projudi TEXT,
                texto_decisao TEXT,
                tipo TEXT, -- 'publica' ou 'nova'
                FOREIGN KEY (processo_id) REFERENCES ExtracaoDiario(id)
            );

CREATE TABLE Devedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_devedor TEXT NOT NULL UNIQUE,
        desagio_min_percentual REAL,
        desagio_max_percentual REAL
    );

CREATE TABLE DocumentosOperacao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        operacao_id INTEGER,
        categoria TEXT,
        item_codigo TEXT,
        item_nome TEXT,
        status TEXT,
        obs TEXT
    );

CREATE TABLE DueDiligencias (
        operacao_id INTEGER PRIMARY KEY,
        tipo TEXT,
        resultado TEXT,
        parecer TEXT
    );

CREATE TABLE EncerramentosOperacao (
        operacao_id INTEGER PRIMARY KEY,
        status_final TEXT,
        data_encerramento TEXT,
        resumo TEXT
    );

CREATE TABLE ExtracaoDiario (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_processo_cnj TEXT NOT NULL UNIQUE,
        polo_ativo_credor TEXT, polo_passivo TEXT, advogado_do_credor TEXT,
        frase_encontrada TEXT, contexto_posterior TEXT,
        pdf_relpath TEXT, arquivo_pdf TEXT, caminho_pasta TEXT,
        inserido_em_iso TEXT, pdf_mtime_iso TEXT, pdf_size_bytes REAL, pagina_da_frase INTEGER
    , status_coleta TEXT DEFAULT 'pendente', data_ultima_tentativa TEXT, mensagem_erro TEXT);

CREATE TABLE GestaoCreditos (id INTEGER PRIMARY KEY, status_workflow TEXT, rascunho_anotacao TEXT, data_ultima_atualizacao DATETIME, valor_ultima_proposta REAL);

CREATE TABLE GestaoCredores (chave_agrupamento TEXT PRIMARY KEY, status_relacionamento TEXT NOT NULL, telefone TEXT, email TEXT, anotacoes_gerais TEXT);

CREATE TABLE Grupos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE NOT NULL,
            descricao TEXT
        );

CREATE TABLE GruposCreditos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            grupo_id INTEGER NOT NULL,
            credito_id INTEGER NOT NULL,
            UNIQUE(grupo_id, credito_id)
        );

CREATE TABLE HistoricoAcoes (
                id_log INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                id_usuario INTEGER,
                nome_usuario TEXT,
                tipo_acao TEXT NOT NULL,
                pagina_origem TEXT,
                chave_agrupamento_credor TEXT,
                id_credito INTEGER,
                detalhes_humanos TEXT,
                dados_alterados_json TEXT
            );

CREATE TABLE "HistoricoAcoes_OLD" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        nome_usuario TEXT NOT NULL,
        perfil_usuario TEXT,
        tipo_acao TEXT NOT NULL,
        detalhes TEXT
    );

CREATE TABLE HistoricoAnotacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        credito_id INTEGER NOT NULL,
        timestamp DATETIME NOT NULL,
        nome_usuario TEXT NOT NULL,
        anotacao_antiga TEXT,
        anotacao_nova TEXT,
        FOREIGN KEY (credito_id) REFERENCES CalculosPrecos (id)
    );

CREATE TABLE HomologacoesOperacao (
        operacao_id INTEGER PRIMARY KEY,
        data_peticao TEXT,
        status_homologacao TEXT,
        data_homologacao TEXT
    );

CREATE TABLE Movimentacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                processo_id INTEGER,
                data TEXT,
                descricao TEXT,
                id_projudi TEXT,
                FOREIGN KEY (processo_id) REFERENCES ExtracaoDiario(id)
            );

CREATE TABLE Operacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        credito_id INTEGER,
        grupo_id INTEGER,
        operador_username TEXT,
        tipo_pessoa TEXT,
        tipo_credito TEXT,
        numero_processo TEXT,
        status_atual TEXT,
        criado_em TEXT
    );

CREATE TABLE PagamentosOperacao (
        operacao_id INTEGER PRIMARY KEY,
        data_pagamento TEXT,
        valor_pago REAL,
        metodo TEXT
    );

CREATE TABLE PropostasAceitas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    credito_id INTEGER NOT NULL,
    valor_proposta REAL,
    status TEXT,
    data_aceite TEXT,
    data_pagamento TEXT,
    observacoes TEXT,
    situacao_pagamento TEXT,
    forma_pagamento TEXT,
    banco TEXT,
    agencia TEXT,
    conta TEXT,
    pix TEXT,
    FOREIGN KEY (credito_id) REFERENCES CalculosPrecos(id)
);

CREATE TABLE RedacoesOperacao (
        operacao_id INTEGER PRIMARY KEY,
        status_redacao TEXT,
        arquivo_link TEXT
    );

CREATE TABLE Usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_usuario TEXT NOT NULL UNIQUE, perfil TEXT NOT NULL,
        senha_hash TEXT NOT NULL, senha_salt TEXT NOT NULL
    , grupo_id INTEGER);

CREATE INDEX idx_dados_chave ON DadosProcesso(chave);

CREATE INDEX idx_dados_processo ON DadosProcesso(numero_processo);

CREATE INDEX idx_gruposcreditos_credito ON GruposCreditos (credito_id);

CREATE INDEX idx_gruposcreditos_grupo ON GruposCreditos (grupo_id);
"""

# SQL para processos_v2.db
SQL_PROCESSOS_V2 = """
PRAGMA foreign_keys = ON;
PRAGMA case_sensitive_like = OFF;

CREATE TABLE Decisoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    movimentacao_id INTEGER, -- Liga com a movimentação específica
                    texto_completo TEXT,
                    data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (movimentacao_id) REFERENCES Movimentacoes (id) ON DELETE CASCADE
                );

CREATE TABLE HistoricoEstado (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    processo_id INTEGER,
                    classe_judicial TEXT,
                    assunto TEXT,
                    fase_processual TEXT,
                    data_coleta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (processo_id) REFERENCES Processos (id) ON DELETE CASCADE
                );

CREATE TABLE Movimentacoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    processo_id INTEGER,
                    data_movimentacao TEXT,
                    descricao TEXT,
                    usuario TEXT,
                    movimentacao_id_projudi TEXT, -- ID da movimentação no site do Projudi
                    FOREIGN KEY (processo_id) REFERENCES Processos (id) ON DELETE CASCADE
                );

CREATE TABLE Processos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    numero_processo TEXT UNIQUE NOT NULL,
                    data_distribuicao TEXT,
                    valor_causa TEXT,
                    data_ultima_coleta TIMESTAMP
                );
"""

def create_database_from_sql(db_name, sql_content):
    """
    Cria um banco de dados SQLite a partir de uma string SQL contendo as declarações.
    O arquivo .db será criado no mesmo diretório de execução.
    """
    db_path = db_name # Cria no diretório de trabalho atual
    
    print(f"Iniciando a criação do banco de dados: {db_name} em {os.path.abspath(db_path)}")
    
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Arquivo existente {db_name} removido.")

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # O executescript é ideal para executar múltiplas declarações SQL de uma só vez
        cursor.executescript(sql_content)
        
        conn.commit()
        print(f"Banco de dados {db_name} criado com sucesso.")

    except sqlite3.Error as e:
        print(f"Erro ao criar o banco de dados {db_name}: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # Definições dos bancos de dados a serem criados
    databases_to_create = [
        {
            "db_name": "precatorios_estrategico.db",
            "sql_content": SQL_PRECATORIOS_ESTRATEGICO
        },
        {
            "db_name": "processos_v2.db",
            "sql_content": SQL_PROCESSOS_V2
        }
    ]

    for db_info in databases_to_create:
        create_database_from_sql(db_info["db_name"], db_info["sql_content"])
