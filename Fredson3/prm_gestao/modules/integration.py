# modules/integration.py
# Módulo para funções que integram os dois bancos de dados (coleta e gestão).

import sqlite3
from datetime import datetime

DB_GESTAO = "precatorios_estrategico.db"
DB_COLETA = "processos_v2.db"

def promover_processo_para_credito(numero_cnj: str):
    """
    Promove um processo do banco de coleta para um crédito no banco de gestão.
    Retorna uma tupla: (sucesso_booleano, mensagem_ou_novo_id).
    """
    if not numero_cnj:
        return (False, "Número do processo não foi fornecido.")

    try:
        # Usar 'with' garante que as conexões sejam fechadas mesmo se ocorrer um erro
        with sqlite3.connect(DB_GESTAO) as con_gestao, sqlite3.connect(DB_COLETA) as con_coleta:
            # Usar row_factory para acessar colunas pelo nome
            con_gestao.row_factory = sqlite3.Row
            con_coleta.row_factory = sqlite3.Row
            
            cur_gestao = con_gestao.cursor()
            cur_coleta = con_coleta.cursor()

            # 1. VERIFICAÇÃO DE SEGURANÇA: O crédito já existe no banco de gestão?
            cur_gestao.execute("SELECT id FROM CalculosPrecos WHERE numero_processo = ?", (numero_cnj,))
            if cur_gestao.fetchone():
                return (False, "Este processo já foi promovido a crédito anteriormente.")

            # 2. BUSCA DE DADOS: Pega os dados essenciais do banco de coleta
            cur_coleta.execute("SELECT polo_ativo, polo_passivo FROM Processos WHERE numero_processo = ?", (numero_cnj,))
            dados_brutos = cur_coleta.fetchone()
            if not dados_brutos:
                return (False, "Processo não encontrado no banco de dados de coleta para ser promovido.")

            # 3. INSERÇÃO NO BANCO DE GESTÃO: Cria o registro na tabela principal de créditos
            # Nota: Usamos um CPF genérico "A_PREENCHER" para satisfazer a restrição NOT NULL.
            # Este campo deverá ser atualizado manualmente depois.
            cur_gestao.execute("""
                INSERT INTO CalculosPrecos (numero_processo, cpf_credor, requerente, requerido_1, arquivo_de_origem)
                VALUES (?, ?, ?, ?, ?)
            """, (
                numero_cnj,
                "A_PREENCHER",
                dados_brutos["polo_ativo"],
                dados_brutos["polo_passivo"],
                "Promovido da Coleta Automática"
            ))
            
            novo_credito_id = cur_gestao.lastrowid

            # 4. CRIAÇÃO DO STATUS INICIAL: Adiciona o crédito à tabela de gestão de workflow
            cur_gestao.execute(
                "INSERT INTO GestaoCreditos (id, status_workflow, data_ultima_atualizacao) VALUES (?, ?, ?)",
                (novo_credito_id, 'Novo', datetime.now())
            )
            
            # Confirma todas as operações no banco de gestão
            con_gestao.commit()
            
            # Retorna sucesso e o ID do novo crédito criado
            return (True, novo_credito_id)

    except sqlite3.Error as e:
        # Em caso de qualquer erro no banco de dados, retorna falha e a mensagem de erro
        return (False, f"Erro de banco de dados durante a promoção: {e}")

