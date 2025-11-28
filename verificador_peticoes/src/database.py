import sqlite3
import os
import logging
import shutil
from datetime import datetime
from typing import List, Dict, Optional

class DatabaseManager:
    def __init__(self, db_path: str = "data/verificacoes.db"):
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        if not os.path.isabs(db_path):
            db_path = os.path.join(base, db_path)
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Inicializa o banco de dados com as tabelas necessárias"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('PRAGMA journal_mode=WAL')
                cursor.execute('PRAGMA synchronous=NORMAL')
            except Exception:
                pass
            
            # Tabela de verificações
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS verificacoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    numero_processo TEXT NOT NULL,
                    identificador_peticao TEXT NOT NULL,
                    nome_arquivo_original TEXT NOT NULL,
                    status_verificacao TEXT NOT NULL,
                    peticao_encontrada TEXT,
                    data_verificacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    detalhes TEXT,
                    UNIQUE(numero_processo, identificador_peticao)
                )
            ''')
            
            # Tabela de logs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS logs_verificacao (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    nivel TEXT NOT NULL,
                    mensagem TEXT NOT NULL,
                    detalhes TEXT,
                    batch_id TEXT,
                    worker_id TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS execucoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT UNIQUE,
                    iniciado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finalizado_em TIMESTAMP,
                    usuario_projudi TEXT,
                    navegador_modo TEXT,
                    host_execucao TEXT,
                    total_arquivos INTEGER,
                    total_protocolizadas INTEGER,
                    total_nao_encontradas INTEGER,
                    status TEXT DEFAULT 'pending',
                    progress INTEGER DEFAULT 0
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    nome_arquivo TEXT NOT NULL,
                    numero_processo TEXT NOT NULL,
                    identificador TEXT,
                    status TEXT DEFAULT 'pending',
                    mensagem TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP
                )
            ''')

            # Índices para performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_processo ON verificacoes(numero_processo)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_identificador ON verificacoes(identificador_peticao)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_data ON verificacoes(data_verificacao)')

            # Tabela de configurações
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS config (
                    chave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS credenciais (
                    usuario TEXT PRIMARY KEY,
                    senha TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Migração de colunas extras para verificacoes
            try:
                cursor.execute("PRAGMA table_info(verificacoes)")
                cols = {row[1] for row in cursor.fetchall()}
                if 'usuario_projudi' not in cols:
                    cursor.execute("ALTER TABLE verificacoes ADD COLUMN usuario_projudi TEXT DEFAULT ''")
                if 'navegador_modo' not in cols:
                    cursor.execute("ALTER TABLE verificacoes ADD COLUMN navegador_modo TEXT DEFAULT ''")
                if 'host_execucao' not in cols:
                    cursor.execute("ALTER TABLE verificacoes ADD COLUMN host_execucao TEXT DEFAULT ''")
                if 'batch_id' not in cols:
                    cursor.execute("ALTER TABLE verificacoes ADD COLUMN batch_id TEXT DEFAULT ''")
            except Exception:
                pass

            try:
                cursor.execute("PRAGMA table_info(logs_verificacao)")
                cols = {row[1] for row in cursor.fetchall()}
                if 'batch_id' not in cols:
                    cursor.execute("ALTER TABLE logs_verificacao ADD COLUMN batch_id TEXT")
                if 'worker_id' not in cols:
                    cursor.execute("ALTER TABLE logs_verificacao ADD COLUMN worker_id TEXT")
            except Exception:
                pass

            try:
                cursor.execute("PRAGMA table_info(execucoes)")
                cols = {row[1] for row in cursor.fetchall()}
                if 'status' not in cols:
                    cursor.execute("ALTER TABLE execucoes ADD COLUMN status TEXT DEFAULT 'pending'")
                if 'progress' not in cols:
                    cursor.execute("ALTER TABLE execucoes ADD COLUMN progress INTEGER DEFAULT 0")
                if 'heartbeat_at' not in cols:
                    cursor.execute("ALTER TABLE execucoes ADD COLUMN heartbeat_at TIMESTAMP")
            except Exception:
                pass

            conn.commit()
    
    def registrar_verificacao(self, numero_processo: str, identificador_peticao: str, 
                            nome_arquivo: str, status: str, peticao_encontrada: str = None, 
                            detalhes: str = None, usuario_projudi: str = '', navegador_modo: str = '',
                            host_execucao: str = '', batch_id: str = '') -> int:
        """Registra uma verificação no banco de dados"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO verificacoes 
                    (numero_processo, identificador_peticao, nome_arquivo_original, 
                     status_verificacao, peticao_encontrada, detalhes, usuario_projudi, navegador_modo, host_execucao, batch_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (numero_processo, identificador_peticao, nome_arquivo, 
                      status, peticao_encontrada, detalhes, usuario_projudi, navegador_modo, host_execucao, batch_id))
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            logging.error(f"Erro ao registrar verificação: {e}")
            return -1
    
    def obter_verificacoes_recentes(self, limite: int = 100) -> List[Dict]:
        """Obtém as verificações mais recentes"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM verificacoes 
                    ORDER BY data_verificacao DESC 
                    LIMIT ?
                ''', (limite,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao obter verificações: {e}")
            return []
    
    def obter_estatisticas(self) -> Dict:
        """Obtém estatísticas das verificações"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Total de verificações
                cursor.execute('SELECT COUNT(*) FROM verificacoes')
                total = cursor.fetchone()[0]
                
                # Verificações por status
                cursor.execute('''
                    SELECT status_verificacao, COUNT(*) as quantidade 
                    FROM verificacoes 
                    GROUP BY status_verificacao
                ''')
                por_status = dict(cursor.fetchall())
                
                # Verificações do dia
                cursor.execute('''
                    SELECT COUNT(*) FROM verificacoes 
                    WHERE DATE(data_verificacao) = DATE('now')
                ''')
                hoje = cursor.fetchone()[0]
                
                return {
                    'total_verificacoes': total,
                    'verificacoes_hoje': hoje,
                    'por_status': por_status
                }
        except Exception as e:
            logging.error(f"Erro ao obter estatísticas: {e}")
            return {}
    
    def registrar_log(self, nivel: str, mensagem: str, detalhes: str = None, batch_id: str = None, worker_id: str = None):
        """Registra um log no banco de dados"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO logs_verificacao (nivel, mensagem, detalhes, batch_id, worker_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (nivel, mensagem, detalhes, batch_id, worker_id))
                conn.commit()
        except Exception as e:
            print(f"Erro ao registrar log: {e}")

    def obter_logs_recentes(self, limite: int = 200) -> List[Dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM logs_verificacao 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (limite,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao obter logs: {e}")
            return []

    def set_config(self, chave: str, valor: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO config (chave, valor) VALUES (?, ?)
                    ON CONFLICT(chave) DO UPDATE SET valor=excluded.valor
                ''', (chave, valor))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao salvar configuração {chave}: {e}")

    def get_config(self, chave: str) -> str:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT valor FROM config WHERE chave = ?', (chave,))
                row = cursor.fetchone()
                return row[0] if row else ''
        except Exception as e:
            logging.error(f"Erro ao obter configuração {chave}: {e}")
            return ''

    def get_all_config(self) -> dict:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT chave, valor FROM config')
                return {row['chave']: row['valor'] for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Erro ao obter configurações: {e}")
            return {}

    def save_credencial(self, usuario: str, senha: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO credenciais (usuario, senha) VALUES (?, ?)', (usuario, senha))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao salvar credencial {usuario}: {e}")

    def list_usuarios(self) -> List[str]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT usuario FROM credenciais ORDER BY usuario')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao listar usuários: {e}")
            return []

    def get_password(self, usuario: str) -> str:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT senha FROM credenciais WHERE usuario=?', (usuario,))
                row = cursor.fetchone()
                return row[0] if row else ''
        except Exception as e:
            logging.error(f"Erro ao obter senha de {usuario}: {e}")
            return ''

    def iniciar_execucao(self, batch_id: str, usuario_projudi: str, navegador_modo: str, host_execucao: str, total_arquivos: int):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO execucoes (batch_id, iniciado_em, usuario_projudi, navegador_modo, host_execucao, total_arquivos, status, progress)
                    VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, 'queued', 0)
                ''', (batch_id, usuario_projudi, navegador_modo, host_execucao, total_arquivos))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao iniciar execução: {e}")

    def finalizar_execucao(self, batch_id: str, total_protocolizadas: int, total_nao_encontradas: int):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE execucoes SET finalizado_em=CURRENT_TIMESTAMP, total_protocolizadas=?, total_nao_encontradas=?, status='done'
                    WHERE batch_id=?
                ''', (total_protocolizadas, total_nao_encontradas, batch_id))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao finalizar execução: {e}")

    def obter_execucoes(self, limite: int = 200) -> List[Dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM execucoes ORDER BY iniciado_em DESC LIMIT ?
                ''', (limite,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao obter execuções: {e}")
            return []

    def obter_logs_por_batch(self, batch_id: str) -> List[Dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM logs_verificacao WHERE batch_id=? ORDER BY timestamp DESC
                ''', (batch_id,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao obter logs por batch: {e}")
            return []

    def obter_verificacoes_por_batch(self, batch_id: str) -> List[Dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM verificacoes WHERE batch_id=? ORDER BY data_verificacao DESC
                ''', (batch_id,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao obter verificações por batch: {e}")
            return []

    def backup_e_reset(self) -> str:
        try:
            base_dir = os.path.dirname(self.db_path)
            os.makedirs(base_dir, exist_ok=True)
            backup_dir = os.path.join(base_dir, 'backups')
            os.makedirs(backup_dir, exist_ok=True)
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(backup_dir, f'verificacoes_{ts}.db')

            db_exists = os.path.exists(self.db_path)

            if db_exists:
                try:
                    src = sqlite3.connect(self.db_path)
                    # VACUUM INTO cria uma cópia consistente mesmo com o BD aberto.
                    safe_backup = backup_path.replace("'", "''")
                    src.execute(f"VACUUM INTO '{safe_backup}'")
                    src.close()
                except Exception as e:
                    logging.warning(f"VACUUM INTO falhou, tentando cópia direta: {e}")
                    try:
                        shutil.copy2(self.db_path, backup_path)
                    except Exception as e2:
                        logging.error(f"Falha ao copiar banco para backup: {e2}")
                        # Cria um backup vazio para não falhar completamente
                        sqlite3.connect(backup_path).close()
            else:
                # Caso não exista, cria backup vazio
                sqlite3.connect(backup_path).close()

            removed = False
            if db_exists:
                try:
                    os.remove(self.db_path)
                    removed = True
                except Exception as e:
                    logging.warning(f"Falha ao remover banco (arquivo pode estar em uso): {e}. Aplicando reset por DROP.")
                    try:
                        conn = sqlite3.connect(self.db_path)
                        cur = conn.cursor()
                        cur.execute('DROP TABLE IF EXISTS verificacoes')
                        cur.execute('DROP TABLE IF EXISTS logs_verificacao')
                        cur.execute('DROP TABLE IF EXISTS execucoes')
                        cur.execute('DROP TABLE IF EXISTS config')
                        conn.commit()
                        conn.close()
                    except Exception as e2:
                        logging.error(f"Falha ao dropar tabelas: {e2}")

            # Recria estrutura
            self.init_database()
            return backup_path
        except Exception as e:
            logging.error(f"Erro no backup e reset do banco: {e}")
            return ''

    def execucoes_ativas(self) -> int:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM execucoes WHERE finalizado_em IS NULL')
                row = cursor.fetchone()
                return int(row[0]) if row and row[0] is not None else 0
        except Exception as e:
            logging.error(f"Erro ao verificar execuções ativas: {e}")
            return 0

    def contar_status_por_batch(self, batch_id: str) -> Dict:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT status_verificacao, COUNT(*) FROM verificacoes WHERE batch_id=? GROUP BY status_verificacao
                ''', (batch_id,))
                rows = cursor.fetchall()
                mapa = {r[0]: int(r[1]) for r in rows}
                total_protocolizadas = mapa.get('Protocolizada', 0)
                total_nao_encontradas = mapa.get('Não encontrada', 0)
                return {
                    'protocolizadas': total_protocolizadas,
                    'nao_encontradas': total_nao_encontradas
                }
        except Exception as e:
            logging.error(f"Erro ao contar status por batch: {e}")
            return {'protocolizadas': 0, 'nao_encontradas': 0}

    def finalizar_execucao_forcada(self, batch_id: str):
        try:
            contagem = self.contar_status_por_batch(batch_id)
            self.finalizar_execucao(batch_id, contagem['protocolizadas'], contagem['nao_encontradas'])
            return True
        except Exception as e:
            logging.error(f"Erro ao finalizar execução forçada: {e}")
            return False

    def finalizar_todas_execucoes_ativas(self) -> int:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT batch_id FROM execucoes WHERE finalizado_em IS NULL')
                batches = [row['batch_id'] for row in cursor.fetchall() if row['batch_id']]
            total = 0
            for b in batches:
                if self.finalizar_execucao_forcada(b):
                    total += 1
            return total
        except Exception as e:
            logging.error(f"Erro ao finalizar todas execuções ativas: {e}")
            return 0

    def adicionar_itens_execucao(self, batch_id: str, itens: List[Dict]):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for it in itens:
                    cursor.execute('''
                        INSERT INTO job_items (batch_id, nome_arquivo, numero_processo, identificador, status, mensagem)
                        VALUES (?, ?, ?, ?, 'pending', '')
                    ''', (batch_id, it.get('nome_original',''), it.get('numero_processo',''), it.get('identificador_peticao','')))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao adicionar itens da execução: {e}")

    def obter_itens_pendentes(self, batch_id: Optional[str] = None) -> List[Dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                if batch_id:
                    cursor.execute('SELECT * FROM job_items WHERE batch_id=? AND status="pending" ORDER BY id ASC', (batch_id,))
                else:
                    cursor.execute('SELECT * FROM job_items WHERE status="pending" ORDER BY id ASC')
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Erro ao obter itens pendentes: {e}")
            return []

    def atualizar_item_status(self, item_id: int, status: str, mensagem: str = ''):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE job_items SET status=?, mensagem=?, updated_at=CURRENT_TIMESTAMP WHERE id=?', (status, mensagem, item_id))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao atualizar status do item {item_id}: {e}")

    def tentar_iniciar_item(self, item_id: int) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE job_items SET status="running", mensagem="", updated_at=CURRENT_TIMESTAMP WHERE id=? AND status="pending"', (item_id,))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Erro ao tentar iniciar item {item_id}: {e}")
            return False

    def atualizar_execucao_status(self, batch_id: str, status: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE execucoes SET status=? WHERE batch_id=?', (status, batch_id))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao atualizar status da execução {batch_id}: {e}")

    def obter_execucao_por_batch(self, batch_id: str) -> Dict:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM execucoes WHERE batch_id=? LIMIT 1', (batch_id,))
                row = cursor.fetchone()
                return dict(row) if row else {}
        except Exception as e:
            logging.error(f"Erro ao obter execução {batch_id}: {e}")
            return {}

    def atualizar_execucao_heartbeat(self, batch_id: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE execucoes SET heartbeat_at=CURRENT_TIMESTAMP WHERE batch_id=?', (batch_id,))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao atualizar heartbeat da execução {batch_id}: {e}")

    def marcar_timeout_batch(self, batch_id: str, mensagem: str = 'Timeout 30s'):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE job_items SET status="failed", mensagem=? WHERE batch_id=? AND status IN ("pending","running")', (mensagem, batch_id))
                cursor.execute('UPDATE execucoes SET status="error", finalizado_em=CURRENT_TIMESTAMP WHERE batch_id=?', (batch_id,))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao marcar timeout do batch {batch_id}: {e}")

    def incrementar_progresso(self, batch_id: str, delta: int = 1):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('UPDATE execucoes SET progress = COALESCE(progress,0) + ? WHERE batch_id=?', (delta, batch_id))
                conn.commit()
        except Exception as e:
            logging.error(f"Erro ao incrementar progresso da execução {batch_id}: {e}")

# Configuração de logging
class LogManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.setup_logging()
    
    def setup_logging(self):
        """Configura o sistema de logging"""
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base_dir, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'verificador.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def info(self, mensagem: str, detalhes: str = None):
        logging.info(mensagem)
        self.db_manager.registrar_log('INFO', mensagem, detalhes)
    
    def warning(self, mensagem: str, detalhes: str = None):
        logging.warning(mensagem)
        self.db_manager.registrar_log('WARNING', mensagem, detalhes)
    
    def error(self, mensagem: str, detalhes: str = None):
        logging.error(mensagem)
        self.db_manager.registrar_log('ERROR', mensagem, detalhes)
    
    def debug(self, mensagem: str, detalhes: str = None):
        logging.debug(mensagem)
        self.db_manager.registrar_log('DEBUG', mensagem, detalhes)