import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
import sqlite3
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

# --- Configurações Globais ---
NOME_BANCO_DADOS = 'processos_v2.db'
ARQUIVO_PROCESSOS_CSV = 'processos_publicos.csv'

# --- POOL DE CREDENCIAIS ---
POOL_DE_CREDENCIAS = [
    {"usuario": "85413054149", "senha": "Cioni6725-"},
    {"usuario": "07871865625", "senha": "asdASD00-"}
]

# --- Funções do Banco de Dados ---
def salvar_dados_completos(numero_processo, dados_gerais, movimentacoes, decisoes):
    with sqlite3.connect(NOME_BANCO_DADOS) as conn:
        cursor = conn.cursor()
        
        # Garante que o processo exista
        cursor.execute("SELECT id FROM Processos WHERE numero_processo = ?", (numero_processo,))
        resultado = cursor.fetchone()
        processo_id = resultado[0] if resultado else cursor.execute("INSERT INTO Processos (numero_processo) VALUES (?)", (numero_processo,)).lastrowid

        # Atualiza a tabela Processos
        cursor.execute("UPDATE Processos SET data_distribuicao = ?, valor_causa = ?, data_ultima_coleta = CURRENT_TIMESTAMP WHERE id = ?", 
                       (dados_gerais.get('data_distribuicao'), dados_gerais.get('valor_causa'), processo_id))

        # Lógica do Histórico de Estado
        cursor.execute("SELECT classe_judicial, assunto, fase_processual FROM HistoricoEstado WHERE processo_id = ? ORDER BY data_coleta DESC LIMIT 1", (processo_id,))
        ultimo_estado = cursor.fetchone()
        estado_atual = (dados_gerais.get('classe_judicial', 'N/A'), dados_gerais.get('assunto', 'N/A'), dados_gerais.get('fase_processual', 'N/A'))
        if not ultimo_estado or ultimo_estado != estado_atual:
            cursor.execute("INSERT INTO HistoricoEstado (processo_id, classe_judicial, assunto, fase_processual) VALUES (?, ?, ?, ?)", (processo_id, *estado_atual))

        # Limpa movimentações e decisões antigas e insere as novas
        cursor.execute("DELETE FROM Movimentacoes WHERE processo_id = ?", (processo_id,))
        if movimentacoes:
            for mov_id_projudi, data_mov, desc, usr in movimentacoes:
                cursor.execute('INSERT INTO Movimentacoes (processo_id, movimentacao_id_projudi, data_movimentacao, descricao, usuario) VALUES (?, ?, ?, ?, ?)', 
                               (processo_id, mov_id_projudi, data_mov, desc, usr))
                # Se houver uma decisão para esta movimentação, salva-a
                if mov_id_projudi in decisoes:
                    movimentacao_db_id = cursor.lastrowid
                    texto_decisao = decisoes[mov_id_projudi]
                    cursor.execute("INSERT INTO Decisoes (movimentacao_id, texto_completo) VALUES (?, ?)", (movimentacao_db_id, texto_decisao))
        
        conn.commit()
        logging.info(f"SUCESSO: Dados, {len(movimentacoes)} movimentações e {len(decisoes)} decisões salvas para o processo {numero_processo}.")

# --- Lógica de Extração ---
def extrair_dados_completos(driver):
    time.sleep(1)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    # Extração dos dados gerais (cabeçalho)
    dados_gerais = {}
    def find_value_by_label(label_text):
        try:
            label_tag = soup.find(lambda tag: tag.name == 'div' and tag.get_text(strip=True) == label_text)
            if label_tag and (value_tag := label_tag.find_next_sibling('span')):
                return ' '.join(value_tag.get_text(strip=True).split())
            return "N/A"
        except Exception: return "N/A"
    
    dados_gerais.update({
        'classe_judicial': find_value_by_label('Classe'), 'assunto': find_value_by_label('Assunto(s)'),
        'fase_processual': find_value_by_label('Fase Processual'), 'data_distribuicao': find_value_by_label('Dt. Distribuição'),
        'valor_causa': find_value_by_label('Valor da Causa')
    })

    # Extração das movimentações e decisões
    movimentacoes = []
    decisoes = {} # Dicionário para guardar {mov_id_projudi: texto_da_decisao}
    aba_principal = driver.current_window_handle
    
    linhas_mov = soup.find_all('tr', class_='filtro-entrada')
    for mov_tr in linhas_mov:
        colunas = mov_tr.find_all('td')
        if len(colunas) < 4: continue

        descricao_completa = colunas[1].get_text(separator=' ', strip=True)
        data_mov = colunas[2].text.strip()
        usuario_mov = colunas[3].text.strip()
        
        # Pega o ID da movimentação no Projudi, que está no elemento 'dropMovimentacao'
        mov_id_projudi_tag = mov_tr.find('div', class_='dropMovimentacao')
        mov_id_projudi = mov_id_projudi_tag['id_movi'] if mov_id_projudi_tag else None
        
        movimentacoes.append((mov_id_projudi, data_mov, descricao_completa, usuario_mov))

        # Se for uma decisão, extrai o texto
        if 'movi-destaque' in mov_tr.get('class', []) and mov_id_projudi:
            try:
                expand_link = mov_tr.find('img', id=lambda x: x and x.startswith('MostrarArquivos_'))
                if not expand_link: continue
                
                driver.execute_script(f"document.getElementById('{expand_link['id']}').click();")
                time.sleep(2)
                
                soup_atualizado = BeautifulSoup(driver.page_source, 'html.parser')
                linha_arquivos = soup_atualizado.find('img', id=expand_link['id']).find_parent('tr').find_next_sibling('tr')
                link_documento = linha_arquivos.find('a', href=lambda x: x and 'BuscaProcesso?PaginaAtual=6' in x)
                
                if not link_documento: continue
                
                driver.execute_script(f"window.open('{link_documento['href']}', '_blank');")
                time.sleep(2)
                
                nova_aba = [aba for aba in driver.window_handles if aba != aba_principal][0]
                driver.switch_to.window(nova_aba)
                
                soup_decisao = BeautifulSoup(driver.page_source, 'html.parser')
                texto_decisao = soup_decisao.get_text(separator='\n', strip=True)
                decisoes[mov_id_projudi] = texto_decisao
                
                driver.close()
                driver.switch_to.window(aba_principal)
                logging.info(f"Texto da decisão para a movimentação {mov_id_projudi} extraído com sucesso.")
            except Exception as e:
                logging.error(f"Falha ao extrair texto da decisão para mov {mov_id_projudi}: {e}")
                if len(driver.window_handles) > 1: driver.switch_to.window(aba_principal)

    return dados_gerais, movimentacoes, decisoes

# --- Funções do Robô (Selenium) e Orquestrador ---



def configurar_driver_logado():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # roda sem abrir janela
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--start-maximized")

    try:
        service = Service(ChromeDriverManager().install())  # automático!
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logging.info("ChromeDriver automático configurado com sucesso.")
        return driver
    except Exception as e:
        logging.error(f"Erro ao configurar o WebDriver: {e}")
        return None




def realizar_login(driver, credencial):
    usuario, senha = credencial['usuario'], credencial['senha']
    logging.info(f"Tentando realizar login com o usuário: {usuario[-4:]}")
    try:
        driver.get("https://projudi.tjgo.jus.br/LogOn?PaginaAtual=-200" )
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.ID, "login"))).send_keys(usuario)
        driver.find_element(By.ID, "senha").send_keys(senha)
        driver.find_element(By.NAME, "entrar").click()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "userMainFrame")))
        time.sleep(1)
        logging.info(f"Login com usuário {usuario[-4:]} realizado com sucesso.")
        return True
    except Exception as e:
        logging.error(f"Falha no login para o usuário {usuario[-4:]}: {e}")
        return False

def processar_lista_de_processos(lista_processos_thread, credencial):
    usuario_id = credencial['usuario']
    logging.info(f"Robô com usuário {usuario_id[-4:]} iniciando. Total de {len(lista_processos_thread)} processos.")
    
    driver = configurar_driver_logado()
    if not driver: return

    try:
        if not realizar_login(driver, credencial): return

        wait = WebDriverWait(driver, 15)
        
        for numero_processo in lista_processos_thread:
            try:
                logging.info(f"[{usuario_id[-4:]}] Buscando: {numero_processo}")
                driver.get("https://projudi.tjgo.jus.br/BuscaProcesso" )
                
                campo_busca = wait.until(EC.visibility_of_element_located((By.ID, "ProcessoNumero")))
                campo_busca.clear()
                campo_busca.send_keys(numero_processo)
                
                try:
                    limpar_filtro_btn = driver.find_element(By.XPATH, "//i[contains(@class, 'fa-circle-xmark')]")
                    limpar_filtro_btn.click()
                    logging.info(f"[{usuario_id[-4:]}] Filtro de status limpo.")
                    time.sleep(0.5)
                except NoSuchElementException:
                    logging.warning(f"[{usuario_id[-4:]}] Botão de limpar filtro não encontrado.")

                driver.find_element(By.NAME, "imgSubmeter").click()
                
                wait.until(EC.presence_of_element_located((By.ID, "span_proc_numero")))
                time.sleep(1)

                logging.info(f"[{usuario_id[-4:]}] Processo {numero_processo} encontrado. Extraindo dados completos...")
                dados_gerais, movimentacoes, decisoes = extrair_dados_completos(driver)
                salvar_dados_completos(numero_processo, dados_gerais, movimentacoes, decisoes)

            except TimeoutException:
                logging.warning(f"[{usuario_id[-4:]}] FALHA: Página de detalhes do processo {numero_processo} não carregou.")
            except Exception as e:
                logging.error(f"[{usuario_id[-4:]}] Erro crítico no CNJ {numero_processo}: {e}", exc_info=True)
                logging.info("Reiniciando o navegador devido a um erro crítico...")
                driver.quit()
                driver = configurar_driver_logado()
                if not realizar_login(driver, credencial):
                    logging.error(f"[{usuario_id[-4:]}] Não foi possível relogar. Abortando tarefas deste robô.")
                    break
    finally:
        if driver:
            driver.quit()
        logging.info(f"Robô com usuário {usuario_id[-4:]} finalizado.")

def main():
    logging.info("--- INICIANDO ROBO FINAL V17 (JURISTA) ---")
    
    if not os.path.exists(NOME_BANCO_DADOS):
        logging.error(f"ERRO FATAL: O arquivo de banco de dados '{NOME_BANCO_DADOS}' não foi encontrado.")
        logging.error("Por favor, execute o script 'cria_banco_v13.py' primeiro.")
        return

    num_credenciais = len(POOL_DE_CREDENCIAS)
    logging.info(f"Encontradas {num_credenciais} credenciais no pool.")
    
    while True:
        try:
            num_workers_str = input(f"Digite o número de robôs (threads) que deseja usar (1-{num_credenciais}): ")
            NUMERO_DE_WORKERS = int(num_workers_str)
            if 1 <= NUMERO_DE_WORKERS <= num_credenciais: break
            else: print(f"Número inválido. Use de 1 a {num_credenciais} robôs.")
        except ValueError:
            print("Entrada inválida. Por favor, digite um número.")

    try:
        df_processos = pd.read_csv(ARQUIVO_PROCESSOS_CSV, header=None, usecols=[0], sep=';', dtype=str)
        lista_total_processos = [p.strip() for p in df_processos[0].dropna().tolist()]
        if lista_total_processos and 'processo cnj' in lista_total_processos[0].lower():
            lista_total_processos.pop(0)
        if not lista_total_processos:
            logging.warning("Arquivo CSV está vazio.")
            return
        logging.info(f"Encontrados {len(lista_total_processos)} processos para dividir entre os robôs.")
    except FileNotFoundError:
        logging.error(f"ERRO: O arquivo '{ARQUIVO_PROCESSOS_CSV}' não foi encontrado.")
        return

    listas_divididas = [lista_total_processos[i::NUMERO_DE_WORKERS] for i in range(NUMERO_DE_WORKERS)]

    with ThreadPoolExecutor(max_workers=NUMERO_DE_WORKERS, thread_name_prefix='RoboJurista') as executor:
        futures = [executor.submit(processar_lista_de_processos, listas_divididas[i], POOL_DE_CREDENCIAS[i]) for i in range(NUMERO_DE_WORKERS)]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logging.error(f'Uma thread gerou uma exceção não capturada: {exc}')

    logging.info("--- FINALIZANDO ROBO FINAL ---")

if __name__ == "__main__":
    main()
