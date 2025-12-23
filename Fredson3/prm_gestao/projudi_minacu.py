# ==============================================================================
# ROBÔ PROJUDI - COMPLETO COM DOWNLOAD DE PDFs E SALVAMENTO DE HTMLs NO BANCO
# ==============================================================================

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, NoSuchWindowException
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import time
import logging
import sqlite3
import os
import random
from collections import deque, defaultdict
from webdriver_manager.chrome import ChromeDriverManager
from colorama import init, Fore, Style

# ==============================================================================
# CONFIGURAÇÃO
# ==============================================================================

init(autoreset=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

NOME_BANCO_DADOS = 'processos_v2.db'
BANCO_ESTRATEGICO = 'precatorios_estrategico.db'
ARQUIVO_PROCESSOS_CSV = 'processos_devedores.csv'
PASTA_DOWNLOADS = os.path.abspath("downloads_projudi")

POOL_DE_CREDENCIAIS = [
    {"usuario": "85413054149", "senha": "Cioni6725-"},
]

# Configuração de filtros (será preenchida no início)
CONFIG = {
    "palavras_chave": [],  # Palavras positivas
    "palavras_nega": []    # Palavras negativas
}

def configurar_filtros_e_quantidade(total_pendentes):
    """
    Configura filtros e quantidade a processar.
    Se AUTO_CONFIG=1 estiver definido no ambiente, usa PALAVRAS_POS, PALAVRAS_NEG e QTD;
    caso contrário, mantém o comportamento interativo original.
    Retorna a quantidade (int) a ser processada.
    """
    auto = os.getenv("AUTO_CONFIG", "0")
    if auto == "1":
        pos = os.getenv("PALAVRAS_POS", "").strip()
        neg = os.getenv("PALAVRAS_NEG", "").strip()
        qtd_env = os.getenv("QTD", "0")
        try:
            qtd = int(qtd_env)
        except ValueError:
            qtd = total_pendentes
        if qtd == 0:
            qtd = total_pendentes

        if pos:
            CONFIG["palavras_chave"] = [p.strip() for p in pos.split(',') if p.strip()]
            logging.info(f"✅ Palavras positivas (env): {CONFIG['palavras_chave']}")
        if neg:
            CONFIG["palavras_nega"] = [p.strip() for p in neg.split(',') if p.strip()]
            logging.info(f"✅ Palavras negativas (env): {CONFIG['palavras_nega']}")

        if not CONFIG["palavras_chave"] and not CONFIG["palavras_nega"]:
            logging.info("⚠️ Nenhum filtro configurado via env. Todas as movimentações serão processadas.")

        return qtd

    # Modo interativo (padrão)
    print(f"\n{Fore.CYAN}Você pode filtrar movimentações por palavras-chave na DESCRIÇÃO:")
    print(f"  • Palavras POSITIVAS: descrição precisa conter pelo menos uma")
    print(f"  • Palavras NEGATIVAS: descrição não pode conter nenhuma")
    print(f"  • Formato: separe por vírgula (ex: cpc,cálculo,honorários)")
    print(f"  • Deixe em branco para não filtrar")
    print(f"  • Filtro se aplica à descrição da movimentação, não ao texto da decisão{Style.RESET_ALL}\n")

    palavras_pos = input(f"{Fore.GREEN}Digite palavras-chave POSITIVAS (ou Enter para pular): {Style.RESET_ALL}").strip()
    if palavras_pos:
        CONFIG["palavras_chave"] = [p.strip() for p in palavras_pos.split(',') if p.strip()]
        print(f"{Fore.GREEN}✅ Palavras positivas: {CONFIG['palavras_chave']}{Style.RESET_ALL}")

    palavras_neg = input(f"{Fore.RED}Digite palavras-chave NEGATIVAS (ou Enter para pular): {Style.RESET_ALL}").strip()
    if palavras_neg:
        CONFIG["palavras_nega"] = [p.strip() for p in palavras_neg.split(',') if p.strip()]
        print(f"{Fore.RED}✅ Palavras negativas: {CONFIG['palavras_nega']}{Style.RESET_ALL}")

    if not CONFIG["palavras_chave"] and not CONFIG["palavras_nega"]:
        print(f"{Fore.YELLOW}⚠️ Nenhum filtro configurado. Todas as movimentações serão processadas.{Style.RESET_ALL}")

    # Pergunta quantos processos processar
    while True:
        try:
            qtd = input(f"{Fore.YELLOW}Quantos processar? (1-{total_pendentes} ou 0 para todos): {Style.RESET_ALL}")
            qtd = int(qtd)
            if qtd == 0:
                qtd = total_pendentes
                break
            elif 1 <= qtd <= total_pendentes:
                break
            else:
                print(f"{Fore.RED}Número inválido!")
        except ValueError:
            print(f"{Fore.RED}Digite um número válido!")

    return qtd

# ==============================================================================
# FUNÇÃO DE FORMATAÇÃO CNJ
# ==============================================================================

def formatar_numero_cnj(numero):
    """Formata número de processo para padrão CNJ"""
    apenas_digitos = ''.join(filter(str.isdigit, numero))
    if len(apenas_digitos) != 20:
        return numero
    return f"{apenas_digitos[0:7]}-{apenas_digitos[7:9]}.{apenas_digitos[9:13]}.{apenas_digitos[13]}.{apenas_digitos[14:16]}.{apenas_digitos[16:20]}"

# ==============================================================================
# FUNÇÕES DO BANCO ESTRATÉGICO
# ==============================================================================

def adicionar_coluna_status_se_necessario():
    """Adiciona colunas de status na tabela ExtracaoDiario"""
    with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("ALTER TABLE ExtracaoDiario ADD COLUMN status_coleta TEXT DEFAULT 'pendente'")
            cursor.execute("ALTER TABLE ExtracaoDiario ADD COLUMN data_ultima_tentativa TEXT")
            cursor.execute("ALTER TABLE ExtracaoDiario ADD COLUMN mensagem_erro TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # Colunas já existem

def buscar_processos_do_banco(quantidade):
    """Busca processos pendentes do banco"""
    with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, numero_processo_cnj 
            FROM ExtracaoDiario 
            WHERE status_coleta IS NULL OR status_coleta IN ('pendente', 'erro')
            LIMIT ?
        """, (quantidade,))
        return cursor.fetchall()

def atualizar_status_processo(processo_id, status, mensagem_erro=None):
    """Atualiza status do processo"""
    from datetime import datetime
    with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ExtracaoDiario 
            SET status_coleta = ?, data_ultima_tentativa = ?, mensagem_erro = ?
            WHERE id = ?
        """, (status, datetime.now().isoformat(), mensagem_erro, processo_id))
        conn.commit()

# ==============================================================================
# FUNÇÕES DO BANCO DE DADOS
# ==============================================================================

def salvar_dados_completos(numero_processo, dados_gerais, movimentacoes, decisoes_html):
    """Salva dados do processo, movimentações e decisões HTML no banco"""
    with sqlite3.connect(NOME_BANCO_DADOS) as conn:
        cursor = conn.cursor()
        
        # Garante que o processo exista
        cursor.execute("SELECT id FROM Processos WHERE numero_processo = ?", (numero_processo,))
        resultado = cursor.fetchone()
        if resultado:
            processo_id = resultado[0]
        else:
            cursor.execute("INSERT INTO Processos (numero_processo) VALUES (?)", (numero_processo,))
            processo_id = cursor.lastrowid

        # Atualiza dados gerais do processo
        cursor.execute("""UPDATE Processos 
                         SET data_distribuicao = ?, valor_causa = ?, data_ultima_coleta = CURRENT_TIMESTAMP 
                         WHERE id = ?""", 
                       (dados_gerais.get('data_distribuicao'), dados_gerais.get('valor_causa'), processo_id))

        # Histórico de estado
        cursor.execute("""SELECT classe_judicial, assunto, fase_processual 
                         FROM HistoricoEstado 
                         WHERE processo_id = ? 
                         ORDER BY data_coleta DESC LIMIT 1""", (processo_id,))
        ultimo_estado = cursor.fetchone()
        estado_atual = (
            dados_gerais.get('classe_judicial', 'N/A'), 
            dados_gerais.get('assunto', 'N/A'), 
            dados_gerais.get('fase_processual', 'N/A')
        )
        
        if not ultimo_estado or ultimo_estado != estado_atual:
            cursor.execute("""INSERT INTO HistoricoEstado 
                             (processo_id, classe_judicial, assunto, fase_processual) 
                             VALUES (?, ?, ?, ?)""", (processo_id, *estado_atual))

        # Limpa movimentações antigas e insere novas
        cursor.execute("DELETE FROM Movimentacoes WHERE processo_id = ?", (processo_id,))
        
        if movimentacoes:
            for mov_id_projudi, data_mov, desc, usr in movimentacoes:
                cursor.execute("""INSERT INTO Movimentacoes 
                                 (processo_id, data_movimentacao, descricao, usuario, movimentacao_id_projudi) 
                                 VALUES (?, ?, ?, ?, ?)""", 
                               (processo_id, data_mov, desc, usr, mov_id_projudi))
                
                # Se houver decisão HTML para esta movimentação, salva
                if mov_id_projudi in decisoes_html:
                    movimentacao_db_id = cursor.lastrowid
                    texto_decisao = decisoes_html[mov_id_projudi]
                    cursor.execute("INSERT INTO Decisoes (movimentacao_id, texto_completo) VALUES (?, ?)", 
                                  (movimentacao_db_id, texto_decisao))
        
        conn.commit()
        logging.info(f"✅ SUCESSO: {len(movimentacoes)} movimentações e {len(decisoes_html)} decisões HTML salvas para {numero_processo}")

# ==============================================================================
# FUNÇÕES DE EXTRAÇÃO
# ==============================================================================

def filtrar_movimentacao(descricao_movimentacao):
    """Filtra movimentação baseado em palavras-chave positivas e negativas na DESCRIÇÃO"""
    if not CONFIG["palavras_chave"] and not CONFIG["palavras_nega"]:
        return True  # Sem filtros, aceita tudo
    
    desc_lower = descricao_movimentacao.lower()
    
    # Se tem palavras positivas, precisa ter pelo menos uma
    if CONFIG["palavras_chave"]:
        tem_positiva = any(palavra.lower() in desc_lower for palavra in CONFIG["palavras_chave"])
        if not tem_positiva:
            return False
    
    # Se tem palavras negativas, não pode ter nenhuma
    if CONFIG["palavras_nega"]:
        tem_negativa = any(palavra.lower() in desc_lower for palavra in CONFIG["palavras_nega"])
        if tem_negativa:
            return False
    
    return True

def extrair_dados_gerais(soup):
    """Extrai dados gerais do cabeçalho do processo"""
    dados_gerais = {}
    
    def find_value_by_label(label_text):
        try:
            label_tag = soup.find(lambda tag: tag.name == 'div' and tag.get_text(strip=True) == label_text)
            if label_tag and (value_tag := label_tag.find_next_sibling('span')):
                return ' '.join(value_tag.get_text(strip=True).split())
            return "N/A"
        except Exception:
            return "N/A"
    
    dados_gerais.update({
        'classe_judicial': find_value_by_label('Classe'),
        'assunto': find_value_by_label('Assunto(s)'),
        'fase_processual': find_value_by_label('Fase Processual'),
        'data_distribuicao': find_value_by_label('Dt. Distribuição'),
        'valor_causa': find_value_by_label('Valor da Causa')
    })
    
    return dados_gerais

def processar_movimentacoes_e_arquivos(driver, wait, numero_processo):
    """Processa movimentações, baixa PDFs e extrai HTMLs"""
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    
    movimentacoes = []
    decisoes_html = {}
    pdfs_baixados = 0
    aba_principal = driver.current_window_handle
    
    linhas_mov = soup.find_all('tr', class_='filtro-entrada')
    logging.info(f"📋 Encontradas {len(linhas_mov)} movimentações para processar")
    
    for idx, mov_tr in enumerate(linhas_mov, 1):
        colunas = mov_tr.find_all('td')
        if len(colunas) < 4:
            continue

        # Extrai dados da movimentação
        descricao_completa = colunas[1].get_text(separator=' ', strip=True)
        data_mov = colunas[2].text.strip()
        usuario_mov = colunas[3].text.strip()
        
        mov_id_projudi_tag = mov_tr.find('div', class_='dropMovimentacao')
        mov_id_projudi = mov_id_projudi_tag['id_movi'] if mov_id_projudi_tag else None
        
        movimentacoes.append((mov_id_projudi, data_mov, descricao_completa, usuario_mov))

        # Aplica filtro na DESCRIÇÃO da movimentação
        if not filtrar_movimentacao(descricao_completa):
            logging.info(f"⏭️ Movimentação {idx} ignorada pelo filtro (descrição não corresponde)")
            continue

        # Processa arquivos anexados (PDFs e HTMLs)
        try:
            # Tenta expandir a movimentação
            expand_link = mov_tr.find('img', id=lambda x: x and x.startswith('MostrarArquivos_'))
            if not expand_link:
                continue
            
            # Clica para expandir
            ActionChains(driver).move_to_element(
                driver.find_element(By.ID, expand_link['id'])
            ).click().perform()
            time.sleep(0.5)  # Reduzido de 1s para 0.5s
            
            # Atualiza soup
            soup_atualizado = BeautifulSoup(driver.page_source, 'html.parser')
            linha_arquivos = soup_atualizado.find('img', id=expand_link['id']).find_parent('tr').find_next_sibling('tr')
            
            if not linha_arquivos:
                continue
            
            # Procura todos os links de arquivos
            links_arquivos = linha_arquivos.find_all('a', href=True)
            
            for link in links_arquivos:
                href = link['href']
                nome_arquivo = link.get('title', 'arquivo')
                
                # Verifica se é PDF ou HTML
                if '.pdf' in nome_arquivo.lower() or 'pdf' in href.lower():
                    # É PDF - faz download
                    logging.info(f"📄 Baixando PDF: {nome_arquivo}")
                    try:
                        driver.find_element(By.XPATH, f"//a[@href='{href}']").click()
                        time.sleep(1.5)  # Reduzido de 3s para 1.5s
                        pdfs_baixados += 1
                    except Exception as e:
                        logging.warning(f"⚠️ Erro ao baixar PDF: {e}")
                
                elif 'BuscaProcesso?PaginaAtual=6' in href:
                    # É HTML (decisão) - extrai texto
                    logging.info(f"📝 Extraindo HTML (decisão): mov {mov_id_projudi}")
                    try:
                        driver.execute_script(f"window.open('{href}', '_blank');")
                        time.sleep(1)  # Reduzido de 2s para 1s
                        
                        nova_aba = [aba for aba in driver.window_handles if aba != aba_principal][0]
                        driver.switch_to.window(nova_aba)
                        
                        soup_decisao = BeautifulSoup(driver.page_source, 'html.parser')
                        texto_decisao = soup_decisao.get_text(separator='\n', strip=True)
                        decisoes_html[mov_id_projudi] = texto_decisao
                        
                        driver.close()
                        driver.switch_to.window(aba_principal)
                        logging.info(f"✅ Decisão HTML extraída e salva")
                    except Exception as e:
                        logging.error(f"❌ Erro ao extrair HTML: {e}")
                        if len(driver.window_handles) > 1:
                            driver.switch_to.window(aba_principal)
            
            # Recolhe a movimentação
            ActionChains(driver).move_to_element(
                driver.find_element(By.ID, expand_link['id'])
            ).click().perform()
            time.sleep(0.3)  # Reduzido de 0.5s para 0.3s
            
        except Exception as e:
            logging.warning(f"⚠️ Erro na movimentação {idx}: {e}")
            continue
    
    logging.info(f"✅ Processamento concluído: {pdfs_baixados} PDFs baixados, {len(decisoes_html)} HTMLs extraídos")
    return movimentacoes, decisoes_html

# ==============================================================================
# FUNÇÕES DE NAVEGAÇÃO E LOGIN
# ==============================================================================

def configurar_driver():
    """Configura o Chrome com opções de download"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    os.makedirs(PASTA_DOWNLOADS, exist_ok=True)
    
    prefs = {
        "download.default_directory": PASTA_DOWNLOADS,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def realizar_login(driver, credencial):
    """Faz login no PROJUDI"""
    usuario, senha = credencial['usuario'], credencial['senha']
    logging.info(f"🔐 Login com usuário: {usuario[-4:]}")
    
    try:
        driver.get("https://projudi.tjgo.jus.br/LogOn?PaginaAtual=-200")
        wait = WebDriverWait(driver, 20)
        
        wait.until(EC.presence_of_element_located((By.ID, "login"))).send_keys(usuario)
        driver.find_element(By.ID, "senha").send_keys(senha)
        driver.find_element(By.NAME, "entrar").click()
        wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "userMainFrame")))
        
        logging.info(f"✅ Login realizado com sucesso")
        return True
    except Exception as e:
        logging.error(f"❌ Falha no login: {e}")
        return False

def solicitar_acesso_processo(driver, wait, numero_processo):
    """
    Solicita acesso ao processo
    Retorna: ('sucesso', True) | ('rate_limit', False) | ('erro', False)
    """
    logging.info(f"🔓 Solicitando acesso ao processo...")
    
    try:
        driver.get("https://projudi.tjgo.jus.br/DescartarPendenciaProcesso?PaginaAtual=8")
        time.sleep(2)
        
        try:
            dialogo = WebDriverWait(driver, 7).until(EC.visibility_of_element_located((By.ID, "dialog")))
            botao_ok = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'OK')]")))
            botao_ok.click()
            logging.info(f"✅ Acesso concedido!")
            
            # Volta para o processo
            driver.get("https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4")
            campo_busca = wait.until(EC.visibility_of_element_located((By.ID, "ProcessoNumero")))
            campo_busca.clear()
            campo_busca.send_keys(numero_processo)
            driver.find_element(By.NAME, "imaLimparProcessoStatus").click()
            driver.find_element(By.NAME, "imgSubmeter").click()
            time.sleep(2)
            logging.info(f"✅ De volta ao processo")
            
            return ('sucesso', True)
            
        except TimeoutException:
            src = driver.page_source
            if "Usuário tem que esperar 24h" in src or "usuário já tem acesso" in src:
                logging.info(f"✅ Acesso já existente")
                
                # Volta para o processo
                driver.get("https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4")
                campo_busca = wait.until(EC.visibility_of_element_located((By.ID, "ProcessoNumero")))
                campo_busca.clear()
                campo_busca.send_keys(numero_processo)
                driver.find_element(By.NAME, "imaLimparProcessoStatus").click()
                driver.find_element(By.NAME, "imgSubmeter").click()
                time.sleep(2)
                logging.info(f"✅ De volta ao processo")
                
                return ('sucesso', True)
                
            elif "Só é permitido" in src or "atingiu o limite" in src:
                logging.warning(f"🚫 RATE LIMIT atingido! Precisa reiniciar navegador.")
                return ('rate_limit', False)
            else:
                logging.error(f"❌ Estado desconhecido ao solicitar acesso")
                return ('erro', False)
                
    except Exception as e:
        logging.error(f"❌ Erro ao solicitar acesso: {e}")
        return ('erro', False)

# ==============================================================================
# FUNÇÃO PRINCIPAL DE PROCESSAMENTO
# ==============================================================================

def verificar_filtro_antes_acesso(driver, numero_processo):
    """
    Verifica se o processo passa no filtro ANTES de pedir acesso
    Retorna: True se passa no filtro, False se não passa
    """
    if not CONFIG["palavras_chave"] and not CONFIG["palavras_nega"]:
        return True  # Sem filtros, aceita tudo
    
    try:
        logging.info(f"🔍 Verificando filtro antes de pedir acesso...")
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        linhas_mov = soup.find_all('tr', class_='filtro-entrada')
        
        # Verifica se alguma movimentação passa no filtro
        for mov_tr in linhas_mov:
            colunas = mov_tr.find_all('td')
            if len(colunas) >= 2:
                descricao = colunas[1].get_text(separator=' ', strip=True)
                if filtrar_movimentacao(descricao):
                    logging.info(f"✅ Processo tem movimentações que passam no filtro")
                    return True
        
        logging.info(f"⏭️ Nenhuma movimentação passa no filtro. Pulando processo.")
        return False
        
    except Exception as e:
        logging.warning(f"⚠️ Erro ao verificar filtro: {e}. Processando por segurança.")
        return True  # Em caso de erro, processa

def processar_processo(driver, wait, numero_processo, devedor):
    """
    Processa um processo completo
    Retorna: ('sucesso', True) | ('rate_limit', False) | ('filtrado', False) | ('erro', False)
    """
    logging.info(f"\n{'='*80}")
    logging.info(f"🔍 PROCESSO: {numero_processo} - {devedor}")
    logging.info(f"{'='*80}\n")
    
    try:
        # Busca o processo
        driver.get("https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4")
        campo_busca = wait.until(EC.visibility_of_element_located((By.ID, "ProcessoNumero")))
        campo_busca.clear()
        campo_busca.send_keys(numero_processo)
        driver.find_element(By.NAME, "imaLimparProcessoStatus").click()
        driver.find_element(By.NAME, "imgSubmeter").click()
        time.sleep(2)

        # Em modo MINACU, assumimos acesso prévio e pulamos a solicitação
        assume_access = os.getenv("MINACU_ASSUME_ACCESS", "0") == "1"
        if not assume_access:
            # VERIFICA FILTRO ANTES DE PEDIR ACESSO
            if not verificar_filtro_antes_acesso(driver, numero_processo):
                logging.info(f"⏭️ Processo não passa no filtro. Pulando sem pedir acesso.")
                return ('filtrado', False)

            # Solicita acesso
            status, sucesso = solicitar_acesso_processo(driver, wait, numero_processo)
            if not sucesso:
                if status == 'rate_limit':
                    logging.error(f"🚫 Rate limit atingido")
                    return ('rate_limit', False)
                else:
                    logging.error(f"❌ Não foi possível obter acesso ao processo")
                    return ('erro', False)
        
        # Extrai dados gerais
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        dados_gerais = extrair_dados_gerais(soup)
        
        # Processa movimentações e arquivos
        movimentacoes, decisoes_html = processar_movimentacoes_e_arquivos(driver, wait, numero_processo)
        
        # Salva no banco
        salvar_dados_completos(numero_processo, dados_gerais, movimentacoes, decisoes_html)
        
        return ('sucesso', True)
        
    except Exception as e:
        logging.error(f"❌ Erro crítico no processo {numero_processo}: {e}")
        import traceback
        traceback.print_exc()
        
        # Salva log completo do erro
        try:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            erro_dir = f"logs_erro/{numero_processo}_{timestamp}"
            os.makedirs(erro_dir, exist_ok=True)
            
            # Salva screenshot
            screenshot_path = f"{erro_dir}/screenshot_erro.png"
            driver.save_screenshot(screenshot_path)
            logging.info(f"📸 Screenshot salvo em: {screenshot_path}")
            
            # Salva HTML da página
            html_path = f"{erro_dir}/pagina_erro.html"
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logging.info(f"📄 HTML salvo em: {html_path}")
            
            # Salva traceback completo
            traceback_path = f"{erro_dir}/traceback.txt"
            with open(traceback_path, 'w', encoding='utf-8') as f:
                f.write(f"Processo: {numero_processo}\n")
                f.write(f"Erro: {str(e)}\n\n")
                f.write(traceback.format_exc())
            logging.info(f"📝 Traceback salvo em: {traceback_path}")
            
            logging.info(f"✅ Logs de erro salvos em: {erro_dir}")
        except Exception as log_err:
            logging.error(f"⚠️ Não foi possível salvar logs de erro: {log_err}")
        
        # Se a janela foi fechada, sinaliza para reiniciar o navegador
        if isinstance(e, NoSuchWindowException):
            return ('janela_fechada', False)
        
        return ('erro', False)

# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print(f"\n{Style.BRIGHT}{'='*80}")
    print(f"{Fore.CYAN}🤖 ROBÔ PROJUDI - COMPLETO v2.0")
    print(f"{'='*80}{Style.RESET_ALL}\n")
    
    # Verifica se existe o arquivo CSV
    if os.path.exists(ARQUIVO_PROCESSOS_CSV):
        # MODO CSV: carrega processos do arquivo
        print(f"{Fore.GREEN}📄 Arquivo CSV encontrado: {ARQUIVO_PROCESSOS_CSV}")
        print(f"{Fore.GREEN}Carregando processos do CSV...{Style.RESET_ALL}\n")
        
        df = pd.read_csv(ARQUIVO_PROCESSOS_CSV)
        total_pendentes = len(df)
        
        print(f"{Fore.CYAN}📊 Total de processos no CSV: {total_pendentes}")
        
        # Pergunta sobre filtros de palavras-chave
        print(f"\n{Fore.YELLOW}{'='*80}")
        print(f"CONFIGURAÇÃO DE FILTROS DE PALAVRAS-CHAVE")
        print(f"{'='*80}{Style.RESET_ALL}")
        # Configuração de filtros e quantidade (env ou interativo)
        qtd = configurar_filtros_e_quantidade(total_pendentes)
        
        # Carrega processos do CSV
        processos_selecionados = []
        for _, row in df.head(qtd).iterrows():
            # Usa exatamente o número como está no CSV, sem exigir 20 dígitos
            # Suporta cabeçalhos comuns: 'numero_processo', 'numero_processo_cnj', 'processo cnj'
            numero_raw = row.get('numero_processo', row.get('numero_processo_cnj', row.get('processo cnj', '')))
            numero_csv = str(numero_raw).strip()

            processos_selecionados.append({
                "id": None,  # CSV não tem ID do banco
                "numero": numero_csv,  # mantém como está no CSV (sem reformatar)
                "devedor": str(row.get('devedor', 'N/A'))
            })
        
        print(f"{Fore.GREEN}✅ {len(processos_selecionados)} processo(s) carregado(s) do CSV!\n")
        
    else:
        # MODO BANCO: carrega processos do banco de dados
        print(f"{Fore.YELLOW}📄 Arquivo CSV não encontrado: {ARQUIVO_PROCESSOS_CSV}")
        print(f"{Fore.CYAN}Carregando processos do banco de dados...{Style.RESET_ALL}\n")
        
        # Adiciona colunas de status se necessário
        adicionar_coluna_status_se_necessario()
        
        # Pergunta quantos processos processar
        with sqlite3.connect(BANCO_ESTRATEGICO) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM ExtracaoDiario WHERE status_coleta IS NULL OR status_coleta IN ('pendente', 'erro')")
            total_pendentes = cursor.fetchone()[0]
        
        print(f"{Fore.CYAN}📊 Total de processos pendentes: {total_pendentes}")
        
        # Pergunta sobre filtros de palavras-chave
        print(f"\n{Fore.YELLOW}{'='*80}")
        print(f"CONFIGURAÇÃO DE FILTROS DE PALAVRAS-CHAVE")
        print(f"{'='*80}{Style.RESET_ALL}")
        # Configuração de filtros e quantidade (env ou interativo)
        qtd = configurar_filtros_e_quantidade(total_pendentes)
        
        # Busca processos do banco
        processos_db = buscar_processos_do_banco(qtd)
        processos_selecionados = []
        for processo_id, numero_cnj in processos_db:
            processos_selecionados.append({
                "id": processo_id,
                "numero": formatar_numero_cnj(numero_cnj),
                "devedor": "N/A"
            })
        
        print(f"{Fore.GREEN}✅ {len(processos_selecionados)} processo(s) carregado(s) do banco!\n")
    
    # Configura driver e faz login
    credencial = POOL_DE_CREDENCIAIS[0]
    driver = configurar_driver()
    wait = WebDriverWait(driver, 20)
    
    if not realizar_login(driver, credencial):
        print(f"{Fore.RED}❌ Falha no login. Encerrando.")
        return
    
    # Fila com re-tentativas para não perder processos
    sucessos = 0
    filtrados = 0
    erros = 0
    MAX_TENTATIVAS = 3
    fila = deque(processos_selecionados)
    tentativas = defaultdict(int)
    total_alvos = len(processos_selecionados)
    finalizados = 0

    while fila and finalizados < total_alvos:
        processo = fila.popleft()
        chave_proc = processo['id'] if processo['id'] is not None else processo['numero']
        tentativa_atual = tentativas[chave_proc] + 1

        print(f"\n{Fore.CYAN}▶️  Processo {finalizados + 1}/{total_alvos} (tentativa {tentativa_atual}/{MAX_TENTATIVAS})")

        # Marca como coletando
        if processo['id'] is not None:
            atualizar_status_processo(processo['id'], 'coletando')

        status, resultado = processar_processo(driver, wait, processo['numero'], processo['devedor'])

        if status == 'sucesso':
            if processo['id'] is not None:
                atualizar_status_processo(processo['id'], 'sucesso')
            sucessos += 1
            finalizados += 1
            continue

        if status == 'filtrado':
            if processo['id'] is not None:
                atualizar_status_processo(processo['id'], 'filtrado')
            filtrados += 1
            finalizados += 1
            continue

        if status == 'rate_limit':
            # REINICIA NAVEGADOR PARA ACESSO ILIMITADO E TENTA NOVAMENTE
            logging.warning(f"\n{'='*80}")
            logging.warning(f"🔄 RATE LIMIT ATINGIDO! Reiniciando navegador...")
            logging.warning(f"{'='*80}\n")
            try:
                driver.quit()
                time.sleep(2)
            except:
                pass
            driver = configurar_driver()
            wait = WebDriverWait(driver, 20)
            if not realizar_login(driver, credencial):
                logging.error(f"❌ Falha no login após reiniciar. Encerrando.")
                # Marca erro definitivo
                if processo['id'] is not None:
                    atualizar_status_processo(processo['id'], 'erro', 'falha_login_pos_rate_limit')
                erros += 1
                finalizados += 1
                break
            logging.info(f"✅ Navegador reiniciado e login realizado!")
            logging.info(f"🔄 Tentando processar novamente: {processo['numero']}\n")
            status, resultado = processar_processo(driver, wait, processo['numero'], processo['devedor'])
            # Após nova tentativa, cai para lógica comum de finalizar ou re-enfileirar

        if status == 'janela_fechada':
            logging.warning("🪟 Janela fechada detectada. Processo será re-enfileirado.")

        # Se chegou aqui, não foi sucesso nem filtrado
        if tentativas[chave_proc] + 1 < MAX_TENTATIVAS:
            tentativas[chave_proc] += 1
            # Atualiza status para pendente no banco para refletir re-enfileiramento
            if processo['id'] is not None:
                atualizar_status_processo(processo['id'], 'pendente')
            fila.append(processo)
            logging.info(f"🔁 Processo {processo['numero']} re-enfileirado (tentativa {tentativas[chave_proc]}/{MAX_TENTATIVAS}).")
        else:
            # Atingiu o máximo de tentativas: marca erro definitivo
            if processo['id'] is not None:
                atualizar_status_processo(processo['id'], 'erro', str(resultado))
            erros += 1
            finalizados += 1
    
    # Resultado final
    print(f"\n{Style.BRIGHT}{'='*80}")
    print(f"{Fore.GREEN}✅ CONCLUÍDO!")
    print(f"  • Sucessos: {sucessos}/{total_alvos}")
    print(f"  • Filtrados: {filtrados}/{total_alvos}")
    print(f"  • Erros: {erros}/{total_alvos}")
    print(f"{'='*80}{Style.RESET_ALL}\n")
    
    # Mantém navegador aberto
    print(f"{Fore.CYAN}Navegador permanecerá aberto. Pressione Ctrl+C para fechar.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Fechando navegador...")
        driver.quit()
        print(f"{Fore.GREEN}Navegador fechado!")

if __name__ == "__main__":
    main()