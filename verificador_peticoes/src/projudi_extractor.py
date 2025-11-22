import pandas as pd
from bs4 import BeautifulSoup
import time
import logging
import os
import asyncio
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

class ProjudiExtractor:
    """Classe para extração de dados do Projudi TJGO"""
    
    def __init__(self, logger=None, batch_id: str = None):
        self.driver = None
        self.wait = None
        if logger is not None:
            self.logger = logger
        elif batch_id:
            self.logger = logging.getLogger(f"exec.{batch_id}.projudi_extractor")
        else:
            self.logger = logging.getLogger(__name__)
        self.batch_id = batch_id or ""
    
    def setup_logging(self):
        return
    
    def configurar_driver(self, headless=True):
        try:
            if os.name == 'nt':
                try:
                    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
                except Exception:
                    pass
            self.logger.info(f"Inicializando navegador headless={headless}")
            self._pw = sync_playwright().start()
            self.browser = self._pw.chromium.launch(headless=headless, args=["--start-maximized"]) 
            self.context = self.browser.new_context(ignore_https_errors=True)
            self.page = self.context.new_page()
            self.logger.info("Browser Playwright configurado")
            try:
                self.snapshot("browser_configurado")
            except Exception:
                pass
            return True
        except Exception as e:
            self.logger.error(f"Erro ao configurar Playwright: {e}")
            return False

    def realizar_login(self, usuario: str, senha: str) -> bool:
        try:
            self.logger.info("Abrindo página de login")
            self.page.goto("https://projudi.tjgo.jus.br/LogOn?PaginaAtual=-200", wait_until="domcontentloaded")
            try:
                self.page.wait_for_selector("#login", timeout=30000)
                self.page.wait_for_selector("#senha", timeout=30000)
            except PWTimeoutError:
                self.logger.error("Campos de login não disponíveis")
                return False
            self.logger.info("Preenchendo credenciais")
            self.page.locator("#login").fill(usuario)
            self.page.locator("#senha").fill(senha)
            self.logger.info("Submetendo login")
            self.page.locator("[name='entrar']").click()
            try:
                self.page.wait_for_selector("iframe[name='userMainFrame']", timeout=20000)
                self.logger.info("Frame principal disponível")
            except PWTimeoutError:
                pass
            time.sleep(1)
            self.logger.info("Login realizado")
            try:
                self.snapshot("login_ok")
            except Exception:
                pass
            return True
        except Exception as e:
            self.logger.error(f"Falha no login: {e}")
            try:
                self.snapshot("login_erro")
            except Exception:
                pass
            return False

    def pesquisar_processo(self, numero_processo: str) -> bool:
        try:
            self.logger.info(f"Abrindo busca do processo {numero_processo}")
            self.page.goto("https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4", wait_until="domcontentloaded")
            campo = self.page.locator("#ProcessoNumero")
            campo.fill("")
            campo.fill(numero_processo)
            self.logger.info("Campo preenchido")
            try:
                self.page.locator("[name='imaLimparProcessoStatus']").click()
            except Exception:
                pass
            self.page.locator("[name='imgSubmeter']").click()
            self.logger.info("Submetendo busca")
            self.page.wait_for_selector("#span_proc_numero", timeout=20000)
            self.logger.info("Página do processo carregada")
            time.sleep(1)
            try:
                self.snapshot(f"proc_{numero_processo}")
            except Exception:
                pass
            return True
        except Exception as e:
            self.logger.error(f"Erro ao pesquisar processo {numero_processo}: {e}")
            return False
    
    def extrair_documentos_processo(self, numero_processo: str) -> list:
        """
        Extrai lista de documentos/petições de um processo
        Retorna lista com informações dos documentos
        """
        documentos = []
        
        try:
            # Acessa página de busca do processo
            self.page.goto("https://projudi.tjgo.jus.br/BuscaProcesso", wait_until="domcontentloaded")
            self.logger.info("Página de busca aberta")
            
            # Aguarda e preenche campo de busca
            campo = self.page.locator("#ProcessoNumero")
            campo.fill("")
            campo.fill(numero_processo)
            self.logger.info("Número do processo preenchido")
            
            # Clica no botão de busca
            self.page.locator("[name='imgSubmeter']").click()
            self.logger.info("Busca submetida")
            
            # Aguarda carregar resultado
            self.page.wait_for_selector("#span_proc_numero", timeout=20000)
            self.logger.info("Resultado carregado")
            
            time.sleep(2)  # Pequena pausa para carregar completamente
            try:
                self.snapshot("resultado_carregado")
            except Exception:
                pass
            
            # Expande cada movimentação e coleta os anexos (padrão do Projudi)
            anexos = self._listar_anexos()
            self.logger.info(f"Anexos coletados: {len(anexos)}")
            for doc in anexos:
                documentos.append({
                    'numero_processo': numero_processo,
                    'nome_documento': doc.get('nome', ''),
                    'id_documento': doc.get('id', ''),
                    'data_protocolo': doc.get('data', ''),
                    'tipo_documento': doc.get('tipo', ''),
                    'link_download': doc.get('link', '')
                })
            
            self.logger.info(f"Encontrados {len(documentos)} documentos no processo {numero_processo}")
            
        except PWTimeoutError:
            self.logger.error(f"Timeout ao buscar processo {numero_processo}")
        except Exception as e:
            self.logger.error(f"Erro ao extrair documentos do processo {numero_processo}: {e}")
        
        return documentos
    
    def _listar_anexos(self) -> list:
        documentos = []
        try:
            rows = self.page.locator("tr.filtro-entrada")
            total = rows.count()
            self.logger.info(f"Movimentações encontradas: {total}")
            for i in range(total):
                row = rows.nth(i)
                expand = row.locator("img[id^='MostrarArquivos_']")
                if expand.count() == 0:
                    continue
                try:
                    aid = expand.first.get_attribute("id")
                    self.logger.info(f"Expandindo anexos: {aid}")
                except Exception:
                    pass
                expand.first.click()
                time.sleep(0.5)
                anchors = row.locator("xpath=following-sibling::tr[1]//a[@href]")
                count = anchors.count()
                self.logger.info(f"Arquivos anexos: {count}")
                for j in range(count):
                    a = anchors.nth(j)
                    href = a.get_attribute("href")
                    title = a.get_attribute("title") or a.inner_text()
                    info = self._parse_nome_documento(title or "")
                    if not info.get('id'):
                        try:
                            import re
                            ms = re.findall(r'_(\d+)_(\d+)_', f"{title} {href}")
                            if ms:
                                u = ms[-1]
                                info['id'] = f"_{u[0]}_{u[1]}_"
                        except Exception:
                            pass
                    info['link'] = href or ""
                    documentos.append(info)
                expand.first.click()
                time.sleep(0.3)
        except Exception as e:
            self.logger.error(f"Erro ao listar anexos: {e}")
        return documentos
    
    def _parse_nome_documento(self, nome: str, id_sistema: str = None) -> dict:
        """
        Analisa nome do documento e extrai informações
        Ex: id_483887823_doc._00_6000929_09_2024_8_09_0051_15553_56747_cumprimento_.pdf
        """
        try:
            # Remove espaços extras
            nome = nome.strip()
            
            id_info = {
                'nome': nome,
                'id': '',
                'data': '',
                'tipo': ''
            }
            
            # SE TEMOS ID DO SISTEMA, PROCURAR ESPECIFICAMENTE POR ELE
            if id_sistema:
                import re
                id_sistema_limpo = id_sistema.strip('_')
                
                # Procurar variações do ID do sistema (com/sem underscore final)
                pattern_com_underscore = re.escape(id_sistema)
                pattern_sem_underscore = re.escape(f"_{id_sistema_limpo}")
                pattern_final = re.escape(f"_{id_sistema_limpo}_")
                
                # Tentar encontrar qualquer variação
                for pattern in [pattern_com_underscore, pattern_sem_underscore, pattern_final]:
                    match = re.search(pattern, nome)
                    if match:
                        id_info['id'] = match.group()
                        break
                
                # Se não encontrou com patterns exatos, procurar por partes
                if not id_info['id']:
                    partes_id = id_sistema_limpo.split('_')
                    if len(partes_id) == 2:
                        # Encontrar todos os _x_x_ e verificar qual contém as partes do ID
                        todos_matches = re.findall(r'_\d+_\d+_?', nome)
                        for match in todos_matches:
                            if partes_id[0] in match and partes_id[1] in match:
                                id_info['id'] = match
                                break
            
            # SE NÃO TEMOS ID DO SISTEMA OU NÃO ENCONTROU, USAR LÓGICA ORIGINAL MELHORADA
            if not id_info['id']:
                import re
                
                # Encontrar todos os padrões _digitos_digitos_ (com ou sem underscore final)
                todos_padroes = re.findall(r'_\d+_\d+_?', nome)
                
                # Selecionar o mais provável (último com estrutura válida)
                for padrao in reversed(todos_padroes):
                    parte_numerica = padrao.strip('_')
                    partes = parte_numerica.split('_')
                    
                    # Critérios para ID válido: ambos os números devem ter 3+ dígitos
                    if len(partes) == 2 and len(partes[0]) >= 3 and len(partes[1]) >= 3:
                        # Priorizar números que não parecem datas
                        if not (partes[0].startswith('0') and len(partes[0]) == 4):  # Não é ano
                            id_info['id'] = padrao
                            break
            
            # Tenta extrair data
            padrao_data = r'(\d{2})\.(\d{2})\.(\d{4})'
            data_match = re.search(padrao_data, nome)
            if data_match:
                id_info['data'] = data_match.group(0)
            
            # Tenta identificar tipo de documento
            tipos_comuns = ['manifestação', 'petição', 'certidão', 'despacho', 'decisão', 'cumprimento']
            nome_lower = nome.lower()
            for tipo in tipos_comuns:
                if tipo in nome_lower:
                    id_info['tipo'] = tipo.capitalize()
                    break
            
            return id_info
            
        except Exception as e:
            self.logger.error(f"Erro ao parsear nome do documento '{nome}': {e}")
            return {'nome': nome, 'id': '', 'data': '', 'tipo': ''}

    def _normalizar_id(self, texto: str) -> str:
        try:
            if not texto:
                return ''
            import re
            ms = re.findall(r'_(\d+)_(\d+)_', texto)
            if ms:
                u = ms[-1]
                return f"{u[0]}_{u[1]}"
            # Se vier sem sublinhados externos, tenta padrão sem o último underscore
            ms2 = re.findall(r'(\d+)_(\d+)', texto)
            if ms2:
                u = ms2[-1]
                return f"{u[0]}_{u[1]}"
            return ''
        except Exception:
            return ''

    def _parece_nome_documento(self, texto: str) -> bool:
        """
        Verifica se o texto parece ser um nome de documento
        baseado em padrões comuns
        """
        # Verifica se tem extensão de arquivo
        if not any(ext in texto.lower() for ext in ['.pdf', '.doc', '.docx']):
            return False
        
        # Verifica se tem números que parecem identificadores
        import re
        if not re.search(r'_\d+_\d+_', texto):
            return False
        
        return True
    
    def verificar_protocolizacao(self, numero_processo: str, identificador_peticao: str) -> dict:
        """
        Verifica se uma petição específica foi protocolizada no processo
        
        Args:
            numero_processo: Número completo do processo
            identificador_peticao: Identificador único da petição (ex: _3551_56791_)
        
        Returns:
            Dict com resultado da verificação
        """
        resultado = {
            'processo': numero_processo,
            'identificador_peticao': identificador_peticao,
            'encontrado': False,
            'nome_documento': '',
            'data_protocolo': '',
            'tipo_documento': '',
            'link_download': '',
            'mensagem': ''
        }
        
        try:
            self.logger.info(f"Verificando protocolização da petição {identificador_peticao} no processo {numero_processo}")
            
            if not self.pesquisar_processo(numero_processo):
                resultado['mensagem'] = 'Falha ao abrir o processo'
                return resultado
            if identificador_peticao:
                alvo_norm = self._normalizar_id(identificador_peticao)
                doc_match = self._buscar_anexo_por_id(identificador_peticao)
                if doc_match:
                    id_norm = self._normalizar_id(doc_match.get('id', '')) or self._normalizar_id(doc_match.get('nome','')) or self._normalizar_id(doc_match.get('link',''))
                    if id_norm and id_norm == alvo_norm:
                        resultado.update({
                            'encontrado': True,
                            'nome_documento': doc_match.get('nome', ''),
                            'data_protocolo': doc_match.get('data', ''),
                            'tipo_documento': doc_match.get('tipo', ''),
                            'link_download': doc_match.get('link', ''),
                            'mensagem': f'Petição protocolizada (match de identificador): {doc_match.get("nome","")}'
                        })
                        self.logger.info("Identificador exato encontrado (early stop)")
                        return resultado

            documentos = self.extrair_documentos_processo(numero_processo)
            self.logger.info(f"Documentos coletados: {len(documentos)}")
            
            if not documentos:
                resultado['mensagem'] = 'Nenhum documento encontrado no processo'
                return resultado
            
            alvo_norm = self._normalizar_id(identificador_peticao)
            for doc in documentos:
                id_doc = doc.get('id_documento', '')
                id_norm = self._normalizar_id(id_doc) or self._normalizar_id(doc.get('nome_documento','')) or self._normalizar_id(doc.get('link_download',''))
                if id_norm and id_norm == alvo_norm:
                    resultado.update({
                        'encontrado': True,
                        'nome_documento': doc['nome_documento'],
                        'data_protocolo': doc['data_protocolo'],
                        'tipo_documento': doc['tipo_documento'],
                        'link_download': doc['link_download'],
                        'mensagem': f'Petição protocolizada (match de identificador): {doc["nome_documento"]}'
                    })
                    self.logger.info("Identificador exato encontrado")
                    break
            
            if not resultado['encontrado']:
                resultado['mensagem'] = f'Petição com identificador {identificador_peticao} não encontrada no processo'
                
                # Sugere documentos similares
                sugestoes = [doc['nome_documento'] for doc in documentos if alvo_norm and (alvo_norm in (self._normalizar_id(doc['nome_documento']) or '') or alvo_norm in (self._normalizar_id(doc.get('link_download','')) or ''))]
                if sugestoes:
                    resultado['mensagem'] += f'. Documentos similares encontrados: {", ".join(sugestoes[:3])}'
            
            self.logger.info(f"Resultado da verificação: {resultado['mensagem']}")
            try:
                self.snapshot("verificacao")
            except Exception:
                pass
            
        except Exception as e:
            self.logger.error(f"Erro ao verificar protocolização: {e}")
            resultado['mensagem'] = f'Erro ao verificar: {str(e)}'
            try:
                self.snapshot("erro_verificacao")
            except Exception:
                pass
        
        return resultado

    def _buscar_anexo_por_id(self, target_id: str) -> dict:
        try:
            rows = self.page.locator("tr.filtro-entrada")
            total = rows.count()
            for i in range(total):
                row = rows.nth(i)
                expand = row.locator("img[id^='MostrarArquivos_']")
                if expand.count() == 0:
                    continue
                expand.first.click()
                time.sleep(0.5)
                anchors = row.locator("xpath=following-sibling::tr[1]//a[@href]")
                count = anchors.count()
                for j in range(count):
                    a = anchors.nth(j)
                    href = a.get_attribute("href")
                    title = a.get_attribute("title") or a.inner_text()
                    info = self._parse_nome_documento(title or "", target_id)  # PASSAR O ID DO SISTEMA
                    if not info.get('id'):
                        try:
                            import re
                            ms = re.findall(r'_(\d+)_(\d+)_', f"{title} {href}")
                            if ms:
                                u = ms[-1]
                                info['id'] = f"_{u[0]}_{u[1]}_"
                        except Exception:
                            pass
                    alvo_norm = self._normalizar_id(target_id)
                    id_norm = self._normalizar_id(info.get('id','')) or self._normalizar_id(title) or self._normalizar_id(href)
                    if id_norm and alvo_norm and id_norm == alvo_norm:
                        info['link'] = href or ""
                        return info
                expand.first.click()
                time.sleep(0.3)
        except Exception as e:
            self.logger.error(f"Erro ao buscar anexo por id: {e}")
        return {}
    
    def fechar_driver(self):
        try:
            if getattr(self, 'context', None):
                self.context.close()
            if getattr(self, 'browser', None):
                self.browser.close()
            if getattr(self, '_pw', None):
                self._pw.stop()
            self.logger.info("Browser fechado")
        except Exception as e:
            self.logger.error(f"Erro ao fechar browser: {e}")

    def snapshot(self, tag: str = "snap"):
        try:
            base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "snapshots"))
            os.makedirs(base, exist_ok=True)
            ts = time.strftime("%Y%m%d_%H%M%S")
            bn = f"{self.batch_id}_{ts}_{tag}.png" if self.batch_id else f"{ts}_{tag}.png"
            fp = os.path.join(base, bn)
            self.page.screenshot(path=fp, full_page=True)
            return fp
        except Exception:
            return None


# Funções auxiliares para parsing de arquivos
def extrair_informacao_arquivo(nome_arquivo: str) -> dict:
    """
    Extrai informações de um nome de arquivo de petição
    Ex: 5188032.43.2019.8.09.0152_9565_56790_Manifestação.pdf
    
    Returns:
        Dict com numero_processo, identificador_peticao, nome_original
    """
    try:
        nome_sem_extensao = os.path.splitext(nome_arquivo)[0]
        import re
        padrao_processo = r'(\d{1,7}\.\d{2}\.\d{4}\.\d{1}\.\d{2}\.\d{4})'
        processos = re.findall(padrao_processo, nome_sem_extensao)
        numero_processo = processos[-1] if processos else ''
        if numero_processo:
            parts = numero_processo.split('.')
            if len(parts) == 6:
                parts[0] = parts[0].zfill(7)
                numero_processo = '.'.join(parts)
        padrao_id = r'_(\d+)_(\d+)_'
        ids = re.findall(padrao_id, nome_sem_extensao)
        identificador = f"_{ids[-1][0]}_{ids[-1][1]}_" if ids else ''
        if numero_processo:
            return {
                'numero_processo': numero_processo,
                'identificador_peticao': identificador,
                'nome_original': nome_arquivo
            }
        return None
    except Exception as e:
        logging.error(f"Erro ao extrair informações do arquivo '{nome_arquivo}': {e}")
        return None


def processar_lista_arquivos(arquivos: list) -> list:
    """
    Processa uma lista de nomes de arquivos e extrai informações
    """
    processados = []
    
    for arquivo in arquivos:
        arquivo = arquivo.strip()
        if arquivo:
            info = extrair_informacao_arquivo(arquivo)
            if info:
                processados.append(info)
            else:
                logging.warning(f"Não foi possível extrair informações do arquivo: {arquivo}")
    
    return processados
pass