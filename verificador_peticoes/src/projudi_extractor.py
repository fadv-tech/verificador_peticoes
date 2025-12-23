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
        self.last_error = ""
    
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
            self.browser = self._pw.chromium.launch(
                headless=headless,
                args=[
                    "--start-maximized",
                    "--disable-gpu",
                    "--disable-software-rasterizer",
                    "--disable-features=VizDisplayCompositor"
                ]
            ) 
            self.context = self.browser.new_context(ignore_https_errors=True)
            try:
                self.context.set_default_timeout(15000)
            except Exception:
                pass
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
            try:
                self.page.evaluate("""
                    () => {
                        const ov = document.querySelector('.ui-widget-overlay.ui-front');
                        if (ov) ov.remove();
                    }
                """)
            except Exception:
                pass
            self.page.locator("[name='entrar']").click(force=True, no_wait_after=True)
            try:
                self.page.wait_for_selector("iframe[name='userMainFrame']", timeout=20000)
                self.logger.info("Frame principal disponível")
            except PWTimeoutError:
                try:
                    self.page.locator("[name='entrar']").click(force=True, no_wait_after=True)
                    self.page.wait_for_selector("iframe[name='userMainFrame']", timeout=20000)
                    self.logger.info("Frame principal disponível")
                except Exception:
                    try:
                        self.page.locator("#senha").press("Enter")
                        self.page.wait_for_selector("iframe[name='userMainFrame']", timeout=20000)
                        self.logger.info("Frame principal disponível")
                    except Exception:
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
            try:
                self.page.goto("https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4", wait_until="domcontentloaded")
            except Exception:
                try:
                    if getattr(self, 'page', None) and self.page.is_closed():
                        self.page = self.context.new_page()
                    self.page.goto("https://projudi.tjgo.jus.br/BuscaProcesso?PaginaAtual=4", wait_until="domcontentloaded")
                except Exception:
                    try:
                        self.last_error = "falha_abrir_busca"
                    except Exception:
                        pass
                    self.logger.error("Falha ao abrir busca de processo")
                    return False
            frame = self.page.frame(name="userMainFrame")
            base = frame if frame else self.page
            campo = base.locator("#ProcessoNumero")
            campo.fill("")
            campo.fill(numero_processo)
            self.logger.info("Campo preenchido")
            try:
                base.locator("[name='imaLimparProcessoStatus']").wait_for(state="visible", timeout=5000)
                base.locator("[name='imaLimparProcessoStatus']").click()
            except Exception:
                try:
                    (frame or self.page).evaluate("""
                        () => {
                            const btn = document.querySelector('[name="imaLimparProcessoStatus"]');
                            try { if (btn) btn.click(); } catch(e) {}
                            ['Id_ProcessoStatus','ProcessoStatusCodigo','ProcessoStatus'].forEach(id => {
                                const el = document.getElementById(id);
                                if (el) el.value = '';
                            });
                        }
                    """)
                except Exception:
                    pass
            try:
                (frame or self.page).evaluate("""
                    () => {
                        const ov = document.querySelector('.ui-widget-overlay.ui-front');
                        if (ov) ov.remove();
                    }
                """)
            except Exception:
                pass
            base.locator("[name='imgSubmeter']").click(force=True, no_wait_after=True)
            try:
                campo.press("Enter")
            except Exception:
                pass
            try:
                self.page.wait_for_load_state("networkidle")
            except Exception:
                pass
            self.logger.info("Submetendo busca")
            ok = False
            try:
                try:
                    (frame or self.page).wait_for_selector("#span_proc_numero", timeout=8000)
                    ok = True
                except Exception:
                    (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=8000)
                    ok = True
            except PWTimeoutError:
                pass
            if not ok:
                try:
                    self.logger.info("Tentando localizar link do processo pelo número")
                    alvo = base.locator(f"text={numero_processo}")
                    if alvo.count() > 0:
                        alvo.first.click()
                        try:
                            if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                self.page = self.context.pages[-1]
                                try:
                                    for p in self.context.pages[:-1]:
                                        try: p.close()
                                        except Exception: pass
                                except Exception:
                                    pass
                                frame = self.page.frame(name="userMainFrame")
                        except Exception:
                            pass
                        try:
                            (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                            ok = True
                        except Exception:
                            (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                            ok = True
                except Exception:
                    pass
            if not ok:
                try:
                    self.logger.info("Tentando abrir link de VisualizarProcesso")
                    link = base.locator("a[href*='VisualizarProcesso']")
                    if link.count() > 0:
                        link.first.click()
                        try:
                            if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                self.page = self.context.pages[-1]
                                try:
                                    for p in self.context.pages[:-1]:
                                        try: p.close()
                                        except Exception: pass
                                except Exception:
                                    pass
                                frame = self.page.frame(name="userMainFrame")
                        except Exception:
                            pass
                        try:
                            (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                            ok = True
                        except Exception:
                            (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                            ok = True
                except Exception:
                    pass
            if not ok:
                try:
                    self.logger.info("Tentando abrir link por título Visualizar")
                    v = base.locator("a[title*='Visualizar']")
                    if v.count() > 0:
                        v.first.click()
                        try:
                            if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                self.page = self.context.pages[-1]
                                try:
                                    for p in self.context.pages[:-1]:
                                        try: p.close()
                                        except Exception: pass
                                except Exception:
                                    pass
                                frame = self.page.frame(name="userMainFrame")
                        except Exception:
                            pass
                        try:
                            (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                            ok = True
                        except Exception:
                            (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                            ok = True
                except Exception:
                    pass
            if not ok:
                try:
                    self.logger.info("Tentando clicar linha com número do processo")
                    row = base.locator("tr", has_text=numero_processo)
                    if row.count() > 0:
                        a2 = row.first.locator("a[href*='VisualizarProcesso']")
                        if a2.count() > 0:
                            a2.first.click()
                            try:
                                if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                    self.page = self.context.pages[-1]
                                    try:
                                        for p in self.context.pages[:-1]:
                                            try: p.close()
                                            except Exception: pass
                                    except Exception:
                                        pass
                                    frame = self.page.frame(name="userMainFrame")
                            except Exception:
                                pass
                            try:
                                (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                                ok = True
                            except Exception:
                                (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                                ok = True
                except Exception:
                    pass
            if not ok:
                try:
                    self.logger.info("Verificando conteúdo dentro de userMainFrame")
                    frame = self.page.frame(name="userMainFrame")
                    if frame:
                        try:
                            alvo_f = frame.locator(f"text={numero_processo}")
                            if alvo_f.count() > 0:
                                alvo_f.first.click()
                                try:
                                    if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                        self.page = self.context.pages[-1]
                                        try:
                                            for p in self.context.pages[:-1]:
                                                try: p.close()
                                                except Exception: pass
                                        except Exception:
                                            pass
                                        frame = self.page.frame(name="userMainFrame")
                                except Exception:
                                    pass
                                try:
                                    frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                    ok = True
                                except Exception:
                                    frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    ok = True
                        except Exception:
                            pass
                        if not ok:
                            try:
                                link_f = frame.locator("a[href*='VisualizarProcesso']")
                                if link_f.count() > 0:
                                    link_f.first.click()
                                    try:
                                        if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                            self.page = self.context.pages[-1]
                                            try:
                                                for p in self.context.pages[:-1]:
                                                    try: p.close()
                                                    except Exception: pass
                                            except Exception:
                                                pass
                                            frame = self.page.frame(name="userMainFrame")
                                    except Exception:
                                        pass
                                    try:
                                        frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                        ok = True
                                    except Exception:
                                        frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                        ok = True
                            except Exception:
                                pass
                        if not ok:
                            try:
                                v_f = frame.locator("a[title*='Visualizar']")
                                if v_f.count() > 0:
                                    v_f.first.click()
                                    try:
                                        if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                            self.page = self.context.pages[-1]
                                            try:
                                                for p in self.context.pages[:-1]:
                                                    try: p.close()
                                                    except Exception: pass
                                            except Exception:
                                                pass
                                            frame = self.page.frame(name="userMainFrame")
                                    except Exception:
                                        pass
                                    try:
                                        frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                        ok = True
                                    except Exception:
                                        frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                        ok = True
                            except Exception:
                                pass
                        if not ok:
                            try:
                                row_f = frame.locator("tr", has_text=numero_processo)
                                if row_f.count() > 0:
                                    a2f = row_f.first.locator("a[href*='VisualizarProcesso']")
                                    if a2f.count() > 0:
                                        a2f.first.click()
                                        try:
                                            if hasattr(self, 'context') and len(getattr(self.context, 'pages', [])) > 1:
                                                self.page = self.context.pages[-1]
                                                frame = self.page.frame(name="userMainFrame")
                                        except Exception:
                                            pass
                                        try:
                                            frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                            ok = True
                                        except Exception:
                                            frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                            ok = True
                            except Exception:
                                pass
                        try:
                            frame.wait_for_selector("#span_proc_numero", timeout=8000)
                            ok = True
                        except Exception:
                            try:
                                frame.wait_for_selector("tr.filtro-entrada", timeout=8000)
                                ok = True
                            except Exception:
                                pass
                except Exception:
                    pass
            if not ok:
                try:
                    self.snapshot("falha_abrir_processo")
                except Exception:
                    pass
                try:
                    cur_url = ""
                    try:
                        cur_url = str(getattr(self.page, 'url', '') or "")
                    except Exception:
                        pass
                    has_frame = False
                    try:
                        has_frame = bool(self.page.frame(name="userMainFrame"))
                    except Exception:
                        has_frame = False
                    self.last_error = f"nao_abriu_processo url={cur_url or '-'} frame={('ok' if has_frame else 'na')}"
                except Exception:
                    self.last_error = "nao_abriu_processo"
                self.logger.error("Não foi possível abrir a página do processo")
                return False
            self.logger.info("Página do processo carregada")
            time.sleep(1)
            try:
                self.snapshot(f"proc_{numero_processo}")
            except Exception:
                pass
            return True
        except Exception as e:
            try:
                cur_url = ""
                try:
                    cur_url = str(getattr(self.page, 'url', '') or "")
                except Exception:
                    pass
                self.last_error = f"erro_pesquisar url={cur_url or '-'} msg={str(e)}"
            except Exception:
                self.last_error = f"erro_pesquisar msg={str(e)}"
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
            try:
                self.page.goto("https://projudi.tjgo.jus.br/BuscaProcesso", wait_until="domcontentloaded")
            except Exception:
                try:
                    if getattr(self, 'page', None) and self.page.is_closed():
                        self.page = self.context.new_page()
                    self.page.goto("https://projudi.tjgo.jus.br/BuscaProcesso", wait_until="domcontentloaded")
                except Exception:
                    try:
                        self.last_error = "falha_abrir_busca_docs"
                    except Exception:
                        pass
                    self.logger.error("Falha ao abrir página de busca de processo")
                    return []
            self.logger.info("Página de busca aberta")
            
            # Aguarda e preenche campo de busca
            frame = self.page.frame(name="userMainFrame")
            base = frame if frame else self.page
            campo = base.locator("#ProcessoNumero")
            campo.fill("")
            campo.fill(numero_processo)
            self.logger.info("Número do processo preenchido")
            try:
                base.locator("[name='imaLimparProcessoStatus']").wait_for(state="visible", timeout=5000)
                base.locator("[name='imaLimparProcessoStatus']").click()
            except Exception:
                try:
                    (frame or self.page).evaluate("""
                        () => {
                            try {
                                if (typeof LimparChaveEstrangeira === 'function') {
                                    LimparChaveEstrangeira('Id_ProcessoStatus','ProcessoStatus');
                                    LimparChaveEstrangeira('ProcessoStatusCodigo','ProcessoStatus');
                                }
                            } catch (e) {}
                            const btn = document.querySelector('[name="imaLimparProcessoStatus"]');
                            try { if (btn) btn.click(); } catch(e) {}
                            ['Id_ProcessoStatus','ProcessoStatusCodigo','ProcessoStatus'].forEach(id => {
                                const el = document.getElementById(id);
                                if (el) el.value = '';
                            });
                        }
                    """)
                except Exception:
                    pass
            time.sleep(0.2)
            
            # Clica no botão de busca
            try:
                self.page.evaluate("""
                    () => {
                        const ov = document.querySelector('.ui-widget-overlay.ui-front');
                        if (ov) ov.remove();
                    }
                """)
            except Exception:
                pass
            base.locator("[name='imgSubmeter']").click(force=True, no_wait_after=True)
            try:
                self.page.wait_for_load_state("networkidle")
            except Exception:
                pass
            time.sleep(1)
            frame = self.page.frame(name="userMainFrame")
            base = frame if frame else self.page
            self.logger.info("Busca submetida")
            
            carregado = False
            try:
                try:
                    (frame or self.page).wait_for_selector("#span_proc_numero", timeout=8000)
                    carregado = True
                except Exception:
                    (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=8000)
                    carregado = True
            except PWTimeoutError:
                pass
            if not carregado:
                try:
                    self.logger.info("Tentando localizar link do processo pelo número")
                    try:
                        base.wait_for_selector(f"text={numero_processo}", timeout=12000)
                    except Exception:
                        pass
                    alvo = base.locator(f"text={numero_processo}")
                    if alvo.count() > 0:
                        alvo.first.click()
                        try:
                            (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                            carregado = True
                        except Exception:
                            (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                            carregado = True
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Tentando abrir link de VisualizarProcesso")
                    try:
                        base.wait_for_selector("a[href*='VisualizarProcesso']", timeout=12000)
                    except Exception:
                        pass
                    link = base.locator("a[href*='VisualizarProcesso']")
                    if link.count() > 0:
                        link.first.click()
                        try:
                            (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                            carregado = True
                        except Exception:
                            (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                            carregado = True
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Tentando abrir link por título Visualizar")
                    try:
                        base.wait_for_selector("a[title*='Visualizar']", timeout=12000)
                    except Exception:
                        pass
                    v = base.locator("a[title*='Visualizar']")
                    if v.count() > 0:
                        v.first.click()
                        try:
                            (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                            carregado = True
                        except Exception:
                            (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                            carregado = True
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Tentando abrir link por texto normalizado")
                    import re
                    alvo = re.sub(r"\D", "", numero_processo)
                    links = base.locator("a[href*='Visualizar']")
                    cnt = links.count()
                    for k in range(cnt):
                        try:
                            t = links.nth(k).inner_text() or ""
                            if re.sub(r"\D", "", t) == alvo:
                                links.nth(k).click()
                                (frame or self.page).wait_for_selector("#span_proc_numero", timeout=20000)
                                carregado = True
                                break
                        except Exception:
                            pass
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Varredura em todos os frames por link do processo")
                    for fr in getattr(self.page, 'frames', []):
                        if carregado:
                            break
                        try:
                            alvo_t = fr.locator(f"text={numero_processo}")
                            if alvo_t.count() > 0:
                                alvo_t.first.click()
                                try:
                                    fr.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    fr.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                                break
                        except Exception:
                            pass
                        try:
                            link_t = fr.locator("a[href*='VisualizarProcesso']")
                            if link_t.count() > 0:
                                link_t.first.click()
                                try:
                                    fr.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    fr.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                                break
                        except Exception:
                            pass
                        try:
                            v_t = fr.locator("a[title*='Visualizar']")
                            if v_t.count() > 0:
                                v_t.first.click()
                                try:
                                    fr.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    fr.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                                break
                        except Exception:
                            pass
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Tentando clicar linha com número do processo")
                    row = base.locator("tr", has_text=numero_processo)
                    if row.count() > 0:
                        a2 = row.first.locator("a[href*='VisualizarProcesso']")
                        if a2.count() > 0:
                            a2.first.click()
                            try:
                                (frame or self.page).wait_for_selector("#span_proc_numero", timeout=12000)
                                carregado = True
                            except Exception:
                                (frame or self.page).wait_for_selector("tr.filtro-entrada", timeout=12000)
                                carregado = True
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Verificando conteúdo dentro de userMainFrame")
                    frame = self.page.frame(name="userMainFrame")
                    if frame:
                        try:
                            alvo_f = frame.locator(f"text={numero_processo}")
                            if alvo_f.count() > 0:
                                alvo_f.first.click()
                                try:
                                    frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                        except Exception:
                            pass
                        if not carregado:
                            try:
                                link_f = frame.locator("a[href*='VisualizarProcesso']")
                                if link_f.count() > 0:
                                    link_f.first.click()
                                    try:
                                        frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                        carregado = True
                                    except Exception:
                                        frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                        carregado = True
                            except Exception:
                                pass
                        if not carregado:
                            try:
                                v_f = frame.locator("a[title*='Visualizar']")
                                if v_f.count() > 0:
                                    v_f.first.click()
                                    try:
                                        frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                        carregado = True
                                    except Exception:
                                        frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                        carregado = True
                            except Exception:
                                pass
                        if not carregado:
                            try:
                                row_f = frame.locator("tr", has_text=numero_processo)
                                if row_f.count() > 0:
                                    a2f = row_f.first.locator("a[href*='VisualizarProcesso']")
                                    if a2f.count() > 0:
                                        a2f.first.click()
                                        try:
                                            frame.wait_for_selector("#span_proc_numero", timeout=12000)
                                            carregado = True
                                        except Exception:
                                            frame.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                            carregado = True
                            except Exception:
                                pass
                        try:
                            frame.wait_for_selector("#span_proc_numero", timeout=8000)
                            carregado = True
                        except Exception:
                            try:
                                frame.wait_for_selector("tr.filtro-entrada", timeout=8000)
                                carregado = True
                            except Exception:
                                pass
                except Exception:
                    pass
            if not carregado:
                try:
                    self.logger.info("Varredura em todos os frames por link do processo")
                    for fr in getattr(self.page, 'frames', []):
                        if carregado:
                            break
                        try:
                            alvo_t = fr.locator(f"text={numero_processo}")
                            if alvo_t.count() > 0:
                                alvo_t.first.click()
                                try:
                                    fr.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    fr.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                                break
                        except Exception:
                            pass
                        try:
                            link_t = fr.locator("a[href*='VisualizarProcesso']")
                            if link_t.count() > 0:
                                link_t.first.click()
                                try:
                                    fr.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    fr.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                                break
                        except Exception:
                            pass
                        try:
                            v_t = fr.locator("a[title*='Visualizar']")
                            if v_t.count() > 0:
                                v_t.first.click()
                                try:
                                    fr.wait_for_selector("#span_proc_numero", timeout=12000)
                                    carregado = True
                                except Exception:
                                    fr.wait_for_selector("tr.filtro-entrada", timeout=12000)
                                    carregado = True
                                break
                        except Exception:
                            pass
                except Exception:
                    pass
            if not carregado:
                try:
                    self.snapshot("falha_resultado_processo")
                except Exception:
                    pass
                try:
                    cur_url = ""
                    try:
                        cur_url = str(getattr(self.page, 'url', '') or "")
                    except Exception:
                        pass
                    has_frame = False
                    try:
                        has_frame = bool(self.page.frame(name="userMainFrame"))
                    except Exception:
                        has_frame = False
                    self.last_error = f"falha_resultado url={cur_url or '-'} frame={('ok' if has_frame else 'na')}"
                except Exception:
                    self.last_error = "falha_resultado"
                self.logger.error("Falha ao carregar resultado do processo")
                return []
            self.logger.info("Resultado carregado")
            
            time.sleep(2)  # Pequena pausa para carregar completamente
            try:
                self.snapshot("resultado_carregado")
            except Exception:
                pass
            
            # Expande cada movimentação e coleta os anexos (padrão do Projudi)
            try:
                self.page.evaluate("""
                    () => {
                        document.querySelectorAll('.ui-widget-overlay.ui-front').forEach(el => {
                            try { el.remove(); } catch(e) {}
                        });
                    }
                """)
            except Exception:
                pass
            try:
                if frame:
                    frame.evaluate("""
                        () => {
                            document.querySelectorAll('.ui-widget-overlay.ui-front').forEach(el => {
                                try { el.remove(); } catch(e) {}
                            });
                        }
                    """)
            except Exception:
                pass
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
            frame = self.page.frame(name="userMainFrame")
            base = frame if frame else self.page
            rows = base.locator("tr.filtro-entrada")
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
                try:
                    self.page.evaluate("""
                        () => {
                            document.querySelectorAll('.ui-widget-overlay.ui-front').forEach(el => {
                                try { el.remove(); } catch(e) {}
                            });
                        }
                    """)
                except Exception:
                    pass
                try:
                    if frame:
                        frame.evaluate("""
                            () => {
                                document.querySelectorAll('.ui-widget-overlay.ui-front').forEach(el => {
                                    try { el.remove(); } catch(e) {}
                                });
                            }
                        """)
                except Exception:
                    pass
                expand.first.click(force=True)
                time.sleep(0.5)
                try:
                    row.locator("xpath=following-sibling::tr[1]//a[@href]").wait_for(state="visible", timeout=2000)
                except Exception:
                    pass
                mov_date = ''
                try:
                    mov_date = self._pick_movement_date(row) or ''
                except Exception:
                    mov_date = ''
                anchors = row.locator("xpath=following-sibling::tr[1]//a[@href]")
                count = anchors.count()
                self.logger.info(f"Arquivos anexos: {count}")
                for j in range(count):
                    a = anchors.nth(j)
                    href = a.get_attribute("href")
                    title = a.get_attribute("title") or a.inner_text()
                    info = self._parse_nome_documento(title or "")
                    try:
                        att_row = row.locator("xpath=following-sibling::tr[1]")
                        att_text = att_row.inner_text()
                        if not info.get('data'):
                            info['data'] = self._pick_protocol_date(att_text)
                    except Exception:
                        pass
                    if not info.get('data'):
                        try:
                            cells = att_row.locator("td")
                            ccount = cells.count()
                            for ci in range(ccount):
                                if info.get('data'):
                                    break
                                tx = cells.nth(ci).inner_text()
                                d2 = self._pick_protocol_date(tx)
                                if d2:
                                    info['data'] = d2
                                    break
                        except Exception:
                            pass
                    if not info.get('data'):
                        try:
                            for k in [2,3]:
                                if info.get('data'):
                                    break
                                rowk = row.locator(f"xpath=following-sibling::tr[{k}]")
                                if rowk.count() > 0:
                                    txk = rowk.inner_text()
                                    d3 = self._pick_protocol_date(txk)
                                    if d3:
                                        info['data'] = d3
                                        break
                        except Exception:
                            pass
                    if not info.get('data'):
                        try:
                            info['data'] = self._pick_protocol_date(title or "")
                        except Exception:
                            pass
                    if not info.get('data') and mov_date:
                        info['data'] = mov_date
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
            
            padrao_data1 = r'(\d{2})\.(\d{2})\.(\d{4})'
            padrao_data2 = r'(\d{2})\/(\d{2})\/(\d{4})'
            padrao_data3 = r'(\d{2})\-(\d{2})\-(\d{4})'
            import re
            data_match = re.search(padrao_data1, nome) or re.search(padrao_data2, nome) or re.search(padrao_data3, nome)
            if data_match:
                id_info['data'] = self._sanitize_date_token(data_match.group(0))
            
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

    def _sanitize_date_token(self, token: str) -> str:
        try:
            if not token:
                return ''
            s = str(token).strip().replace('.', '/').replace('-', '/').replace(' ', '')
            import re
            m = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', s)
            if m:
                dd = int(m.group(1)); mm = int(m.group(2)); yy = int(m.group(3))
                if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                    return ''
                return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
            m2 = re.match(r'^(\d{4})/(\d{2})/(\d{2})$', s)
            if m2:
                yy = int(m2.group(1)); mm = int(m2.group(2)); dd = int(m2.group(3))
                if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                    return ''
                return f"{m2.group(3)}/{m2.group(2)}/{m2.group(1)}"
            return ''
        except Exception:
            return ''

    def _pick_protocol_date(self, text: str) -> str:
        try:
            import re
            txt = str(text or '')
            pats = [
                r'(?i)Data\s+de\s+Protocolo[:\s]+(\d{2}[-\./]\d{2}[-\./]\d{4})',
                r'(?i)Data\s+do\s+Protocolo[:\s]+(\d{2}[-\./]\d{2}[-\./]\d{4})',
                r'(?i)Protocolo\s+em[:\s]+(\d{2}[-\./]\d{2}[-\./]\d{4})',
                r'(?i)Protocolo[:\s]+(\d{2}[-\./]\d{2}[-\./]\d{4})',
                r'(?i)Protocolada\s+em[:\s]?(\d{2}[-\./]\d{2}[-\./]\d{4})',
                r'(?i)Protocolado\s+em[:\s]?(\d{2}[-\./]\d{2}[-\./]\d{4})'
            ]
            for p in pats:
                m = re.search(p, txt)
                if m:
                    s = self._sanitize_date_token(m.group(1))
                    if s:
                        return s
            matches = list(re.finditer(r'(\d{2}[-\./]\d{2}[-\./]\d{4})', txt))
            iso_matches = list(re.finditer(r'(\d{4}[-/]\d{2}[-/]\d{2})', txt))
            if not matches:
                matches = iso_matches
                if not matches:
                    return ''
            keypos = None
            for k in ['protocolo', 'protocol', 'protocolada', 'protocolado', 'protocolização']:
                p = txt.lower().find(k)
                if p >= 0:
                    keypos = p if keypos is None else min(keypos, p)
            candidates = []
            for m in matches:
                tok = m.group(1)
                norm = self._sanitize_date_token(tok)
                if not norm:
                    continue
                dist = abs((m.start() - keypos)) if keypos is not None else m.start()
                candidates.append((dist, norm))
            if not candidates:
                return ''
            candidates.sort(key=lambda x: x[0])
            return candidates[0][1]
        except Exception:
            return ''
    
    def _pick_movement_date(self, row) -> str:
        try:
            td = row.locator("td[width='100'][align='center']")
            if td.count() > 0:
                txt = (td.first.inner_text() or '').strip()
                import re
                m = re.search(r"(\d{2}[-\./]\d{2}[-\./]\d{4})", txt)
                if m:
                    return self._sanitize_date_token(m.group(1))
                m_iso = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2})", txt)
                if m_iso:
                    return self._sanitize_date_token(m_iso.group(1))
        except Exception:
            pass
        try:
            td3 = row.locator("xpath=./td[3]")
            if td3.count() > 0:
                txt = (td3.first.inner_text() or '').strip()
                import re
                m = re.search(r"(\d{2}[-\./]\d{2}[-\./]\d{4})", txt)
                if m:
                    return self._sanitize_date_token(m.group(1))
                m_iso = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2})", txt)
                if m_iso:
                    return self._sanitize_date_token(m_iso.group(1))
        except Exception:
            pass
        try:
            txt2 = (row.inner_text() or '')
            import re
            m2 = re.search(r"(\d{2}[-\./]\d{2}[-\./]\d{4})", txt2)
            if m2:
                return self._sanitize_date_token(m2.group(1))
            m2_iso = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2})", txt2)
            if m2_iso:
                return self._sanitize_date_token(m2_iso.group(1))
        except Exception:
            pass
        return ''

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
            
            ok_proc = self.pesquisar_processo(numero_processo)
            if identificador_peticao and ok_proc:
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
                if ok_proc:
                    resultado['mensagem'] = 'Nenhum documento encontrado no processo'
                else:
                    msg = 'Falha ao abrir o processo'
                    try:
                        if getattr(self, 'last_error', ''):
                            msg = f'{msg} ({self.last_error})'
                    except Exception:
                        pass
                    resultado['mensagem'] = msg
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
            frame = self.page.frame(name="userMainFrame")
            base = frame if frame else self.page
            rows = base.locator("tr.filtro-entrada")
            total = rows.count()
            for i in range(total):
                row = rows.nth(i)
                expand = row.locator("img[id^='MostrarArquivos_']")
                if expand.count() == 0:
                    continue
                try:
                    if frame:
                        frame.evaluate("""
                            () => {
                                document.querySelectorAll('.ui-widget-overlay.ui-front').forEach(el => {
                                    try { el.remove(); } catch(e) {}
                                });
                            }
                        """)
                except Exception:
                    pass
                expand.first.click(force=True)
                time.sleep(0.5)
                try:
                    row.locator("xpath=following-sibling::tr[1]//a[@href]").wait_for(state="visible", timeout=2000)
                except Exception:
                    pass
                mov_date = ''
                try:
                    mov_date = self._pick_movement_date(row) or ''
                except Exception:
                    mov_date = ''
                anchors = row.locator("xpath=following-sibling::tr[1]//a[@href]")
                count = anchors.count()
                for j in range(count):
                    a = anchors.nth(j)
                    href = a.get_attribute("href")
                    title = a.get_attribute("title") or a.inner_text()
                    info = self._parse_nome_documento(title or "", target_id)  # PASSAR O ID DO SISTEMA
                    try:
                        att_row = row.locator("xpath=following-sibling::tr[1]")
                        att_text = att_row.inner_text()
                        if not info.get('data'):
                            info['data'] = self._pick_protocol_date(att_text)
                    except Exception:
                        pass
                    if not info.get('data'):
                        try:
                            cells = att_row.locator("td")
                            ccount = cells.count()
                            for ci in range(ccount):
                                if info.get('data'):
                                    break
                                tx = cells.nth(ci).inner_text()
                                d2 = self._pick_protocol_date(tx)
                                if d2:
                                    info['data'] = d2
                                    break
                        except Exception:
                            pass
                    if not info.get('data'):
                        try:
                            info['data'] = self._pick_protocol_date(title or "")
                        except Exception:
                            pass
                    if not info.get('data') and mov_date:
                        info['data'] = mov_date
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
