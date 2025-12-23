import os
import sys
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from projudi_extractor import ProjudiExtractor
from database import DatabaseManager
try:
    from test_matching import USUARIO as DEFAULT_USER, SENHA as DEFAULT_PASS
except Exception:
    DEFAULT_USER = ""
    DEFAULT_PASS = ""

NUM = "6164109.07.2024.8.09.0051"
ID = "_13894_56691_"

db = DatabaseManager()
usuario = os.environ.get("PROJUDI_USERNAME", "") or db.get_config("PROJUDI_USERNAME") or DEFAULT_USER
senha = os.environ.get("PROJUDI_PASSWORD", "") or db.get_config("PROJUDI_PASSWORD") or DEFAULT_PASS
print("Credenciais:", "ok" if (usuario and senha) else "ausentes")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
extrator = ProjudiExtractor(logger=logging.getLogger("test"), batch_id="access_6164109")

ok = extrator.configurar_driver(headless=False)
print("configurar_driver:", ok)
if not ok:
    sys.exit(1)

ok = extrator.realizar_login(usuario, senha)
print("realizar_login:", ok)
if not ok:
    extrator.fechar_driver()
    sys.exit(2)

for attempt in range(1, 4):
    print(f"tentativa {attempt}")
    ok = extrator.pesquisar_processo(NUM)
    print("pesquisar_processo:", ok)
    if ok:
        documentos = extrator.extrair_documentos_processo(NUM)
        print("docs_count:", len(documentos))
        m = extrator._buscar_anexo_por_id(ID)
        print("match_found:", bool(m))
        print("match:", m)
        if m:
            break
    time.sleep(2)

extrator.fechar_driver()
print("fim")