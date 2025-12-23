#!/usr/bin/env python3
"""
Script rápido para testar matching de identificador _9565_56790_
Processo 5188032.43.2019.8.09.0152
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from projudi_extractor import ProjudiExtractor
from database import DatabaseManager

# Dados do teste
NUMERO_PROCESSO = "5188032.43.2019.8.09.0152"
IDENTIFICADOR_PETICAO = "_9565_56790_"
USUARIO = "07871865625"
SENHA = "asdASD00-"

def test_matching():
    print("=== TESTE DE MATCHING ===")
    print(f"Processo: {NUMERO_PROCESSO}")
    print(f"Identificador: {IDENTIFICADOR_PETICAO}")
    
    db = DatabaseManager()
    
    # Criar extrator com logging detalhado
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("test")
    
    extrator = ProjudiExtractor(logger=logger, batch_id="test_9565_56790")
    
    try:
        print("\n1. Configurando driver...")
        ok = extrator.configurar_driver(headless=False)  # headless=False para ver o navegador
        if not ok:
            print("❌ Falha ao configurar driver")
            return
        print("✅ Driver configurado")
        
        print("\n2. Realizando login...")
        ok = extrator.realizar_login(USUARIO, SENHA)
        if not ok:
            print("❌ Falha no login")
            return
        print("✅ Login realizado")
        
        print("\n3. Pesquisando processo...")
        ok = extrator.pesquisar_processo(NUMERO_PROCESSO)
        if not ok:
            print("❌ Falha ao pesquisar processo")
            return
        print("✅ Processo carregado")
        
        print("\n4. Extraindo documentos do processo...")
        documentos = extrator.extrair_documentos_processo(NUMERO_PROCESSO)
        print(f"📄 Documentos encontrados: {len(documentos)}")
        
        if not documentos:
            print("❌ Nenhum documento encontrado")
            return
            
        # Mostrar todos os documentos
        print("\n--- DOCUMENTOS ENCONTRADOS ---")
        for i, doc in enumerate(documentos, 1):
            print(f"{i}. Nome: {doc.get('nome_documento', '')}")
            print(f"   ID: {doc.get('id_documento', '')}")
            print(f"   Link: {doc.get('link_download', '')}")
            print()
        
        print("\n5. Buscando documento com identificador _9565_56790_...")
        
        # Testar o método de busca por ID
        doc_match = extrator._buscar_anexo_por_id(IDENTIFICADOR_PETICAO)
        print(f"Resultado busca direta: {doc_match}")
        
        # Testar verificação completa
        print("\n6. Executando verificação completa...")
        resultado = extrator.verificar_protocolizacao(NUMERO_PROCESSO, IDENTIFICADOR_PETICAO)
        
        print(f"\n--- RESULTADO DA VERIFICAÇÃO ---")
        print(f"Processo: {resultado['processo']}")
        print(f"Identificador: {resultado['identificador_peticao']}")
        print(f"Encontrado: {resultado['encontrado']}")
        print(f"Nome do documento: {resultado['nome_documento']}")
        print(f"Mensagem: {resultado['mensagem']}")
        
        if resultado['encontrado']:
            print("✅ PETIÇÃO ENCONTRADA!")
        else:
            print("❌ Petição NÃO encontrada")
            
        # Debug: mostrar normalização
        print(f"\n--- DEBUG NORMALIZAÇÃO ---")
        alvo_norm = extrator._normalizar_id(IDENTIFICADOR_PETICAO)
        print(f"Identificador normalizado: '{alvo_norm}'")
        
        for doc in documentos:
            id_doc = doc.get('id_documento', '')
            id_norm = extrator._normalizar_id(id_doc) or extrator._normalizar_id(doc.get('nome_documento','')) or extrator._normalizar_id(doc.get('link_download',''))
            print(f"Doc: '{id_doc}' -> normalizado: '{id_norm}' -> match: {id_norm == alvo_norm}")
        
    except Exception as e:
        print(f"❌ Erro durante o teste: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n7. Fechando driver...")
        extrator.fechar_driver()
        print("✅ Teste finalizado")

if __name__ == "__main__":
    test_matching()