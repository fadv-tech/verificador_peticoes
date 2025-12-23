#!/usr/bin/env python3
"""
Teste com credenciais reais para verificar se a correção funcionou
"""

import sys
import os
sys.path.append('verificador_peticoes/src')

from projudi_extractor import ProjudiExtractor
import logging

def testar_com_credenciais():
    """Testar com as credenciais fornecidas pelo usuário"""
    
    # Configurar logging para ver o que está acontecendo
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Dados fornecidos pelo usuário
    usuario = "07871865625"
    senha = "asdASD00-"
    numero_processo = "5188032.43.2019.8.09.0152"
    identificador_peticao = "_9565_56790_"
    
    print("TESTE COM CREDENCIAIS REAIS")
    print("="*60)
    print(f"Usuário: {usuario}")
    print(f"Processo: {numero_processo}")
    print(f"ID Petição: {identificador_peticao}")
    print("-" * 60)
    
    try:
        # Criar extractor
        extractor = ProjudiExtractor(logger=logging.getLogger("teste.projudi_extractor"))
        
        # Configurar driver
        print("Configurando driver...")
        driver_sucesso = extractor.configurar_driver(headless=True)
        
        if not driver_sucesso:
            print("✗ Falha ao configurar driver")
            return False
        
        # Fazer login
        print("Fazendo login...")
        login_sucesso = extractor.realizar_login(usuario, senha)
        
        if not login_sucesso:
            print("✗ Falha no login")
            return False
        
        print("✓ Login realizado com sucesso")
        
        # Verificar protocolização
        print(f"Verificando protocolização da petição {identificador_peticao}...")
        resultado = extractor.verificar_protocolizacao(numero_processo, identificador_peticao)
        
        print(f"Resultado: {resultado}")
        
        # Analisar resultado
        if resultado.get('encontrado'):
            print(f"\n✓ SUCESSO: Petição encontrada!")
            print(f"  Documento: {resultado.get('nome_documento')}")
            print(f"  Data: {resultado.get('data_protocolo')}")
            print(f"  Tipo: {resultado.get('tipo_documento')}")
            return True
        else:
            print(f"\n✗ Petição não encontrada")
            print(f"  Mensagem: {resultado.get('mensagem')}")
            return False
            
    except Exception as e:
        print(f"\n✗ Erro durante o teste: {e}")
        return False
    
    finally:
        try:
            extractor.fechar_driver()
        except:
            pass

if __name__ == "__main__":
    sucesso = testar_com_credenciais()
    sys.exit(0 if sucesso else 1)