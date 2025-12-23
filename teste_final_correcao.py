#!/usr/bin/env python3
"""
Teste final da correção do matching com o caso real
"""

import sys
import os
sys.path.append('verificador_peticoes/src')

from projudi_extractor import ProjudiExtractor
import re

def testar_correcao_final():
    """Testar a correção com o caso real do problema"""
    
    # Dados do problema
    nome_arquivo = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    id_sistema = "_9565_56790_"
    
    print("TESTE FINAL DA CORREÇÃO")
    print("="*60)
    print(f"Arquivo: {nome_arquivo}")
    print(f"ID do sistema: {id_sistema}")
    print("-" * 60)
    
    # Criar uma instância mock do extractor
    extractor = ProjudiExtractor()
    
    # Testar o parsing com o ID do sistema
    resultado = extractor._parse_nome_documento(nome_arquivo, id_sistema)
    print(f"Resultado do parsing: {resultado}")
    
    # Testar a normalização
    id_parseado = resultado.get('id', '')
    if id_parseado:
        id_normalizado = extractor._normalizar_id(id_parseado)
        id_sistema_normalizado = extractor._normalizar_id(id_sistema)
        
        print(f"ID parseado: '{id_parseado}'")
        print(f"ID parseado normalizado: '{id_normalizado}'")
        print(f"ID sistema normalizado: '{id_sistema_normalizado}'")
        print(f"Match normalizado: {id_normalizado == id_sistema_normalizado}")
        
        # Testar matching direto
        match_direto = id_sistema.strip('_') in id_parseado.strip('_')
        print(f"Match direto (sem underscores): {match_direto}")
        
        # Resultado final
        if id_normalizado == id_sistema_normalizado or match_direto:
            print("\n✓ SUCESSO: O ID foi encontrado corretamente!")
            return True
        else:
            print("\n✗ FALHA: O ID não foi encontrado!")
            return False
    else:
        print("\n✗ FALHA: Nenhum ID foi parseado!")
        return False

if __name__ == "__main__":
    sucesso = testar_correcao_final()
    sys.exit(0 if sucesso else 1)