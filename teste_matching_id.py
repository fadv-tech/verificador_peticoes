#!/usr/bin/env python3
"""
Script de teste para diagnosticar o problema de matching do identificador _9565_56790_
"""

import re

def testar_matching():
    # Caso real do problema
    filename = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    target_id = "_9565_56790_"
    
    print(f"Arquivo: {filename}")
    print(f"ID alvo: {target_id}")
    print("-" * 60)
    
    # Testar diferentes regex
    patterns = [
        r'_(\d+)_(\d+)_',  # Pattern atual
        r'_?\d+_\d+_?',     # Pattern alternativo
        r'_\d+_\d+(?:_|$)', # Pattern que captura até final ou underscore
        r'_(\d+)_([\d_]+)', # Pattern que captura números e underscores
    ]
    
    for i, pattern in enumerate(patterns, 1):
        print(f"\nPattern {i}: {pattern}")
        matches = re.findall(pattern, filename)
        print(f"Matches encontrados: {matches}")
        
        # Verificar se algum match corresponde ao target
        for match in matches:
            if isinstance(match, tuple):
                # Se for tupla, juntar os grupos
                matched_str = '_' + '_'.join(match) + '_'
            else:
                matched_str = match
            
            print(f"  Match processado: {matched_str}")
            
            # Verificar se é o ID que queremos
            if target_id in matched_str or matched_str in target_id:
                print(f"  ✓ CORRESPONDÊNCIA ENCONTRADA!")
            else:
                print(f"  ✗ Não corresponde ao target")
    
    # Testar nova abordagem: encontrar TODOS os padrões _x_x_
    print(f"\n{'='*60}")
    print("Nova abordagem - encontrar TODOS os padrões _x_x_:")
    
    # Pattern para encontrar todos os _digitos_digitos_
    all_patterns = re.findall(r'_\d+_\d+_?', filename)
    print(f"Todos os padrões encontrados: {all_patterns}")
    
    # Verificar qual é o nosso target
    for pattern in all_patterns:
        if target_id in pattern or pattern in target_id:
            print(f"✓ Padrão alvo encontrado: {pattern}")
            return pattern
    
    print("✗ Padrão alvo NÃO encontrado!")
    return None

def testar_normalizacao():
    """Testar a função de normalização"""
    print(f"\n{'='*60}")
    print("Testando normalização:")
    
    ids = ["_9565_56790_", "_9565_56790", "9565_56790_"]
    
    for id_str in ids:
        # Normalizar - remover underscores extras
        normalizado = id_str.strip('_')
        print(f"Original: {id_str} -> Normalizado: {normalizado}")
        
        # Criar versões para matching
        com_underscore = f"_{normalizado}_"
        sem_underscore = normalizado
        print(f"  Versão com underscore: {com_underscore}")
        print(f"  Versão sem underscore: {sem_underscore}")

if __name__ == "__main__":
    print("TESTE DE MATCHING DE IDENTIFICADOR")
    print("="*60)
    
    resultado = testar_matching()
    testar_normalizacao()
    
    print(f"\n{'='*60}")
    print("CONCLUSÃO:")
    if resultado:
        print(f"✓ ID encontrado: {resultado}")
    else:
        print("✗ ID NÃO encontrado - necessário corrigir regex!")