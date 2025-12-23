#!/usr/bin/env python3
"""
Fix para o matching de identificadores - versão corrigida v3
Testa TODOS os _x_x_ e também _x_x (sem underscore final)
"""
import re

def _normalizar_id(texto: str) -> str:
    """Normaliza ID para comparação"""
    try:
        if not texto:
            return ''
        # Procura padrão _NUMERO_NUMERO_ ou _NUMERO_NUMERO (sem underscore final)
        ms = re.findall(r'_\d+_\d+_?', texto)
        if ms:
            # Pega o último match (mais próximo do final do nome)
            ultimo = ms[-1]
            # Remove o underscore final se existir
            return ultimo.rstrip('_')
        return ''
    except Exception:
        return ''

def testar_todos_ids_v3(nome: str, alvo: str) -> bool:
    """Testa se algum dos _x_x_ ou _x_x no nome corresponde ao alvo"""
    try:
        # Encontra TODOS os padrões _x_x_ e _x_x no nome
        todos_ids = re.findall(r'_\d+_\d+_?', nome)
        alvo_norm = _normalizar_id(alvo)
        
        print(f"Nome: {nome}")
        print(f"Alvo: {alvo} -> normalizado: '{alvo_norm}'")
        print(f"IDs encontrados no nome: {todos_ids}")
        
        for id_encontrado in todos_ids:
            id_norm = _normalizar_id(id_encontrado)
            match = id_norm == alvo_norm
            print(f"  '{id_encontrado}' -> '{id_norm}' -> match: {match}")
            if match:
                return True
        
        return False
    except Exception as e:
        print(f"Erro: {e}")
        return False

def test_matching_v3():
    # Nome real do documento
    nome_doc = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    alvo = "_9565_56790_"
    
    print("=== TESTE V3 - TESTANDO TODOS OS IDs (COM E SEM UNDERSCORE FINAL) ===")
    encontrado = testar_todos_ids_v3(nome_doc, alvo)
    
    print(f"\nResultado final: {'✅ ENCONTRADO' if encontrado else '❌ NÃO ENCONTRADO'}")

if __name__ == "__main__":
    test_matching_v3()