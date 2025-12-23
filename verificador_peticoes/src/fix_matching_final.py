#!/usr/bin/env python3
"""
Fix final para o matching de identificadores
Captura _9565_56790 mesmo quando está seguido por texto
"""
import re

def extrair_todos_ids(nome: str) -> list:
    """Extrai TODOS os identificadores possíveis do nome"""
    try:
        # Padrões para capturar:
        # 1. _numero_numero_ (com underscores)
        # 2. _numero_numero (sem underscore final, seguido por texto ou fim)
        # 3. numero_numero (sem underscores, mas entre separadores)
        
        # Primeiro captura todos os _x_x_ explicitos
        explicitos = re.findall(r'_\d+_\d+_', nome)
        
        # Depois captura _x_x que estão no final ou seguidos por texto
        # Lookahead para garantir que tem números e underscore
        implicitos = re.findall(r'_\d+_\d+(?=_?[a-zA-Z]|$)', nome)
        
        # Combina e remove duplicatas
        todos = list(set(explicitos + implicitos))
        
        # Ordena por posição no texto original
        todos.sort(key=lambda x: nome.find(x))
        
        return todos
    except Exception:
        return []

def _normalizar_id(texto: str) -> str:
    """Normaliza ID para comparação"""
    try:
        if not texto:
            return ''
        # Remove underscores externos
        return texto.strip('_')
    except Exception:
        return ''

def testar_matching_final(nome: str, alvo: str) -> bool:
    """Testa se encontra o alvo nos IDs do nome"""
    try:
        todos_ids = extrair_todos_ids(nome)
        alvo_norm = _normalizar_id(alvo)
        
        print(f"Nome: {nome}")
        print(f"Alvo: {alvo} -> normalizado: '{alvo_norm}'")
        print(f"IDs encontrados: {todos_ids}")
        
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

def test_final():
    # Nome real do documento
    nome_doc = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    alvo = "_9565_56790_"
    
    print("=== TESTE FINAL ===")
    encontrado = testar_matching_final(nome_doc, alvo)
    
    print(f"\nResultado final: {'✅ ENCONTRADO' if encontrado else '❌ NÃO ENCONTRADO'}")

if __name__ == "__main__":
    test_final()