#!/usr/bin/env python3
"""
Debug rápido do matching de identificadores
"""
import re

def _normalizar_id(texto: str) -> str:
    """Cópia da função do extrator"""
    try:
        if not texto:
            return ''
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

def test_normalizacao():
    # Testes
    testes = [
        "_9565_56790_",  # Alvo
        "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf",
        "_9565_56790_manifestacao.pdf",
        "9565_56790",
        "doc_9565_56790_teste.pdf",
        "id_9565_56790_",
    ]
    
    alvo = "_9565_56790_"
    alvo_norm = _normalizar_id(alvo)
    
    print(f"Alvo: '{alvo}' -> normalizado: '{alvo_norm}'")
    print()
    
    for t in testes:
        norm = _normalizar_id(t)
        match = norm == alvo_norm
        print(f"'{t}' -> '{norm}' -> match: {match}")

if __name__ == "__main__":
    test_normalizacao()