#!/usr/bin/env python3
"""
Fix para o matching de identificadores - versão corrigida
"""
import re

def _normalizar_id(texto: str) -> str:
    """Normaliza ID para comparação"""
    try:
        if not texto:
            return ''
        # Procura padrão _NUMERO_NUMERO_ (ex: _9565_56790_)
        ms = re.findall(r'_\d+_\d+_', texto)
        if ms:
            # Pega o último match (mais próximo do final do nome)
            ultimo = ms[-1]
            # Remove os underscores externos para comparação
            return ultimo.strip('_')
        return ''
    except Exception:
        return ''

def _parse_nome_documento(nome: str) -> dict:
    """Parse nome do documento"""
    try:
        nome = nome.strip()
        id_info = {
            'nome': nome,
            'id': '',
            'data': '',
            'tipo': ''
        }
        
        # Procura por padrões de identificadores _NUMERO_NUMERO_
        padrao_id = r'_\d+_\d+_'
        matches = re.findall(padrao_id, nome)
        
        if matches:
            # Pega o último match (mais relevante)
            id_info['id'] = matches[-1]
        
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
        print(f"Erro ao parsear: {e}")
        return {'nome': nome, 'id': '', 'data': '', 'tipo': ''}

def test_matching():
    # Nome real do documento
    nome_doc = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    alvo = "_9565_56790_"
    
    print(f"Documento: {nome_doc}")
    print(f"Alvo: {alvo}")
    print()
    
    # Parse do documento
    parsed = _parse_nome_documento(nome_doc)
    print(f"ID parseado: '{parsed['id']}'")
    
    # Normalização
    alvo_norm = _normalizar_id(alvo)
    doc_norm = _normalizar_id(parsed['id'])
    
    print(f"Alvo normalizado: '{alvo_norm}'")
    print(f"Doc normalizado: '{doc_norm}'")
    print(f"Match: {doc_norm == alvo_norm}")
    
    # Teste adicional: verificar se existe no nome
    if alvo in nome_doc:
        print(f"✅ Identificador '{alvo}' encontrado no nome do documento")
    else:
        print(f"❌ Identificador '{alvo}' NÃO encontrado no nome")

if __name__ == "__main__":
    test_matching()