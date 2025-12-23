#!/usr/bin/env python3
"""
Função corrigida para parsing de IDs de documentos
"""

import re

def _parse_nome_documento_corrigido(nome: str) -> dict:
    """
    Versão corrigida da função de parsing
    """
    try:
        nome = nome.strip()
        
        # NOVA ABORDAGEM: Encontrar TODOS os padrões _x_x_ e selecionar o correto
        id_info = {
            'nome': nome,
            'id': '',
            'data': '',
            'tipo': ''
        }
        
        # Encontrar todos os padrões _digitos_digitos_ (com ou sem underscore final)
        todos_padroes = re.findall(r'_\d+_\d+_?', nome)
        print(f"Todos os padrões encontrados: {todos_padroes}")
        
        # Selecionar o padrão mais provável (geralmente o último que tem estrutura válida)
        id_valido = None
        for padrao in reversed(todos_padroes):  # Começar do final
            # Verificar se é um ID válido (não muito curto, não muito longo)
            parte_numerica = padrao.strip('_')
            partes = parte_numerica.split('_')
            
            if len(partes) == 2 and len(partes[0]) >= 3 and len(partes[1]) >= 3:
                # ID válido encontrado
                id_valido = padrao
                break
        
        if id_valido:
            id_info['id'] = id_valido
            print(f"ID selecionado: {id_valido}")
        
        # Extrair data
        padrao_data = r'(\d{2})\.(\d{2})\.(\d{4})'
        data_match = re.search(padrao_data, nome)
        if data_match:
            id_info['data'] = data_match.group(0)
        
        # Identificar tipo
        tipos_comuns = ['manifestação', 'petição', 'certidão', 'despacho', 'decisão', 'cumprimento']
        nome_lower = nome.lower()
        for tipo in tipos_comuns:
            if tipo in nome_lower:
                id_info['tipo'] = tipo.capitalize()
                break
        
        return id_info
        
    except Exception as e:
        print(f"Erro: {e}")
        return {'nome': nome, 'id': '', 'data': '', 'tipo': ''}

def _normalizar_id_corrigido(texto: str) -> str:
    """
    Versão corrigida da normalização
    """
    try:
        if not texto:
            return ''
        
        # Primeiro tenta encontrar _x_x_ (com underscore final)
        ms = re.findall(r'_\d+_\d+_', texto)
        if ms:
            # Retorna o último match
            return ms[-1].strip('_')
        
        # Depois tenta _x_x (sem underscore final)
        ms2 = re.findall(r'_\d+_\d+', texto)
        if ms2:
            return ms2[-1].strip('_')
        
        # Por último, tenta x_x (sem underscores)
        ms3 = re.findall(r'\d+_\d+', texto)
        if ms3:
            return ms3[-1]
        
        return ''
    except Exception:
        return ''

def testar_correcao():
    """Testar a correção"""
    
    # Caso problemático
    nome_arquivo = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    id_sistema = "_9565_56790_"
    
    print(f"Arquivo: {nome_arquivo}")
    print(f"ID do sistema: {id_sistema}")
    print("-" * 60)
    
    # Testar parsing
    resultado = _parse_nome_documento_corrigido(nome_arquivo)
    print(f"ID parseado: '{resultado['id']}'")
    
    # Testar normalização
    id_parseado = resultado['id']
    id_normalizado = _normalizar_id_corrigido(id_parseado)
    id_sistema_normalizado = _normalizar_id_corrigido(id_sistema)
    
    print(f"ID parseado normalizado: '{id_normalizado}'")
    print(f"ID sistema normalizado: '{id_sistema_normalizado}'")
    print(f"São iguais? {id_normalizado == id_sistema_normalizado}")
    
    # Testar matching direto
    if id_parseado:
        match_direto = id_sistema in id_parseado or id_parseado in id_sistema
        print(f"Match direto: {match_direto}")
        
        # Testar match normalizado
        match_normalizado = id_normalizado == id_sistema_normalizado
        print(f"Match normalizado: {match_normalizado}")

if __name__ == "__main__":
    print("TESTE DA CORREÇÃO DO MATCHING")
    print("="*60)
    testar_correcao()