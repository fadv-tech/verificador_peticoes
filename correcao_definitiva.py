#!/usr/bin/env python3
"""
Função corrigida definitiva para encontrar o ID correto
"""

import re

def _parse_nome_documento_definitivo(nome: str, id_sistema: str = None) -> dict:
    """
    Versão definitiva que pode usar o ID do sistema como referência
    """
    try:
        nome = nome.strip()
        
        id_info = {
            'nome': nome,
            'id': '',
            'data': '',
            'tipo': ''
        }
        
        # Se tivermos um ID de sistema, procurar especificamente por ele
        if id_sistema:
            # Procurar o ID do sistema no nome (com ou sem underscore final)
            id_sem_underscore = id_sistema.strip('_')
            
            # Pattern que captura o ID com variações de underscore
            pattern_com_underscore = re.escape(id_sistema)
            pattern_sem_underscore = re.escape(f"_{id_sem_underscore}")
            pattern_final = re.escape(f"_{id_sem_underscore}_")
            
            # Tentar encontrar qualquer variação
            for pattern in [pattern_com_underscore, pattern_sem_underscore, pattern_final]:
                match = re.search(pattern, nome)
                if match:
                    id_info['id'] = match.group()
                    print(f"ID encontrado pelo pattern {pattern}: {match.group()}")
                    break
            
            # Se não encontrou com os patterns exatos, tenta encontrar partes
            if not id_info['id']:
                # Procurar por _x_x_ onde x são números que aparecem no ID do sistema
                partes_id = id_sem_underscore.split('_')
                if len(partes_id) == 2:
                    # Pattern que captura qualquer _num1_num2_ onde num1 e num2 estão no ID
                    pattern_generico = rf'_\d+_\d+_?'
                    todos_matches = re.findall(pattern_generico, nome)
                    
                    for match in todos_matches:
                        if partes_id[0] in match and partes_id[1] in match:
                            id_info['id'] = match
                            print(f"ID encontrado por matching parcial: {match}")
                            break
        
        # Se ainda não encontrou, usar lógica original mas mais inteligente
        if not id_info['id']:
            todos_padroes = re.findall(r'_\d+_\d+_?', nome)
            print(f"Todos os padrões: {todos_padroes}")
            
            # Selecionar o mais provável (último com estrutura válida)
            for padrao in reversed(todos_padroes):
                parte_numerica = padrao.strip('_')
                partes = parte_numerica.split('_')
                
                # Critérios para ID válido: ambos os números devem ter 3+ dígitos
                if len(partes) == 2 and len(partes[0]) >= 3 and len(partes[1]) >= 3:
                    # Priorizar números que não parecem datas (não começam com 0 se for ano)
                    if not (partes[0].startswith('0') and len(partes[0]) == 4):  # Não é ano provavelmente
                        id_info['id'] = padrao
                        break
        
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

def testar_definitivo():
    """Testar a versão definitiva"""
    
    nome_arquivo = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    id_sistema = "_9565_56790_"
    
    print(f"Arquivo: {nome_arquivo}")
    print(f"ID do sistema: {id_sistema}")
    print("-" * 60)
    
    # Testar com ID do sistema
    resultado = _parse_nome_documento_definitivo(nome_arquivo, id_sistema)
    print(f"Resultado com ID sistema: {resultado}")
    
    # Testar matching
    id_parseado = resultado['id']
    if id_parseado:
        # Comparação direta
        match_direto = id_sistema.strip('_') in id_parseado.strip('_')
        print(f"Match direto (ignorando underscores): {match_direto}")
        
        # Comparação normalizada
        id_parseado_norm = id_parseado.strip('_')
        id_sistema_norm = id_sistema.strip('_')
        match_normalizado = id_parseado_norm == id_sistema_norm
        print(f"Match normalizado: {match_normalizado}")

if __name__ == "__main__":
    print("TESTE DA VERSÃO DEFINITIVA")
    print("="*60)
    testar_definitivo()