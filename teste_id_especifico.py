#!/usr/bin/env python3
"""
Script de teste para encontrar o padrão _9565_56790_ especificamente
"""

import re

def encontrar_id_especifico():
    filename = "id_484246117_doc._00_5188032_43_2019_8_09_0152_9565_56790_manifestacao.pdf"
    target_id = "_9565_56790_"
    
    print(f"Arquivo: {filename}")
    print(f"ID alvo: {target_id}")
    print("-" * 60)
    
    # O ID está no final antes de "manifestacao"
    # Vamos procurar especificamente por _9565_56790
    
    # Pattern que captura exatamente _9565_56790
    pattern_especifico = r'_9565_56790_?'
    match = re.search(pattern_especifico, filename)
    
    if match:
        print(f"✓ ID encontrado: {match.group()}")
        return match.group()
    else:
        print("✗ ID não encontrado")
        
    # Testar pattern mais geral que capture o último _x_x_ antes do texto final
    pattern_ultimo = r'(\d+_\d+)_\w+\.pdf$'
    match = re.search(pattern_ultimo, filename)
    
    if match:
        id_candidato = f"_{match.group(1)}_"
        print(f"Candidato encontrado: {id_candidato}")
        if "9565_56790" in id_candidato:
            print("✓ Este é o ID correto!")
            return id_candidato
    
    # Pattern para encontrar todos os _x_x_ e identificar o correto
    all_matches = list(re.finditer(r'_\d+_\d+_?', filename))
    print(f"\nTodos os matches: {[m.group() for m in all_matches]}")
    
    # O ID correto deve ter 4-5 dígitos _ 4-5 dígitos
    for match in all_matches:
        id_part = match.group()
        # Verificar se tem a estrutura correta
        parts = id_part.strip('_').split('_')
        if len(parts) == 2 and len(parts[0]) >= 3 and len(parts[1]) >= 3:
            print(f"ID válido encontrado: {id_part}")
            if "9565" in id_part and "56790" in id_part:
                print("✓ Este é o ID que procuramos!")
                return id_part
    
    return None

def testar_funcao_normalizacao():
    """Testar como deve funcionar a normalização"""
    print(f"\n{'='*60}")
    print("Testando função de normalização:")
    
    def normalizar_id(id_str):
        """Normaliza ID para comparação"""
        # Remove underscores extras e mantém apenas números e underscore entre eles
        return id_str.strip('_').replace('_', '_')
    
    def comparar_ids(id1, id2):
        """Compara dois IDs normalizando"""
        norm1 = normalizar_id(id1)
        norm2 = normalizar_id(id2)
        return norm1 == norm2
    
    # Testes
    id_sistema = "_9565_56790_"
    id_arquivo = "_9565_56790"
    
    print(f"ID do sistema: '{id_sistema}' -> normalizado: '{normalizar_id(id_sistema)}'")
    print(f"ID do arquivo: '{id_arquivo}' -> normalizado: '{normalizar_id(id_arquivo)}'")
    print(f"São iguais? {comparar_ids(id_sistema, id_arquivo)}")

if __name__ == "__main__":
    print("TESTE ESPECÍFICO PARA _9565_56790_")
    print("="*60)
    
    resultado = encontrar_id_especifico()
    testar_funcao_normalizacao()
    
    print(f"\n{'='*60}")
    print("CONCLUSÃO:")
    if resultado:
        print(f"✓ ID encontrado: {resultado}")
        print("✓ A correção deve usar pattern específico ou validação por partes")
    else:
        print("✗ Necessário revisar lógica completa")