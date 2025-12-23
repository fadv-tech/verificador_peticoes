# ==============================================================================
# modules/utils.py (v1.2 - Com Limpeza de Cache Global)
# Funções auxiliares usadas em várias partes do sistema.
#
# VERSÃO ATUALIZADA:
# - Mantém todas as funções originais e estáveis.
# - Adiciona a nova função `limpar_caches_criticos` para sincronização de dados.
# ==============================================================================
import streamlit as st
import pandas as pd

def formatar_valor(valor):
    """Formata um número como moeda brasileira (R$)."""
    if pd.isna(valor) or valor is None:
        return "N/A"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def formatar_cnj(cnj):
    """Formata uma string de 20 dígitos no padrão CNJ."""
    if not cnj or not isinstance(cnj, str):
        return cnj
    cnj_limpo = ''.join(filter(str.isdigit, cnj))
    if len(cnj_limpo) == 20:
        return f"{cnj_limpo[0:7]}-{cnj_limpo[7:9]}.{cnj_limpo[9:13]}.{cnj_limpo[13:14]}.{cnj_limpo[14:16]}.{cnj_limpo[16:20]}"
    return cnj # Retorna o original se não estiver no formato esperado

def registrar_acao(conexao, tipo_acao, detalhes=""):
    """Registra uma ação do usuário na tabela de histórico para auditoria."""
    try:
        cursor = conexao.cursor()
        cursor.execute(
            "INSERT INTO HistoricoAcoes (nome_usuario, perfil_usuario, tipo_acao, detalhes) VALUES (?, ?, ?, ?)",
            (
                st.session_state.get('username', 'N/A'),
                st.session_state.get('perfil', 'N/A'),
                tipo_acao,
                detalhes
            )
        )
        conexao.commit()
    except Exception as e:
        # Em um app real, isso deveria logar em um arquivo, não apenas printar.
        print(f"ERRO ao registrar ação de auditoria: {e}")

# ==============================================================================
# NOVA FUNÇÃO PARA LIMPEZA DE CACHE GLOBAL (Adicionada)
# ==============================================================================
def limpar_caches_criticos():
    """
    Limpa o cache das principais funções de busca de dados do sistema.
    Deve ser chamada sempre que uma alteração de status ocorrer.
    """
    # Importa as funções de busca de dados das páginas diretamente aqui
    # para evitar problemas de importação circular.
    try:
        from pages.d_1_Dashboard_Geral import buscar_credores_dashboard_com_status
        buscar_credores_dashboard_com_status.clear()
    except ImportError:
        # Se a página não existir ou o nome estiver diferente, ignora.
        # Isso torna o código mais robusto a renomeações de arquivos.
        pass
    except Exception as e:
        print(f"Erro ao tentar limpar cache do Dashboard Geral: {e}")

    try:
        from pages.b_2_Mesa_de_Negociacao import buscar_dados_kanban_por_credor
        buscar_dados_kanban_por_credor.clear()
    except ImportError:
        pass
    except Exception as e:
        print(f"Erro ao tentar limpar cache da Mesa de Negociação: {e}")
    
    st.toast("Caches do sistema atualizados!", icon="🔄")

