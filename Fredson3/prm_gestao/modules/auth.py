# ==============================================================================
# modules/auth.py
# Funções relacionadas à autenticação, como criação e verificação de hashes
# de senha.
# ==============================================================================
import hashlib

def criar_hash_senha(senha, salt):
    """Gera o hash de uma senha usando um salt existente."""
    return hashlib.sha256((senha + salt).encode('utf-8')).hexdigest()

def verificar_login(conexao, nome_usuario, senha):
    """Verifica as credenciais do usuário no banco de dados."""
    cursor = conexao.cursor()
    cursor.execute("SELECT senha_hash, senha_salt, perfil FROM Usuarios WHERE nome_usuario = ?", (nome_usuario,))
    resultado = cursor.fetchone()
    if resultado:
        senha_hash, senha_salt, perfil = resultado
        hash_tentativa = criar_hash_senha(senha, senha_salt)
        if hash_tentativa == senha_hash:
            return True, nome_usuario, perfil
    return False, None, None
