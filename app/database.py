import os
import threading
import pymssql
from dotenv import load_dotenv
from contextvars import ContextVar

load_dotenv()

# Contexto para o ID do banco (1 = Sisal, 2 = Paulo Afonso)
current_db_id: ContextVar[str] = ContextVar("current_db_id", default="1")

DB_CONFIGS = {
    "1": {
        "server": os.getenv('DB_HOST', 'labestrela.fmddns.com'),
        "database": os.getenv('DB_NAME', 'smart'),
        "user": os.getenv('DB_USER', 'sa'),
        "password": os.getenv('DB_PASS', 'sa'),
        "port": int(os.getenv('DB_PORT', 1433)),
        "timeout": 90
    },
    "2": {
        "server": os.getenv('DB_HOST_PAULO_AFONSO', '192.168.1.251'),
        "database": os.getenv('DB_NAME_PAULO_AFONSO', 'smart'),
        "user": os.getenv('DB_USER_PAULO_AFONSO', 'sa'),
        "password": os.getenv('DB_PASS_PAULO_AFONSO', 'sa'),
        "port": int(os.getenv('DB_PORT_PAULO_AFONSO', 1433)),
        "timeout": 90
    }
}

# Driver legado exige TDS 7.0
os.environ['TDSVER'] = '7.0'

# ---------------------------------------------------------------------------
# Connection pool simples por thread e por banco.
# Cada thread de worker do uvicorn/gunicorn reutiliza sua própria conexão,
# evitando o overhead de TCP handshake + autenticação a cada request.
# ---------------------------------------------------------------------------
_local = threading.local()

def _is_connection_alive(conn) -> bool:
    """Verifica se a conexão ainda está ativa sem levantar exceção ao caller."""
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False

def get_db_connection():
    """
    Retorna uma conexão reutilizável por thread para o banco selecionado.
    Cria uma nova conexão apenas na primeira chamada da thread ou após falha.
    """
    db_id = current_db_id.get()
    attr = f"conn_{db_id}"
    conn = getattr(_local, attr, None)

    if conn is None or not _is_connection_alive(conn):
        config = DB_CONFIGS.get(db_id, DB_CONFIGS["1"])
        conn = pymssql.connect(**config)
        setattr(_local, attr, conn)

    return conn

def release_connection(conn):
    """
    Em vez de fechar a conexão após cada request, apenas faz rollback
    para limpar qualquer transação pendente e mantém o socket aberto.
    """
    try:
        conn.rollback()
    except Exception:
        # Conexão corrompida — será recriada no próximo get_db_connection()
        db_id = current_db_id.get()
        attr = f"conn_{db_id}"
        setattr(_local, attr, None)

def test_connection():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        return True, version[0]
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    success, result = test_connection()
    if success:
        print(f"✅ Conexão bem-sucedida!\nVersão: {result}")
    else:
        print(f"❌ Erro de conexão: {result}")
