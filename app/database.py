import os
import pymssql
from dotenv import load_dotenv

from contextvars import ContextVar

load_dotenv()

# Contexto para o ID do banco (1 = Sisal, 2 = Paulo Afonso)
current_db_id: ContextVar[str] = ContextVar("current_db_id", default="1")

# Configurações do Banco 1 (Sisal - Default)
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

def get_db_connection():
    """Retorna uma conexão com o banco de dados SQL Server baseado no contexto atual."""
    db_id = current_db_id.get()
    config = DB_CONFIGS.get(db_id, DB_CONFIGS["1"])
    
    return pymssql.connect(**config)

def test_connection():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        conn.close()
        return True, version[0]
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    success, result = test_connection()
    if success:
        print(f"✅ Conexão bem-sucedida!\nVersão: {result}")
    else:
        print(f"❌ Erro de conexão: {result}")
