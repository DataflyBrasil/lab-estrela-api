"""
Helper compartilhado: executa uma query abrindo uma conexão própria.
Usado por serviços que rodam queries em paralelo via ThreadPoolExecutor.
"""
from .database import get_db_connection


def run_query_new_conn(query: str) -> list:
    """Abre uma conexão dedicada, executa a query e retorna as linhas como lista de dicts."""
    conn = get_db_connection()
    cur = conn.cursor(as_dict=True)
    try:
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()
