"""
Helper compartilhado: executa uma query abrindo uma conexão própria.
Usado por serviços que rodam queries em paralelo via ThreadPoolExecutor.
"""
from .database import get_db_connection, release_connection


def run_query_new_conn(query: str) -> list:
    """Executa a query usando o pool de conexões por thread."""
    conn = get_db_connection()
    cur = conn.cursor(as_dict=True)
    try:
        cur.execute(query)
        return cur.fetchall()
    finally:
        release_connection(conn)
