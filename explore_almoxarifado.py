"""
Script de exploração do banco de dados Paulo Afonso (DB 2)
para identificar tabelas relacionadas a almoxarifado/estoque.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

import pymssql

DB2 = {
    "server": os.getenv('DB_HOST_PAULO_AFONSO', '192.168.1.251'),
    "database": os.getenv('DB_NAME_PAULO_AFONSO', 'smart'),
    "user": os.getenv('DB_USER_PAULO_AFONSO', 'sa'),
    "password": os.getenv('DB_PASS_PAULO_AFONSO', 'sa'),
    "port": int(os.getenv('DB_PORT_PAULO_AFONSO', 1433)),
    "timeout": 90
}

os.environ['TDSVER'] = '7.0'

def run(cursor, label, query):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        if not rows:
            print("  (sem resultados)")
        else:
            # Header
            widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
            header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
            print("  " + header)
            print("  " + "-" * len(header))
            for row in rows:
                print("  " + " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row)))
    except Exception as e:
        print(f"  ERRO: {e}")

conn = pymssql.connect(**DB2)
cursor = conn.cursor()

# 1. Todas as tabelas que contêm palavras-chave de estoque/almoxarifado
run(cursor, "TABELAS RELACIONADAS A ESTOQUE/ALMOXARIFADO", """
    SELECT TABLE_NAME, TABLE_TYPE
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
      AND (
          TABLE_NAME LIKE '%ALM%'
          OR TABLE_NAME LIKE '%EST%'
          OR TABLE_NAME LIKE '%MAT%'
          OR TABLE_NAME LIKE '%INS%'
          OR TABLE_NAME LIKE '%PRD%'
          OR TABLE_NAME LIKE '%REQ%'
          OR TABLE_NAME LIKE '%MOV%'
          OR TABLE_NAME LIKE '%LTE%'
          OR TABLE_NAME LIKE '%FOR%'
          OR TABLE_NAME LIKE '%REC%'
          OR TABLE_NAME LIKE '%SAI%'
          OR TABLE_NAME LIKE '%ENT%'
          OR TABLE_NAME LIKE '%PED%'
          OR TABLE_NAME LIKE '%NF%'
          OR TABLE_NAME LIKE '%NOT%'
      )
    ORDER BY TABLE_NAME
""")

# 2. Listar TODAS as tabelas do banco para ter uma visão completa
run(cursor, "TODAS AS TABELAS DO BANCO (visão geral)", """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
""")

conn.close()
print("\n✅ Exploração concluída.")
