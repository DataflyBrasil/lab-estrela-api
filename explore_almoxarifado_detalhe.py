"""
Script de exploração DETALHADA das tabelas de almoxarifado/estoque
no banco de dados Paulo Afonso (DB 2).
"""
import os
os.environ['TDSVER'] = '7.0'
import pymssql
from dotenv import load_dotenv
load_dotenv()

DB2 = {
    "server": os.getenv('DB_HOST_PAULO_AFONSO', '192.168.1.251'),
    "database": os.getenv('DB_NAME_PAULO_AFONSO', 'smart'),
    "user": os.getenv('DB_USER_PAULO_AFONSO', 'sa'),
    "password": os.getenv('DB_PASS_PAULO_AFONSO', 'sa'),
    "port": int(os.getenv('DB_PORT_PAULO_AFONSO', 1433)),
    "timeout": 90
}

def run(cursor, label, query):
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        if not rows:
            print("  (sem resultados)")
        else:
            widths = [max(len(str(c)), max((len(str(r[i] or '')) for r in rows), default=0)) for i, c in enumerate(cols)]
            header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
            print("  " + header)
            print("  " + "-" * len(header))
            for row in rows:
                print("  " + " | ".join(str(v or '').ljust(widths[i]) for i, v in enumerate(row)))
    except Exception as e:
        print(f"  ERRO: {e}")

conn = pymssql.connect(**DB2)
cursor = conn.cursor()

TABLES_TO_INSPECT = [
    'MAT', 'est', 'CADASTRO_MATERIAIS', 'CAD_CATMAT',
    'MMT_MAT', 'itm_mat', 'mat_str', 'mat_unm',
    'DISPENSA_MATERIAL', 'DEVOLUCAO_MATERIAL', 'CENTRO_CONSUMO',
    'ped', 'nfd_nfe', 'nfd_nfe_itens', 'img_mat', 'MB_INS'
]

for tbl in TABLES_TO_INSPECT:
    run(cursor, f"ESTRUTURA: {tbl}", f"""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{tbl}'
        ORDER BY ORDINAL_POSITION
    """)

# Sample data from key tables
SAMPLE_TABLES = ['MAT', 'est', 'MMT_MAT', 'DISPENSA_MATERIAL', 'ped']
for tbl in SAMPLE_TABLES:
    run(cursor, f"AMOSTRA (5 linhas): {tbl}", f"SELECT TOP 5 * FROM {tbl} WITH(NOLOCK)")

conn.close()
print("\n✅ Exploração detalhada concluída.")
