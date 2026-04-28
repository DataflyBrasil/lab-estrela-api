"""
Script de exploração profunda das tabelas MAT, MTE, NFD_NFE
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

conn = pymssql.connect(**DB2)
c = conn.cursor()

def run(label, query):
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    try:
        c.execute(query)
        rows = c.fetchall()
        cols = [d[0] for d in c.description]
        if not rows:
            print("  (sem resultados)")
        else:
            for row in rows[:10]:
                print("  " + " | ".join(f"{cols[i]}={str(v or '')[:40]}" for i, v in enumerate(row)))
    except Exception as e:
        print(f"  ERRO: {e}")

# 1. MAT - Cadastro de materiais
run("MAT - COLUNAS", """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MAT'
    ORDER BY ORDINAL_POSITION
""")
run("MAT - AMOSTRA", "SELECT TOP 5 * FROM MAT WITH(NOLOCK)")

# 2. MTE - Movimentação de Estoque
run("MTE - COLUNAS", """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'MTE'
    ORDER BY ORDINAL_POSITION
""")
run("MTE - AMOSTRA", "SELECT TOP 5 * FROM MTE WITH(NOLOCK)")

# 3. NFD_NFE - Nota Fiscal de Entrada
run("NFD_NFE - COLUNAS", """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'nfd_nfe'
    ORDER BY ORDINAL_POSITION
""")
run("NFD_NFE - AMOSTRA", "SELECT TOP 3 * FROM nfd_nfe WITH(NOLOCK)")

# 4. NFD_NFE_ITENS
run("NFD_NFE_ITENS - COLUNAS", """
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'nfd_nfe_itens'
    ORDER BY ORDINAL_POSITION
""")
run("NFD_NFE_ITENS - AMOSTRA", "SELECT TOP 3 * FROM nfd_nfe_itens WITH(NOLOCK)")

# 5. mat_str - Saldo por unidade
run("MAT_STR - AMOSTRA", "SELECT TOP 10 * FROM mat_str WITH(NOLOCK)")

# 6. Contagens gerais
run("CONTAGENS DE REGISTROS", """
    SELECT 
        (SELECT COUNT(*) FROM MAT WITH(NOLOCK)) AS total_mat,
        (SELECT COUNT(*) FROM mat_str WITH(NOLOCK)) AS total_mat_str,
        (SELECT COUNT(*) FROM nfd_nfe WITH(NOLOCK)) AS total_nfd_nfe,
        (SELECT COUNT(*) FROM nfd_nfe_itens WITH(NOLOCK)) AS total_nfd_nfe_itens,
        (SELECT COUNT(*) FROM MTE WITH(NOLOCK)) AS total_mte
""")

conn.close()
print("\n✅ Exploração concluída.")
