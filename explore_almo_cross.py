"""
Verificação cruzada dos dois bancos (Sisal DB1 + Paulo Afonso DB2)
para tabelas de estoque, saldo e movimentação de material.
"""
import os
os.environ['TDSVER'] = '7.0'
import pymssql
from dotenv import load_dotenv
load_dotenv(dotenv_path='/Users/rafaelborgesbezerra/Documents/Programacao/06 - Datafly/04 - Laboratório Estrela/backend/.env')

DB_CONFIGS = {
    "1 (Sisal)": {
        "server": os.getenv('DB_HOST', 'labestrela.fmddns.com'),
        "database": os.getenv('DB_NAME', 'smart'),
        "user": os.getenv('DB_USER', 'sa'),
        "password": os.getenv('DB_PASS', 'sa'),
        "port": int(os.getenv('DB_PORT', 1433)),
        "timeout": 90
    },
    "2 (Paulo Afonso)": {
        "server": os.getenv('DB_HOST_PAULO_AFONSO', '192.168.1.251'),
        "database": os.getenv('DB_NAME_PAULO_AFONSO', 'smart'),
        "user": os.getenv('DB_USER_PAULO_AFONSO', 'sa'),
        "password": os.getenv('DB_PASS_PAULO_AFONSO', 'sa'),
        "port": int(os.getenv('DB_PORT_PAULO_AFONSO', 1433)),
        "timeout": 90
    }
}

def run_on_db(conn, label, q, max_rows=8):
    print(f"\n  -- {label} --")
    try:
        c = conn.cursor()
        c.execute(q)
        rows = c.fetchall()
        cols = [d[0] for d in c.description]
        if not rows:
            print("     (sem resultados / tabela vazia)")
        else:
            for row in rows[:max_rows]:
                print("     " + " | ".join(f"{cols[i]}={str(v or '')[:40]}" for i,v in enumerate(row)))
    except Exception as e:
        print(f"     ERRO: {e}")

conns = {}
for db_label, cfg in DB_CONFIGS.items():
    try:
        conns[db_label] = pymssql.connect(**cfg)
        print(f"\n✅ Conectado ao DB {db_label}")
    except Exception as e:
        print(f"\n❌ Falha DB {db_label}: {e}")

print("\n" + "="*70)
print("  VERIFICAÇÃO DE SALDOS E MOVIMENTAÇÕES — AMBOS OS BANCOS")
print("="*70)

# === 1. Contagem nas tabelas core de estoque ===
COUNT_QUERY = """
    SELECT 
        (SELECT COUNT(*) FROM MAT WITH(NOLOCK)) AS total_mat,
        (SELECT COUNT(*) FROM mat_str WITH(NOLOCK)) AS total_mat_str_saldo,
        (SELECT ISNULL(SUM(mat_str_qtd), 0) FROM mat_str WITH(NOLOCK)) AS soma_saldo_geral,
        (SELECT COUNT(*) FROM DISPENSA_MATERIAL WITH(NOLOCK)) AS total_dispensa,
        (SELECT COUNT(*) FROM DEVOLUCAO_MATERIAL WITH(NOLOCK)) AS total_devolucao
"""
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] Contagem de registros nas tabelas de estoque:")
    run_on_db(conn, "Contagens", COUNT_QUERY)

# === 2. Verificar estrutura completa de DISPENSA_MATERIAL ===
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] DISPENSA_MATERIAL — Colunas:")
    run_on_db(conn, "Colunas", """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='DISPENSA_MATERIAL' ORDER BY ORDINAL_POSITION
    """)

# === 3. Varrer tabelas SMK e SMM para movimentação de material ===
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] Tabelas com SMM_MAT / SMK_MAT:")
    run_on_db(conn, "SMM e SMK relacionados", """
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME LIKE '%smm%mat%' OR TABLE_NAME LIKE '%smk%mat%' OR TABLE_NAME LIKE '%mat%mov%'
        ORDER BY TABLE_NAME
    """)

# === 4. mat_str sample — where it has data ===
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] mat_str — últimos registros:")
    run_on_db(conn, "mat_str TOP 10 mais recentes", """
        SELECT TOP 10 
            mat_str_mat_cod, mat_str_str_cod, mat_str_sba_cod,
            mat_str_qtd, mat_str_lot_num, mat_str_dtcri, mat_str_dtalt, mat_str_cod_barra
        FROM mat_str WITH(NOLOCK)
        ORDER BY mat_str_dtalt DESC
    """)

# === 5. Buscar tabelas adicionais de saída / requisição de material ===
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] Tabelas relacionadas a REQ/RES/SAI de material:")
    run_on_db(conn, "Tabelas de movimentação", """
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME LIKE '%req%' OR TABLE_NAME LIKE '%REQ%'
           OR TABLE_NAME LIKE '%sai%mat%' OR TABLE_NAME LIKE '%SAI%MAT%'
           OR TABLE_NAME LIKE '%ent%mat%' OR TABLE_NAME LIKE '%ENT%MAT%'
           OR TABLE_NAME LIKE '%mov%mat%' OR TABLE_NAME LIKE '%MOV%MAT%'
           OR TABLE_NAME LIKE '%PEDIDO%' OR TABLE_NAME LIKE '%pedido%'
           OR TABLE_NAME LIKE '%REQUISIC%' OR TABLE_NAME LIKE '%requisic%'
        ORDER BY TABLE_NAME
    """)

# === 6. smk table — might be related to kits/material ===
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] SMK famíla de tabelas (kit/material?):")
    run_on_db(conn, "SMK estrutura parcial", """
        SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='smk' 
        ORDER BY ORDINAL_POSITION
    """)

# === 7. Lote de materiais — tabela LOT ===
for db_label, conn in conns.items():
    print(f"\n[DB {db_label}] MAT — amostra com estoque min/max:")
    run_on_db(conn, "MAT est_min / est_max", """
        SELECT TOP 10 
            MAT_COD, MAT_DESC_RESUMIDA, mat_sba_cod, mat_est_min, mat_est_max, MAT_CONS_MEDIO,
            MAT_IND_CURVA_ABC, MAT_IND_CRITICIDADE
        FROM MAT WITH(NOLOCK)
        WHERE mat_est_min IS NOT NULL AND mat_est_min > 0
        ORDER BY MAT_IND_CURVA_ABC, MAT_COD
    """)

for conn in conns.values():
    conn.close()
print("\n✅ Exploração cruzada concluída.")
