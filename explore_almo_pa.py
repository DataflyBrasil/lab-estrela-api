"""
Exploração completa de tabelas de estoque/material — Paulo Afonso (DB2) apenas.
"""
import os
os.environ['TDSVER'] = '7.0'
import pymssql
from dotenv import load_dotenv
load_dotenv(dotenv_path='/Users/rafaelborgesbezerra/Documents/Programacao/06 - Datafly/04 - Laboratório Estrela/backend/.env')

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
print("✅ Conectado ao DB Paulo Afonso (2)")

def run(label, q, max_rows=10):
    print(f"\n{'='*65}\n  {label}\n{'='*65}")
    try:
        c.execute(q)
        rows = c.fetchall()
        cols = [d[0] for d in c.description]
        if not rows:
            print("  (sem resultados / tabela vazia)")
        else:
            for row in rows[:max_rows]:
                print("  " + " | ".join(f"{cols[i]}={str(v or '')[:40]}" for i,v in enumerate(row)))
    except Exception as e:
        print(f"  ERRO: {e}")

# 1. Contagens gerais para confirmar estado das tabelas core
run("CONTAGEM NAS TABELAS DE ESTOQUE", """
SELECT 
    (SELECT COUNT(*) FROM MAT WITH(NOLOCK)) AS total_mat,
    (SELECT COUNT(*) FROM mat_str WITH(NOLOCK)) AS total_mat_str,
    (SELECT ISNULL(CAST(SUM(mat_str_qtd) AS INT), 0) FROM mat_str WITH(NOLOCK)) AS soma_saldo_geral,
    (SELECT COUNT(*) FROM DISPENSA_MATERIAL WITH(NOLOCK)) AS total_dispensa,
    (SELECT COUNT(*) FROM DEVOLUCAO_MATERIAL WITH(NOLOCK)) AS total_devolucao
""")

# 2. mat_str — saldo por unidade/almoxarifado (mais recentes)
run("MAT_STR — SALDO POR UNIDADE (10 mais recentes)", """
SELECT TOP 10 
    mat_str_mat_cod, mat_str_str_cod, mat_str_sba_cod,
    mat_str_qtd, mat_str_lot_num, mat_str_dtcri, mat_str_dtalt
FROM mat_str WITH(NOLOCK)
ORDER BY mat_str_dtalt DESC
""")

# 3. MAT com campos de estoque min/max e curva ABC
run("MAT — ITENS COM ESTOQUE MIN/MAX DEFINIDO", """
SELECT TOP 15 
    MAT_COD, MAT_DESC_RESUMIDA, mat_sba_cod,
    mat_est_min, mat_est_max, MAT_CONS_MEDIO,
    MAT_IND_CURVA_ABC, MAT_IND_CRITICIDADE
FROM MAT WITH(NOLOCK)
WHERE mat_est_min IS NOT NULL AND mat_est_min > 0
ORDER BY MAT_IND_CURVA_ABC, MAT_COD
""")

# 4. MAT com todos os campos de custo e controle
run("MAT — CAMPOS DE CUSTO E CONTROLE", """
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'MAT' 
ORDER BY ORDINAL_POSITION
""")

# 5. DISPENSA_MATERIAL — estrutura completa
run("DISPENSA_MATERIAL — TODAS AS COLUNAS", """
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'DISPENSA_MATERIAL' 
ORDER BY ORDINAL_POSITION
""")

# 6. DEVOLUCAO_MATERIAL — estrutura
run("DEVOLUCAO_MATERIAL — COLUNAS", """
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'DEVOLUCAO_MATERIAL' 
ORDER BY ORDINAL_POSITION
""")

# 7. Busca ampla por outras tabelas de movimentação
run("TABELAS COM 'MAT' NO NOME", """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%mat%' OR TABLE_NAME LIKE '%MAT%'
ORDER BY TABLE_NAME
""")

# 8. api_atualizacao_est — validade e lotes com dados recentes
run("API_ATUALIZACAO_EST — TOP 10 MAIS RECENTES", """
SELECT TOP 10 *
FROM api_atualizacao_est WITH(NOLOCK)
ORDER BY api_atualizacao_est_dt_atu DESC
""")

# 9. mat_str saldo por sub-almoxarifado
run("MAT_STR — SALDO AGRUPADO POR SUB-ALMOXARIFADO", """
SELECT 
    mat_str_sba_cod AS sub_almox,
    mat_str_str_cod AS unidade,
    COUNT(*) AS qtd_itens,
    SUM(mat_str_qtd) AS saldo_total
FROM mat_str WITH(NOLOCK)
GROUP BY mat_str_sba_cod, mat_str_str_cod
ORDER BY sub_almox, unidade
""")

# 10. Busca por tabelas de lote
run("TABELAS COM 'LOT' NO NOME", """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%lot%' OR TABLE_NAME LIKE '%LOT%'
ORDER BY TABLE_NAME
""")

# 11. Busca por tabelas de requisição/pedido/fornecedor
run("TABELAS DE REQ, PEDIDO, FORNECEDOR", """
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE '%req%' OR TABLE_NAME LIKE '%REQ%'
   OR TABLE_NAME LIKE '%ped%' OR TABLE_NAME LIKE '%PED%'
   OR TABLE_NAME LIKE '%for%' OR TABLE_NAME LIKE '%FOR%'
   OR TABLE_NAME LIKE '%forn%' OR TABLE_NAME LIKE '%FORN%'
ORDER BY TABLE_NAME
""")

conn.close()
print("\n✅ Exploração concluída.")
