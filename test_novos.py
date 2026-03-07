import os
import pymssql
import json
from dotenv import load_dotenv

load_dotenv()
os.environ['TDSVER'] = '7.0'

conn = pymssql.connect(
    server=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASS'),
    database=os.getenv('DB_NAME'),
    port=int(os.getenv('DB_PORT', 1433))
)
cursor = conn.cursor(as_dict=True)

print("Query 1: PAC.pac_cnv_cod")
query = """
SELECT 
    COUNT(*) as total_novos,
    SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN 1 ELSE 0 END) as novos_particular,
    SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN 1 ELSE 0 END) as novos_convenio
FROM PAC p
LEFT JOIN CNV c ON p.pac_cnv_cod = c.cnv_cod
WHERE p.pac_dreg BETWEEN '2026-01-01 00:00:00' AND '2026-01-31 23:59:59'
"""
cursor.execute(query)
print(cursor.fetchone())

print("\nQuery 2: First OSM logic (more accurate for unit filtering later)")
query2 = """
SELECT 
    COUNT(DISTINCT p.pac_reg) as total_novos,
    SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN 1 ELSE 0 END) as novos_particular,
    SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN 1 ELSE 0 END) as novos_convenio
FROM PAC p
INNER JOIN (
    SELECT osm_pac, MIN(osm_dthr) as min_dthr, MIN(osm_cnv) as fst_cnv
    FROM OSM
    GROUP BY osm_pac
) o ON p.pac_reg = o.osm_pac AND o.min_dthr BETWEEN '2026-01-01 00:00:00' AND '2026-01-31 23:59:59'
LEFT JOIN CNV c ON o.fst_cnv = c.cnv_cod
WHERE p.pac_dreg BETWEEN '2026-01-01 00:00:00' AND '2026-01-31 23:59:59'
"""
cursor.execute(query2)
print(cursor.fetchone())

conn.close()
