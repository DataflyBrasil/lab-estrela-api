
from database import get_db_connection
import pandas as pd

def validate_official_sql(start_date, end_date):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # Implementing the Official SQL logic in Python
    # Note: Optimizing by fetching necessary columns and joining in Pandas or SQL
    # SQL is better for initial filtering
    
    query = f"""
    SELECT 
        SUM(sm.smm_vlr) as total_bruto
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s ON o.osm_str = s.str_cod
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    -- LEFT JOIN MTE m -- Used for discount in original SQL, but we want Gross Sum here first
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'C'
    -- AND s.str_str_cod = '01 ' -- User SQL had trailing space '01 '
    -- Let's filter strict '01 ' or '01'
    AND s.str_str_cod LIKE '01%'
    """
    
    print("--- Executing Official SQL Logic ---")
    cursor.execute(query)
    row = cursor.fetchone()
    print(f"Total Bruto (Official Logic): R$ {row['total_bruto']}")

    # Check sum WITHOUT constraints to see what we are missing
    print("\n--- Diagnostic: Total SMM without 'C' filter ---")
    query_all = f"""
    SELECT SUM(sm.smm_vlr) as total
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    WHERE o.osm_dthr {date_filter}
    and (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    """
    cursor.execute(query_all)
    print(f"Total SMM (All): R$ {cursor.fetchone()['total']}")
    
    # Check 'F' items
    print("\n--- Diagnostic: Total SMM with 'F' (Fatura) ---")
    query_f = f"""
    SELECT SUM(sm.smm_vlr) as total
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    WHERE o.osm_dthr {date_filter}
    and (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'F'
    """
    cursor.execute(query_f)
    print(f"Total SMM (F): R$ {cursor.fetchone()['total']}")
    
    conn.close()

if __name__ == "__main__":
    # Test for Dec 2025
    validate_official_sql('2025-12-01', '2025-12-31')
