
from database import get_db_connection
import pandas as pd

def analyze_sqls(start_date, end_date):
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    print(f"--- Analyzing SQLs for Period: {start_date} to {end_date} ---\n")

    # 1. SQL "Total Fatura" (Label do usuario), Query: SUM(SMM_AJUSTE_VLR) WHERE type='C'
    # Nota: Isso parece ser 'Total Descontos/Ajustes Caixa', não 'Total Fatura'.
    query_1 = f"""
    SELECT 
        SUM(ISNULL(sm.SMM_AJUSTE_VLR, 0)) as val
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'C'
    AND s.str_str_cod LIKE '01%'
    """
    cursor.execute(query_1)
    val_1 = cursor.fetchone()['val'] or 0.0
    print(f"1. 'Total Fatura' (Query: Sum Adjustments Type C): R$ {val_1}")

    # 2. SQL "Total Bruto" (Label do usuario), Query: SUM(smm_vlr) WHERE type='C'
    query_2 = f"""
    SELECT 
        SUM(sm.smm_vlr) as val
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'C'
    AND s.str_str_cod LIKE '01%'
    """
    cursor.execute(query_2)
    val_2 = cursor.fetchone()['val'] or 0.0
    print(f"2. 'Total Bruto' (Query: Sum Value Type C): R$ {val_2}")

    # 3. SQL "Total Caixa + Fatura"
    # Logic: (Sum(Vlr) + Sum(Ajuste) WHERE 'C') + (Sum(Vlr) WHERE 'F')
    # Part A: Net Caixa
    val_a = val_2 + val_1 # (Bruto C + Ajuste C)
    
    # Part B: Gross Fatura (Type 'F')
    query_3b = f"""
    SELECT 
        SUM(sm.smm_vlr) as val
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'F'
    AND s.str_str_cod LIKE '01%'
    """
    cursor.execute(query_3b)
    val_3b = cursor.fetchone()['val'] or 0.0
    
    total_3 = val_a + val_3b
    
    print(f"3. 'Total Caixa + Fatura':")
    print(f"   - Caixa Liquido (Bruto C + Ajuste C): R$ {val_a} ({val_2} + {val_1})")
    print(f"   - Fatura Bruto (Type F): R$ {val_3b}")
    print(f"   - Total Final: R$ {total_3}")

    conn.close()

if __name__ == "__main__":
    analyze_sqls('2025-12-01', '2025-12-31')
