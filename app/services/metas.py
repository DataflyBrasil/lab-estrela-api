import pandas as pd
from app.database import current_db_id
import logging

logger = logging.getLogger(__name__)

def get_monthly_execution(cursor, unidade: str = None):
    """
    Busca a execução mensal para o ano atual, utilizando Pandas para agregação.
    """
    db_id = current_db_id.get()
    unit_prefix = "01%" if db_id == "1" else "04%"
    
    unit_where = f"AND s.str_str_cod LIKE '{unit_prefix}'"
    if unidade:
        if "PAULO AFONSO" in unidade.upper():
            logger.info(f"Unidade '{unidade}' identificada como região. Retornando consolidado PA.")
        else:
            unit_where += f" AND s.str_nome LIKE '%{unidade.strip()}%'"

    # Buscamos dados diários simplificados para consolidar em Python
    query = f"""
    SELECT 
        CAST(o.osm_dthr AS DATE) as date,
        c.cnv_caixa_fatura,
        SUM(ISNULL(sm.smm_vlr, 0)) as bruto,
        SUM(ISNULL(sm.SMM_AJUSTE_VLR, 0)) as ajuste,
        COUNT(DISTINCT o.osm_num) as patients
    FROM OSM o WITH(NOLOCK)
    INNER JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    INNER JOIN SMM sm WITH(NOLOCK) ON sm.smm_osm = o.osm_num AND sm.smm_osm_serie = o.osm_serie
    WHERE o.osm_dthr >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura IN ('C', 'F')
    {unit_where}
    GROUP BY CAST(o.osm_dthr AS DATE), c.cnv_caixa_fatura
    ORDER BY date
    """
    
    logger.info(f"Otimização Python: Buscando dados para agregação via Pandas.")
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        return []

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    
    # Cálculo do faturamento baseado na regra de negócio
    df['revenue'] = df.apply(
        lambda x: (float(x['bruto']) + float(x['ajuste'])) if x['cnv_caixa_fatura'] == 'C' else float(x['bruto']),
        axis=1
    )
    
    # Agrupamento Mensal
    df_monthly = df.groupby(df['date'].dt.to_period('M')).agg({
        'revenue': 'sum',
        'patients': 'sum'
    }).reset_index()
    
    df_monthly['month_year'] = df_monthly['date'].dt.strftime('%Y-%m-01')
    
    results = []
    for _, row in df_monthly.iterrows():
        rev = float(row['revenue'])
        pats = int(row['patients'])
        results.append({
            "month_year": row['month_year'],
            "revenue": round(rev, 2),
            "patients": pats,
            "ticket_avg": round((rev / pats) if pats > 0 else 0, 2)
        })
    
    return results

def get_daily_execution(cursor, unidade: str = None):
    """
    Busca a execução diária para o mês atual utilizando Pandas.
    """
    db_id = current_db_id.get()
    unit_prefix = "01%" if db_id == "1" else "04%"
    
    unit_where = f"AND s.str_str_cod LIKE '{unit_prefix}'"
    if unidade:
        if "PAULO AFONSO" in unidade.upper():
            logger.info("Auto-switch Paulo Afonso (Daily).")
        else:
            unit_where += f" AND s.str_nome LIKE '%{unidade.strip()}%'"

    # Filtramos para o mês atual
    query = f"""
    SELECT 
        CAST(o.osm_dthr AS DATE) as date,
        c.cnv_caixa_fatura,
        SUM(ISNULL(sm.smm_vlr, 0)) as bruto,
        SUM(ISNULL(sm.SMM_AJUSTE_VLR, 0)) as ajuste,
        COUNT(DISTINCT o.osm_num) as patients
    FROM OSM o WITH(NOLOCK)
    INNER JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    INNER JOIN SMM sm WITH(NOLOCK) ON sm.smm_osm = o.osm_num AND sm.smm_osm_serie = o.osm_serie
    WHERE o.osm_dthr >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura IN ('C', 'F')
    {unit_where}
    GROUP BY CAST(o.osm_dthr AS DATE), c.cnv_caixa_fatura
    ORDER BY date
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    if not rows:
        return []

    df = pd.DataFrame(rows)
    df['date_dt'] = pd.to_datetime(df['date'])
    
    df['revenue'] = df.apply(
        lambda x: (float(x['bruto']) + float(x['ajuste'])) if x['cnv_caixa_fatura'] == 'C' else float(x['bruto']),
        axis=1
    )
    
    # Agrupamento Diário
    df_daily = df.groupby('date').agg({
        'revenue': 'sum',
        'patients': 'sum'
    }).reset_index()
    
    results = []
    for _, row in df_daily.iterrows():
        rev = float(row['revenue'])
        pats = int(row['patients'])
        # Retornamos ambos os campos para compatibilidade
        results.append({
            "month_year": str(row['date']),
            "date": str(row['date']),
            "revenue": round(rev, 2),
            "patients": pats,
            "ticket_avg": round((rev / pats) if pats > 0 else 0, 2)
        })
    
    return results
