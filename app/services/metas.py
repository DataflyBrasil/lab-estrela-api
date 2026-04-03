import pandas as pd
from app.database import current_db_id
import logging

logger = logging.getLogger(__name__)

def get_monthly_execution(cursor, unidade: str = None):
    """
    Busca a execução mensal (faturamento e pacientes) para o ano atual,
    consolidando dados de OSM, SMM e CNV.
    """
    db_id = current_db_id.get()
    unit_prefix = "01%" if db_id == "1" else "04%"
    
    # Filtro de unidade opcional
    unit_where = f"AND s.str_str_cod LIKE '{unit_prefix}'"
    if unidade:
        # Se for um nome de unidade, limpamos e filtramos com UPPER para evitar erros de case/espaços
        unit_where += f" AND UPPER(LTRIM(RTRIM(s.str_nome))) = UPPER('{unidade.strip()}')"

    # Query otimizada agrupada por mês
    # Usamos DATEFROMPARTS para normalizar para o dia 01 do mês
    query = f"""
    SELECT 
        CONVERT(VARCHAR(10), DATEFROMPARTS(YEAR(o.osm_dthr), MONTH(o.osm_dthr), 1), 120) as month_year,
        SUM(CASE WHEN c.cnv_caixa_fatura = 'C' THEN sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0) ELSE 0 END) +
        SUM(CASE WHEN c.cnv_caixa_fatura = 'F' THEN sm.smm_vlr ELSE 0 END) as revenue,
        COUNT(DISTINCT o.osm_num) as patients
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    WHERE o.osm_dthr >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1)
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura IN ('C', 'F')
    {unit_where}
    GROUP BY DATEFROMPARTS(YEAR(o.osm_dthr), MONTH(o.osm_dthr), 1)
    ORDER BY month_year
    """
    
    logger.info(f"Executando query de metas mensal para unidade: {unidade or 'Todas'}")
    cursor.execute(query)
    rows = cursor.fetchall()
    
    results = []
    for r in rows:
        rev = float(r['revenue'] or 0)
        pats = int(r['patients'] or 0)
        results.append({
            "month_year": r['month_year'],
            "revenue": round(rev, 2),
            "patients": pats,
            "ticket_avg": round((rev / pats) if pats > 0 else 0, 2)
        })
    
    return results
