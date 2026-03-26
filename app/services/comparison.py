import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import contextvars

from ..database import current_db_id
from .._db_runner import run_query_new_conn
from ..models.base import (
    ModularComparisonData, ComparisonPoint, ComparisonMetric, ComparisonValue,
    DiscoveryEntity, UnitComparisonMetrics, UnitComparativeDashboard,
    RankingAgent, RankingComparisonData, ProjectionPoint, ProjectionResult
)

# --- Configuration & Metadata ---

METADATA = [
    DiscoveryEntity(name="laudos_v2", fields=["quantidade", "valor", "no_prazo", "atrasado"]),
    DiscoveryEntity(name="orcamentos", fields=["quantidade_total", "valor_total", "quantidade_convertidos", "valor_convertidos", "taxa_conversao"]),
    DiscoveryEntity(name="financeiro", fields=["faturamento_particular", "faturamento_convenio", "total_descontos"]),
    DiscoveryEntity(name="unidade", fields=["laudos_count", "laudos_value", "orcamentos_count", "orcamentos_conversion", "faturamento_total", "ticket_medio"]),
    DiscoveryEntity(name="ranking", fields=["medicos", "recepcionistas"]),
    DiscoveryEntity(name="projecao", fields=["faturamento"]),
]

def get_comparison_metadata() -> List[DiscoveryEntity]:
    return METADATA

# --- Helpers ---

def _get_past_range(start_date: str, end_date: str, years_back: int) -> tuple[str, str]:
    """Calculates the date range N years ago."""
    s_dt = datetime.strptime(start_date, '%Y-%m-%d')
    e_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    def shift(dt, y):
        try:
            return dt.replace(year=dt.year - y)
        except ValueError: # Leap year Feb 29
            return dt.replace(year=dt.year - y, day=28)
            
    return shift(s_dt, years_back).strftime('%Y-%m-%d'), shift(e_dt, years_back).strftime('%Y-%m-%d')

def _get_virtual_date(date_str: str, granularity: str) -> str:
    """Aligns a date to a virtual timeline (MM-DD, MM, or YYYY)."""
    if granularity == "anual":
        return "TOTAL" # Or the year if we want multiple points, but for comparison usually it's one per year
    if granularity == "mensal":
        return date_str[5:7] # MM
    return date_str[5:10] # MM-DD

# --- Core Logic ---

def get_laudos_comparison_v2(cursor, start_date: str, end_date: str, years_back: int = 1, granularity: str = "diario") -> ModularComparisonData:
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"
    
    # We'll build a query that unions all years
    queries = []
    for y in range(years_back + 1):
        s, e = _get_past_range(start_date, end_date, y)
        label = "Atual" if y == 0 else f"Anterior {y}"
        
        # Granularity grouping
        if granularity == "anual":
            group_sel = f"'{label}'"
        elif granularity == "mensal":
            group_sel = "CONVERT(varchar(7), RCL.RCL_DTHR_LIB, 120)"
        else: # diário
            group_sel = "CONVERT(varchar(10), RCL.RCL_DTHR_LIB, 120)"
            
        q = f"""
        SELECT 
            {group_sel} as data,
            '{label}' as periodo,
            SUM(SMM.SMM_QT)   AS quantidade,
            SUM(SMM.smm_vlr)  AS valor,
            SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) <= 0 THEN SMM.SMM_QT ELSE 0 END) AS no_prazo,
            SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) > 0 THEN SMM.SMM_QT ELSE 0 END) AS atrasado
        FROM RCL WITH(NOLOCK)
        INNER JOIN SMM WITH(NOLOCK) ON RCL.RCL_SMM = SMM.SMM_NUM AND RCL.RCL_OSM = SMM.SMM_OSM AND RCL.RCL_OSM_SERIE = SMM.SMM_OSM_SERIE
        INNER JOIN OSM WITH(NOLOCK) ON OSM.OSM_SERIE = SMM.SMM_OSM_SERIE AND OSM.OSM_NUM = SMM.SMM_OSM
        INNER JOIN STR WITH(NOLOCK) ON STR.STR_COD = OSM.OSM_STR
        WHERE RCL.RCL_DTHR_LIB BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
          AND RCL.rcl_stat IN ('I', 'E', 'L')
          AND STR.STR_STR_COD LIKE '{unit_prefix}'
          AND SMM.SMM_DT_RESULT IS NOT NULL
        GROUP BY {group_sel}
        """
        queries.append(q)
        
    full_query = " UNION ALL ".join(queries)
    cursor.execute(full_query)
    df = pd.DataFrame(cursor.fetchall())
    
    return _process_dataframe(df, granularity, ["quantidade", "valor", "no_prazo", "atrasado"])

def get_orcamentos_comparison(cursor, start_date: str, end_date: str, years_back: int = 1, granularity: str = "diario") -> ModularComparisonData:
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"
    
    queries = []
    for y in range(years_back + 1):
        s, e = _get_past_range(start_date, end_date, y)
        label = "Atual" if y == 0 else f"Anterior {y}"
        
        if granularity == "anual":
            group_sel = f"'{label}'"
        elif granularity == "mensal":
            group_sel = "CONVERT(varchar(7), ORP.ORP_DTHR, 120)"
        else:
            group_sel = "CONVERT(varchar(10), ORP.ORP_DTHR, 120)"
            
        q = f"""
        SELECT 
            {group_sel} as data,
            '{label}' as periodo,
            COUNT(*) as quantidade_total,
            SUM(IOP.IOP_VALOR) as valor_total,
            SUM(CASE WHEN ORP.ORP_OSM_NUM IS NOT NULL THEN 1 ELSE 0 END) as quantidade_convertidos,
            SUM(CASE WHEN ORP.ORP_OSM_NUM IS NOT NULL THEN IOP.IOP_VALOR ELSE 0 END) as valor_convertidos
        FROM ORP WITH(NOLOCK)
        INNER JOIN IOP WITH(NOLOCK) ON IOP.IOP_ORP_NUM = ORP.ORP_NUM
        INNER JOIN STR WITH(NOLOCK) ON STR.STR_COD = ORP.ORP_STR_SOLIC
        WHERE ORP.ORP_DTHR BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
          AND ORP.ORP_STATUS IN ('A', 'P')
          AND STR.STR_STR_COD LIKE '{unit_prefix}'
        GROUP BY {group_sel}
        """
        queries.append(q)
        
    full_query = " UNION ALL ".join(queries)
    cursor.execute(full_query)
    df = pd.DataFrame(cursor.fetchall())
    
    if not df.empty:
        df['taxa_conversao'] = (df['quantidade_convertidos'] / df['quantidade_total'] * 100).fillna(0.0)
        
    return _process_dataframe(df, granularity, ["quantidade_total", "valor_total", "quantidade_convertidos", "valor_convertidos", "taxa_conversao"])

def get_financeiro_comparison(cursor, start_date: str, end_date: str, years_back: int = 1, granularity: str = "diario") -> ModularComparisonData:
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"
    
    queries = []
    for y in range(years_back + 1):
        s, e = _get_past_range(start_date, end_date, y)
        label = "Atual" if y == 0 else f"Anterior {y}"
        
        if granularity == "anual":
            group_sel = f"'{label}'"
        elif granularity == "mensal":
            group_sel = "CONVERT(varchar(7), o.osm_dthr, 120)"
        else:
            group_sel = "CONVERT(varchar(10), o.osm_dthr, 120)"
            
        # Logic inspired by strategic indicators (strategic.py)
        # faturamento_particular = bruto_c + ajuste_c (particular)
        # faturamento_convenio = bruto_f (fatura)
        # total_descontos = -ajuste_c
        q = f"""
        SELECT 
            {group_sel} as data,
            '{label}' as periodo,
            SUM(CASE WHEN c.cnv_caixa_fatura = 'C' THEN sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0) ELSE 0 END) as faturamento_particular,
            SUM(CASE WHEN c.cnv_caixa_fatura = 'F' THEN sm.smm_vlr ELSE 0 END) as faturamento_convenio,
            SUM(CASE WHEN c.cnv_caixa_fatura = 'C' THEN -ISNULL(sm.SMM_AJUSTE_VLR, 0) ELSE 0 END) as total_descontos
        FROM OSM o WITH(NOLOCK)
        INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
        INNER JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
        INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
        WHERE o.osm_dthr BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
          AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
          AND c.cnv_caixa_fatura IN ('C', 'F')
          AND s.str_str_cod LIKE '{unit_prefix}'
        GROUP BY {group_sel}
        """
        queries.append(q)
        
    full_query = " UNION ALL ".join(queries)
    cursor.execute(full_query)
    df = pd.DataFrame(cursor.fetchall())
    
    return _process_dataframe(df, granularity, ["faturamento_particular", "faturamento_convenio", "total_descontos"])


# --- Enhancements v3 ---

def get_unit_comparative_dashboard(cursor, unidade_cod: str, start_date: str, end_date: str, years_back: int = 1) -> UnitComparativeDashboard:
    """Consolidated dashboard for a specific unit vs itself in the past, highly optimized."""
    # Get unit name
    cursor.execute(f"SELECT LTRIM(RTRIM(str_nome)) as nome FROM STR WITH(NOLOCK) WHERE str_cod = '{unidade_cod}'")
    unit_res = cursor.fetchone()
    unit_name = unit_res['nome'] if unit_res else "Unidade Desconhecida"

    comparativos = []
    
    with ThreadPoolExecutor(max_workers=min(10, (years_back + 1) * 3)) as pool:
        futures = {}
        for y in range(years_back + 1):
            s, e = _get_past_range(start_date, end_date, y)
            label = "Atual" if y == 0 else f"Anterior {y}"
            
            # Query 1: Laudos
            q_laudos = f"""
            SELECT COUNT(*) as qt, SUM(smm_vlr) as vlr 
            FROM RCL WITH(NOLOCK) 
            INNER JOIN SMM WITH(NOLOCK) ON RCL.RCL_SMM = SMM.SMM_NUM AND RCL.RCL_OSM = SMM.SMM_OSM AND RCL.RCL_OSM_SERIE = SMM.SMM_OSM_SERIE
            INNER JOIN OSM WITH(NOLOCK) ON OSM.OSM_NUM = SMM.SMM_OSM AND OSM.OSM_SERIE = SMM.SMM_OSM_SERIE
            WHERE RCL.RCL_DTHR_LIB BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
              AND OSM.OSM_STR = '{unidade_cod}'
              AND RCL.rcl_stat IN ('I', 'E', 'L')
            """
            futures[pool.submit(run_query_new_conn, q_laudos)] = (label, "laudos")
            
            # Query 2: Orçamentos
            q_orc = f"""
            SELECT COUNT(*) as tot, SUM(CASE WHEN ORP_OSM_NUM IS NOT NULL THEN 1 ELSE 0 END) as conv
            FROM ORP WITH(NOLOCK) 
            WHERE ORP_DTHR BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
              AND ORP_STR_SOLIC = '{unidade_cod}'
            """
            futures[pool.submit(run_query_new_conn, q_orc)] = (label, "orc")
            
            # Query 3: Faturamento & Pacientes
            q_fin = f"""
            SELECT 
                SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as faturamento,
                COUNT(DISTINCT o.osm_num) as pacientes
            FROM OSM o WITH(NOLOCK)
            INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
            WHERE o.osm_dthr BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
              AND o.osm_str = '{unidade_cod}'
              AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
            """
            futures[pool.submit(run_query_new_conn, q_fin)] = (label, "fin")

        # Gather results
        raw_res = {}
        for future in futures:
            label, domain = futures[future]
            if label not in raw_res: raw_res[label] = {}
            res = future.result()
            raw_res[label][domain] = res[0] if res else {}

    # Build response models
    periods = ["Atual"] + [f"Anterior {y}" for y in range(1, years_back + 1)]
    for label in periods:
        if label not in raw_res: continue
        d = raw_res[label]
        
        l_res = d.get('laudos', {})
        o_res = d.get('orc', {})
        f_res = d.get('fin', {})
        
        laudos_qt = l_res.get('qt') or 0
        laudos_vlr = float(l_res.get('vlr') or 0)
        orc_tot = o_res.get('tot') or 0
        orc_conv = (o_res.get('conv') / o_res.get('tot') * 100) if o_res.get('tot', 0) > 0 else 0.0
        fat = float(f_res.get('faturamento') or 0)
        pac = f_res.get('pacientes') or 0
        tk = (fat / pac) if pac > 0 else 0.0
        
        comparativos.append(UnitComparisonMetrics(
            period_label=label,
            laudos_count=laudos_qt,
            laudos_value=round(laudos_vlr, 2),
            orcamentos_count=orc_tot,
            orcamentos_conversion=round(orc_conv, 2),
            faturamento_total=round(fat, 2),
            ticket_medio=round(tk, 2)
        ))
        
    return UnitComparativeDashboard(
        unidade_nome=unit_name,
        unidade_cod=unidade_cod,
        comparativos=comparativos
    )

def get_ranking_comparison(cursor, entity_type: str, start_date: str, end_date: str, years_back: int = 1, unidade_cod: Optional[str] = None) -> RankingComparisonData:
    """Compares TOP agents across years with Rank Delta support."""
    unit_filter = f"AND o.osm_str = '{unidade_cod}'" if unidade_cod else ""
    
    # Structure to hold ranks for delta calculation: {period_index: {agent_id: rank}}
    ranks_by_period = {}
    period_agents = {} # {period_index: [RankingAgent]}

    for y in range(years_back + 1):
        s, e = _get_past_range(start_date, end_date, y)
        label = "Atual" if y == 0 else f"Anterior {y}"
        ranks_by_period[y] = {}
        period_agents[y] = []
        
        if entity_type == "medicos":
            query = f"""
            SELECT TOP 20
                p.psv_cod as id,
                p.psv_nome as nome,
                SUM(sm.smm_vlr) as valor,
                COUNT(DISTINCT o.osm_num) as volume
            FROM OSM o WITH(NOLOCK)
            INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
            INNER JOIN PSV p WITH(NOLOCK) ON o.osm_mreq = p.psv_cod
            WHERE o.osm_dthr BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
              AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
              {unit_filter}
            GROUP BY p.psv_cod, p.psv_nome
            ORDER BY valor DESC
            """
        else: # recepcionistas
            query = f"""
            SELECT TOP 20
                o.osm_usr_login_cad as id,
                o.osm_usr_login_cad as nome,
                SUM(sm.smm_vlr) as valor,
                COUNT(DISTINCT o.osm_num) as volume
            FROM OSM o WITH(NOLOCK)
            INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
            WHERE o.osm_dthr BETWEEN '{s} 00:00:00' AND '{e} 23:59:59'
              AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
              {unit_filter}
            GROUP BY o.osm_usr_login_cad
            ORDER BY valor DESC
            """
            
        cursor.execute(query)
        rows = cursor.fetchall()
        for idx, row in enumerate(rows):
            agent_id = str(row['id'])
            rank = idx + 1
            ranks_by_period[y][agent_id] = rank
            
            period_agents[y].append(RankingAgent(
                nome=row['nome'],
                period_label=label,
                rank=rank,
                rank_delta=0, # Will calculate after
                valor=round(float(row['valor']), 2),
                volume=row['volume']
            ))
            
    # Calculate Deltas (Current y vs Previous y+1)
    # Logic: rank_delta = prev_rank - current_rank
    for y in range(years_back): 
        prev_y = y + 1
        if prev_y in ranks_by_period:
            for agent in period_agents[y]:
                # We use the agent's name as a secondary key for this specific view
                # as the ID is not exposed in the RankingAgent model.
                # In most cases in this system, psv_nome and usr_login are stable per unit.
                prev_rank = ranks_by_period[prev_y].get(agent.nome)
                if prev_rank:
                    agent.rank_delta = prev_rank - agent.rank

    final_output = []
    for y in range(years_back + 1):
        final_output.extend(period_agents[y])

    return RankingComparisonData(entity_type=entity_type, agents=final_output)

def get_performance_projections(cursor, entity: str = "faturamento") -> ProjectionResult:
    """Sophisticated projection based on run rate and historical seasonality."""
    # 1. Current Month status
    now = datetime.now()
    month_start = now.replace(day=1).strftime('%Y-%m-%d')
    today_str = now.strftime('%Y-%m-%d')
    days_passed = now.day
    last_day = (now.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    total_days = last_day.day
    remaining_days = total_days - days_passed
    
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"
    
    # Get total current month
    cursor.execute(f"""
    SELECT SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as total
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    WHERE o.osm_dthr BETWEEN '{month_start} 00:00:00' AND '{today_str} 23:59:59'
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
      AND s.str_str_cod LIKE '{unit_prefix}'
    """)
    current_total = float(cursor.fetchone()['total'] or 0)
    
    # Get daily avg of last 7 days (Run Rate)
    seven_days_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d')
    cursor.execute(f"""
    SELECT SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) / 7.0 as avg_diaria
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    WHERE o.osm_dthr BETWEEN '{seven_days_ago} 00:00:00' AND '{today_str} 23:59:59'
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
      AND s.str_str_cod LIKE '{unit_prefix}'
    """)
    run_rate = float(cursor.fetchone()['avg_diaria'] or 0)
    
    # 2. Historical Seasonality (Last Year same month)
    ly_start, ly_end = _get_past_range(month_start, last_day.strftime('%Y-%m-%d'), 1)
    ly_today_start, ly_today_end = _get_past_range(month_start, today_str, 1)
    
    cursor.execute(f"""
    SELECT 
        SUM(CASE WHEN o.osm_dthr <= '{ly_today_end} 23:59:59' THEN sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0) ELSE 0 END) as ate_hoje,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as total_mes
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    WHERE o.osm_dthr BETWEEN '{ly_start} 00:00:00' AND '{ly_end} 23:59:59'
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
      AND s.str_str_cod LIKE '{unit_prefix}'
    """)
    ly_res = cursor.fetchone()
    ly_total = float(ly_res['total_mes'] or 1)
    ly_ate_hoje = float(ly_res['ate_hoje'] or 1)
    
    seasonality_factor = ly_total / ly_ate_hoje # How much the full month was compared to this point
    
    # 3. Projections
    # Conservative: Simple Run Rate applied to remaining days
    proj_cons = current_total + (remaining_days * run_rate)
    
    # Optimistic: Run Rate weighted by Seasonality Factor
    # (If the end of the month is usually busier, seasonality_factor will be > 1)
    # We take a weighted average between linear run rate and seasonality projection
    proj_season = current_total * seasonality_factor
    proj_opt = (proj_cons * 0.4) + (proj_season * 0.6)
    
    return ProjectionResult(
        entity=entity,
        last_update=datetime.now().strftime('%Y-%m-%d %H:%M'),
        current_value=round(current_total, 2),
        projections=[
            ProjectionPoint(label="Realizado", valor=round(current_total, 2)),
            ProjectionPoint(label="Projetado (Conservador)", valor=round(proj_cons, 2)),
            ProjectionPoint(label="Projetado (Otimista)", valor=round(proj_opt, 2))
        ],
        confidence_score=0.85 if days_passed > 10 else 0.6 # Confidence grows as month progresses
    )


# --- Processing Helper ---

def _process_dataframe(df: pd.DataFrame, granularity: str, fields: List[str]) -> ModularComparisonData:
    if df.empty:
        return ModularComparisonData(points=[], totals=[])
    
    # Map real dates to virtual dates (MM-DD or MM)
    df['virtual_date'] = df['data'].apply(lambda x: _get_virtual_date(x, granularity))
    
    # Calculate Totals
    totals = []
    # Sort periods: Atual, then Anterior 1, 2, ...
    def period_sort_key(p):
        if p == "Atual": return 0
        try:
             return int(p.split()[-1])
        except:
             return 999

    periods = sorted(df['periodo'].unique(), key=period_sort_key)

    for field in fields:
        val_list = []
        for p in periods:
            val = float(df[df['periodo'] == p][field].sum())
            val_list.append(ComparisonValue(period_label=p, value=round(val, 2)))
        totals.append(ComparisonMetric(field=field, values=val_list))
        
    # Calculate Points
    points = []
    if granularity == "anual":
        v_date = "TOTAL"
        metrics = []
        for field in fields:
            val_list = []
            for p in periods:
                val = float(df[(df['periodo'] == p)][field].sum())
                val_list.append(ComparisonValue(period_label=p, value=round(val, 2)))
            metrics.append(ComparisonMetric(field=field, values=val_list))
        points.append(ComparisonPoint(virtual_date=v_date, metrics=metrics))
    else:
        v_dates = sorted(df['virtual_date'].unique())
        for v_date in v_dates:
            metrics = []
            for field in fields:
                val_list = []
                for p in periods:
                    row = df[(df['virtual_date'] == v_date) & (df['periodo'] == p)]
                    val = float(row[field].iloc[0]) if not row.empty else 0.0
                    val_list.append(ComparisonValue(period_label=p, value=round(val, 2)))
                metrics.append(ComparisonMetric(field=field, values=val_list))
            points.append(ComparisonPoint(virtual_date=v_date, metrics=metrics))
            
    return ModularComparisonData(points=points, totals=totals)
