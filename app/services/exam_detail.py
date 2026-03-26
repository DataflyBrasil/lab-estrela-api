import contextvars
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import logging

from .._db_runner import run_query_new_conn
from ..database import current_db_id

logger = logging.getLogger(__name__)

def get_exam_details(exame_cod: str, start_date: str, end_date: str, tpcod: str = 'LB') -> dict:
    """
    Retorna o aprofundamento de um exame específico.
    Consolida: resumo de performance, ranking de médicos, unidades, convênios
    e histórico recente de pacientes.
    """
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"

    # 1. Resumo Geral
    q_resumo = f"""
    SELECT
        k.smk_cod,
        k.smk_nome,
        SUM(sm.smm_qt) as qtd_total,
        SUM(sm.smm_vlr) as faturado_bruto,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as faturado_liquido,
        AVG(CASE WHEN sm.SMM_DT_RESULT IS NOT NULL 
                 THEN CAST(DATEDIFF(hour, o.osm_dthr, sm.SMM_DT_RESULT) AS FLOAT) 
                 ELSE NULL END) / 24.0 as prazo_medio_dias
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN SMK k WITH(NOLOCK) ON k.smk_cod = sm.smm_cod AND k.smk_tipo = sm.smm_tpcod
    WHERE sm.smm_cod = '{exame_cod}' AND sm.smm_tpcod = '{tpcod}'
      AND o.osm_dthr {date_filter}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    GROUP BY k.smk_cod, k.smk_nome
    """

    # 2. Ranking Médicos (Top 10)
    q_medicos = f"""
    SELECT TOP 10
        LTRIM(RTRIM(p.psv_nome)) as nome,
        SUM(sm.smm_qt) as qtd,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as valor
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN PSV p WITH(NOLOCK) ON o.osm_mreq = p.psv_cod
    WHERE sm.smm_cod = '{exame_cod}' AND sm.smm_tpcod = '{tpcod}'
      AND o.osm_dthr {date_filter}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    GROUP BY p.psv_nome
    ORDER BY qtd DESC
    """

    # 3. Distribuição por Unidade
    q_unidades = f"""
    SELECT
        LTRIM(RTRIM(s.str_nome)) as nome,
        SUM(sm.smm_qt) as qtd,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as valor
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    WHERE sm.smm_cod = '{exame_cod}' AND sm.smm_tpcod = '{tpcod}'
      AND o.osm_dthr {date_filter}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
      AND s.str_str_cod LIKE '{unit_prefix}'
    GROUP BY s.str_nome
    ORDER BY qtd DESC
    """

    # 4. Distribuição por Convênio
    q_convenios = f"""
    SELECT
        LTRIM(RTRIM(c.cnv_nome)) as nome,
        SUM(sm.smm_qt) as qtd,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as valor
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    WHERE sm.smm_cod = '{exame_cod}' AND sm.smm_tpcod = '{tpcod}'
      AND o.osm_dthr {date_filter}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    GROUP BY c.cnv_nome
    ORDER BY qtd DESC
    """

    # 5. Últimos Pacientes
    q_pacientes = f"""
    SELECT TOP 50
        CONVERT(varchar(16), o.osm_dthr, 120) as data,
        LTRIM(RTRIM(p.pac_nome)) as paciente,
        o.osm_num as osm,
        LTRIM(RTRIM(c.cnv_nome)) as convenio,
        (sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) as valor
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN PAC p WITH(NOLOCK) ON o.osm_pac = p.pac_reg
    INNER JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    WHERE sm.smm_cod = '{exame_cod}' AND sm.smm_tpcod = '{tpcod}'
      AND o.osm_dthr {date_filter}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    ORDER BY o.osm_dthr DESC
    """

    parallel = {
        "resumo": q_resumo,
        "medicos": q_medicos,
        "unidades": q_unidades,
        "convenios": q_convenios,
        "pacientes": q_pacientes,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(contextvars.copy_context().run, run_query_new_conn, q): key
            for key, q in parallel.items()
        }
        for f in as_completed(futures):
            results[futures[f]] = f.result()

    res_row = results["resumo"][0] if results["resumo"] else {}
    qtd_total = int(res_row.get('qtd_total') or 0)
    fat_liq = float(res_row.get('faturado_liquido') or 0.0)

    return {
        "resumo": {
            "cod": exame_cod,
            "nome": res_row.get('smk_nome') or "Não encontrado",
            "qtd_total": qtd_total,
            "faturado_bruto": float(res_row.get('faturado_bruto') or 0.0),
            "faturado_liquido": fat_liq,
            "ticket_medio": fat_liq / qtd_total if qtd_total > 0 else 0.0,
            "prazo_medio_dias": float(res_row.get('prazo_medio_dias') or 0.0),
        },
        "ranking_medicos": [
            {"nome": r['nome'], "qtd": int(r['qtd']), "valor": float(r['valor'])}
            for r in results["medicos"]
        ],
        "ranking_unidades": [
            {"nome": r['nome'], "qtd": int(r['qtd']), "valor": float(r['valor'])}
            for r in results["unidades"]
        ],
        "ranking_convenios": [
            {"nome": r['nome'], "qtd": int(r['qtd']), "valor": float(r['valor'])}
            for r in results["convenios"]
        ],
        "ultimos_pacientes": [
            {
                "data": r['data'],
                "paciente": r['paciente'],
                "osm": int(r['osm']),
                "convenio": r['convenio'],
                "valor": float(r['valor'])
            }
            for r in results["pacientes"]
        ]
    }
