from app.database import current_db_id, get_db_connection
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)


def get_units(cursor):
    """
    Lista todas as unidades (STR) baseadas no banco ativo.
    """
    db_id = current_db_id.get()
    unit_prefix = "01%" if db_id == "1" else "04%"

    query = f"""
    SELECT 
        LTRIM(RTRIM(s.str_cod)) as cod, 
        LTRIM(RTRIM(s.str_nome)) as nome
    FROM STR s
    INNER JOIN OSM o ON s.str_cod = o.osm_str
    WHERE s.str_str_cod LIKE '{unit_prefix}'
    AND s.str_status = 'A'
    AND o.osm_dthr >= DATEADD(month, -6, GETDATE())
    GROUP BY s.str_cod, s.str_nome
    HAVING COUNT(o.osm_num) > 5
    ORDER BY nome
    """
    cursor.execute(query)
    return cursor.fetchall()


# ---------------------------------------------------------------------------
# Helper para executar uma query em uma conexão dedicada (uso em threads)
# ---------------------------------------------------------------------------
def _run_query(query: str) -> list:
    """Abre uma conexão própria, executa a query e retorna os resultados."""
    conn = get_db_connection()
    cur = conn.cursor(as_dict=True)
    try:
        cur.execute(query)
        return cur.fetchall()
    finally:
        conn.close()


def get_strategic_indicators(cursor, start_date: str, end_date: str, unidade: str = None):
    """
    Calcula os indicadores estratégicos da aba PA CAPA do Excel.

    Otimizações aplicadas:
      - SuperQuery consolida 5 passes em OSM+SMM em 1 só.
      - Queries independentes (crescimento, novos pacientes, orçamentos, RDI)
        rodam em paralelo via ThreadPoolExecutor.
    """
    logger.info(f"Calculando indicadores estratégicos: {start_date} a {end_date}, Unidade: {unidade}")

    db_id = current_db_id.get()
    unit_prefix = "01%" if db_id == "1" else "04%"

    # -----------------------------------------------------------------------
    # Filtros compartilhados
    # -----------------------------------------------------------------------
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    unit_join = "INNER JOIN STR s ON o.osm_str = s.str_cod"
    unit_where = f"AND (s.str_str_cod LIKE '{unit_prefix}')"
    if unidade:
        unit_where += f" AND LTRIM(RTRIM(s.str_nome)) = '{unidade.strip()}'"

    # Datas do período anterior (YoY)
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    prev_start = (start_dt - timedelta(days=365)).strftime("%Y-%m-%d")
    prev_end = (end_dt - timedelta(days=365)).strftime("%Y-%m-%d")
    prev_date_filter = f"BETWEEN '{prev_start} 00:00:00' AND '{prev_end} 23:59:59'"

    # -----------------------------------------------------------------------
    # SUPER-QUERY  (1 passagem em OSM+SMM para o período atual)
    # Consolida: particular/convênio, total_pacientes, total_exames,
    #            qtd_colaboradores, cortesias, descontos.
    # -----------------------------------------------------------------------
    query_super = f"""
    SELECT
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN sm.smm_vlr ELSE 0 END)  AS valor_particular,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN sm.smm_vlr ELSE 0 END)  AS valor_convenio,
        COUNT(DISTINCT o.osm_num)                                                           AS total_pacientes,
        COUNT(*)                                                                            AS total_exames,
        COUNT(DISTINCT o.osm_usr_login_cad)                                                AS qtd_colaboradores,
        SUM(CASE WHEN sm.smm_vlr = 0 THEN 1 ELSE 0 END)                                   AS cortesias,
        SUM(ISNULL(sm.smm_vlr_desconto, 0))                                                AS total_desconto
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    LEFT JOIN CNV c ON o.osm_cnv = c.cnv_cod
    {unit_join}
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    {unit_where}
    """

    # -----------------------------------------------------------------------
    # Query cancelamentos / erros (apenas OSM, sem SMM)
    # -----------------------------------------------------------------------
    query_erros = f"""
    SELECT COUNT(*) AS total
    FROM OSM o
    {unit_join}
    WHERE o.osm_dthr {date_filter}
    AND o.osm_status = 'C'
    {unit_where}
    """

    # -----------------------------------------------------------------------
    # Queries independentes (rodam em paralelo)
    # -----------------------------------------------------------------------
    query_prev = f"""
    SELECT SUM(sm.smm_vlr) AS total
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    {unit_join}
    WHERE o.osm_dthr {prev_date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    {unit_where}
    """

    query_new = f"""
    SELECT 
        COUNT(DISTINCT p.pac_reg) AS total,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN 1 ELSE 0 END) AS particular,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN 1 ELSE 0 END) AS convenio
    FROM PAC p
    INNER JOIN (
        SELECT osm_pac, MIN(osm_dthr) AS min_dthr, MIN(osm_cnv) AS fst_cnv, MIN(osm_str) AS fst_str
        FROM OSM
        GROUP BY osm_pac
    ) o ON p.pac_reg = o.osm_pac AND o.min_dthr {date_filter}
    LEFT JOIN CNV c ON o.fst_cnv = c.cnv_cod
    INNER JOIN STR s ON o.fst_str = s.str_cod
    WHERE p.pac_dreg {date_filter}
    {unit_where}
    """

    query_orc = f"""
    SELECT 
        COUNT(*) AS total_orcamentos,
        SUM(CASE WHEN r.ORP_OSM_NUM IS NOT NULL THEN 1 ELSE 0 END) AS convertidos
    FROM ORP r
    INNER JOIN STR s ON r.ORP_STR_SOLIC = s.str_cod
    WHERE r.ORP_DTHR {date_filter}
    {unit_where}
    """

    query_rdi = f"""
    SELECT SUM(rdi_valor) AS total_recebido
    FROM RDI r
    WHERE r.rdi_vcto {date_filter}
    """

    query_med = f"""
    SELECT TOP 10
        p.psv_nome AS nome,
        COUNT(DISTINCT o.osm_num) AS qtd_pedidos,
        SUM(sm.smm_vlr) AS valor_total,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN sm.smm_vlr ELSE 0 END) AS valor_particular,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN sm.smm_vlr ELSE 0 END) AS valor_convenio
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN PSV p ON o.osm_mreq = p.psv_cod
    LEFT JOIN CNV c ON o.osm_cnv = c.cnv_cod
    {unit_join}
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    {unit_where}
    GROUP BY p.psv_nome
    ORDER BY valor_total DESC
    """

    query_recep = f"""
    SELECT TOP 10
        o.osm_usr_login_cad AS usuario,
        COUNT(DISTINCT o.osm_num) AS pacientes,
        SUM(sm.smm_vlr) AS faturamento,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN sm.smm_vlr ELSE 0 END) AS valor_particular,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN sm.smm_vlr ELSE 0 END) AS valor_convenio,
        MAX(s.str_nome) AS unidade_principal
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    LEFT JOIN CNV c ON o.osm_cnv = c.cnv_cod
    {unit_join}
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    {unit_where}
    GROUP BY o.osm_usr_login_cad
    ORDER BY faturamento DESC
    """

    # -----------------------------------------------------------------------
    # EXECUÇÃO: super-query e erros sequencialmente no cursor existente,
    #           demais queries em paralelo cada uma com conexão própria.
    # -----------------------------------------------------------------------
    logger.info("Executando super-query e erros...")
    cursor.execute(query_super)
    res_super = cursor.fetchone()

    cursor.execute(query_erros)
    erros_fechamento = cursor.fetchone()['total'] or 0

    # Paralelo ---------------------------------------------------------------
    parallel_queries = {
        "prev":  query_prev,
        "new":   query_new,
        "orc":   query_orc,
        "rdi":   query_rdi,
        "med":   query_med,
        "recep": query_recep,
    }
    results = {}
    logger.info("Executando queries paralelas...")
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_run_query, q): key for key, q in parallel_queries.items()}
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()
    logger.info("Queries paralelas concluídas.")

    # -----------------------------------------------------------------------
    # Pós-processamento – super-query
    # -----------------------------------------------------------------------
    particular    = float(res_super['valor_particular'] or 0)
    convenio      = float(res_super['valor_convenio']   or 0)
    total_pc      = particular + convenio
    pc_percent    = (particular / total_pc * 100) if total_pc > 0 else 0.0
    total_pac     = res_super['total_pacientes'] or 0
    total_exm     = res_super['total_exames']    or 0
    qtd_colab     = res_super['qtd_colaboradores'] or 1
    cortesias     = res_super['cortesias']        or 0
    total_desconto = float(res_super['total_desconto'] or 0)
    val_atual     = total_pc
    tk_medio      = (val_atual / total_pac) if total_pac > 0 else 0.0
    ex_por_pac    = (total_exm / total_pac) if total_pac > 0 else 0.0
    fat_por_colab = val_atual / qtd_colab
    indice_desc   = (total_desconto / (val_atual + total_desconto)) if (val_atual + total_desconto) > 0 else 0.0

    # -----------------------------------------------------------------------
    # Pós-processamento – queries paralelas
    # -----------------------------------------------------------------------
    # Crescimento
    prev_row    = results["prev"][0] if results["prev"] else {}
    val_anterior = float(prev_row.get('total') or 0)
    crescimento_pct = ((val_atual / val_anterior) - 1) if val_anterior > 0 else 0.0

    # Novos pacientes
    new_row = results["new"][0] if results["new"] else {}
    novos_pacientes = {
        "total":     new_row.get('total')     or 0,
        "particular": new_row.get('particular') or 0,
        "convenio":  new_row.get('convenio')   or 0,
    }

    # Orçamentos
    orc_row       = results["orc"][0] if results["orc"] else {}
    total_orc     = orc_row.get('total_orcamentos') or 0
    convertidos_orc = orc_row.get('convertidos')    or 0
    taxa_conv     = (convertidos_orc / total_orc) if total_orc > 0 else 0.0

    # Recebido (RDI)
    rdi_row       = results["rdi"][0] if results["rdi"] else {}
    total_recebido = float(rdi_row.get('total_recebido') or 0)

    # Ranking médicos
    ranking_medicos = []
    for row in results["med"]:
        ranking_medicos.append({
            "nome":            row['nome'],
            "qtd_pedidos":     row['qtd_pedidos'],
            "valor_total":     float(row['valor_total']),
            "valor_particular": float(row['valor_particular']),
            "valor_convenio":  float(row['valor_convenio']),
            "ticket_medio":    float(row['valor_total']) / row['qtd_pedidos'] if row['qtd_pedidos'] > 0 else 0,
        })

    # Ranking recepcionistas
    ranking_recepcionistas = []
    for r in results["recep"]:
        ranking_recepcionistas.append({
            "usuario":           r['usuario'],
            "pacientes":         r['pacientes'],
            "faturamento":       float(r['faturamento']),
            "valor_particular":  float(r['valor_particular']),
            "valor_convenio":    float(r['valor_convenio']),
            "unidade_principal": r['unidade_principal'],
            "ticket_medio":      float(r['faturamento']) / r['pacientes'] if r['pacientes'] > 0 else 0.0,
        })

    # -----------------------------------------------------------------------
    # Montagem da resposta
    # -----------------------------------------------------------------------
    return {
        "particular_convenio": {
            "particular": particular,
            "convenio":   convenio,
            "percent":    pc_percent,
        },
        "crescimento": {
            "valor_atual":        val_atual,
            "valor_anterior":     val_anterior,
            "crescimento_percent": crescimento_pct,
            "meta":               0.15,
        },
        "operacional": {
            "pacientes":          total_pac,
            "exames":             total_exm,
            "ticket_medio":       tk_medio,
            "exames_por_paciente": ex_por_pac,
        },
        "novos_pacientes": novos_pacientes,
        "conversao_orcamento": {
            "convertidos": convertidos_orc,
            "total":       total_orc,
            "taxa":        taxa_conv,
            "meta":        0.75,
        },
        "pacientes_perdidos": 0,
        "descontos": {
            "total_desconto": total_desconto,
            "indice_percent": indice_desc,
        },
        "fluxo_financeiro": {
            "faturado":   val_atual,
            "recebido":   total_recebido,
            "diferenca":  val_atual - total_recebido,
        },
        "faturamento_por_colaborador": fat_por_colab,
        "ranking_medicos":        ranking_medicos,
        "ranking_recepcionistas": ranking_recepcionistas,
        "cortesias":              cortesias,
        "erros_fechamento":       erros_fechamento,
    }
