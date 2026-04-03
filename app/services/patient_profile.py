"""
Serviço de perfil individual de paciente.

Endpoints suportados:
  - search_pacientes(nome, page, limit) → lista paginada
  - get_paciente_perfil(pac_reg)        → perfil completo em paralelo
"""
import contextvars
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date

from app._db_runner import run_query_new_conn
from app.database import current_db_id


# ---------------------------------------------------------------------------
# Busca / listagem paginada
# ---------------------------------------------------------------------------

def search_pacientes(nome: str, page: int = 1, limit: int = 20) -> dict:
    """
    Busca pacientes cujo nome contenha 'nome' (LIKE %nome%).
    Filtra apenas pacientes com atendimentos nas unidades do banco ativo.

    Otimização: COUNT(*) OVER() retorna o total de resultados na mesma passagem
    da query paginada — elimina a varredura duplicada do query_count separado.
    """
    offset = (page - 1) * limit
    nome_safe = nome.strip().replace("'", "''")
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"

    # Otimização: Filtrar por nome PRIMEIRO (na PAC) e depois buscar métricas via CROSS APPLY.
    # Isso evita varrer a tabela OSM inteira para todos os pacientes antes de filtrar pelo nome.
    query = f"""
    SELECT
        pk.pac_reg,
        pk.nome,
        pk.nascimento,
        pk.sexo,
        pk.fone,
        CONVERT(varchar(10), op.ultima_dthr, 120) AS ultima_visita,
        ISNULL(op.total_visitas, 0)               AS total_visitas,
        pk.obs1,
        pk.obs2,
        COUNT(*) OVER()                           AS total_count
    FROM (
        SELECT 
            p.pac_reg,
            LTRIM(RTRIM(p.pac_nome))                   AS nome,
            CONVERT(varchar(10), p.pac_nasc, 120)       AS nascimento,
            p.pac_sexo                                 AS sexo,
            LTRIM(RTRIM(ISNULL(p.pac_fone, '')))        AS fone,
            LTRIM(RTRIM(ISNULL(p.pac_obs,  '')))        AS obs1,
            LTRIM(RTRIM(ISNULL(p.pac_obs2, '')))        AS obs2
        FROM PAC p WITH(NOLOCK)
        WHERE p.pac_nome LIKE '%{nome_safe}%'
          AND p.pac_nome NOT LIKE 'teste%'
    ) pk
    CROSS APPLY (
        SELECT 
            MAX(o.osm_dthr)           AS ultima_dthr,
            COUNT(DISTINCT o.osm_num) AS total_visitas
        FROM OSM o WITH(NOLOCK)
        INNER JOIN STR s WITH(NOLOCK) ON s.str_cod = o.osm_str
        WHERE o.osm_pac = pk.pac_reg
          AND (o.osm_status IS NULL OR o.osm_status <> 'C')
          AND s.str_str_cod LIKE '{unit_prefix}'
    ) op
    WHERE op.ultima_dthr IS NOT NULL
    ORDER BY op.ultima_dthr DESC
    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """

    rows = run_query_new_conn(query)
    total = rows[0].get("total_count", 0) if rows else 0
    hoje  = date.today()

    items = []
    for r in rows:
        ultima = r.get("ultima_visita")
        dias_sem = 0
        if ultima:
            try:
                dias_sem = (hoje - datetime.strptime(ultima[:10], "%Y-%m-%d").date()).days
            except ValueError:
                pass
        # Combinar observações
        o1 = r.get("obs1", "").strip()
        o2 = r.get("obs2", "").strip()
        obs = " ".join(filter(None, [o1, o2])) or None

        items.append({
            "pac_reg":         r["pac_reg"],
            "nome":            r["nome"],
            "nascimento":      r.get("nascimento"),
            "sexo":            r.get("sexo"),
            "fone":            r.get("fone") or None,
            "ultima_visita":   ultima,
            "total_visitas":   int(r.get("total_visitas") or 0),
            "dias_sem_visita": dias_sem,
            "observacoes":     obs,
        })

    return {"total": total, "page": page, "limit": limit, "items": items}


# ---------------------------------------------------------------------------
# Perfil completo (5 queries em paralelo)
# ---------------------------------------------------------------------------

def get_paciente_perfil(pac_reg: int) -> dict:
    """
    Retorna o perfil completo de um paciente:
      identidade, classificação, resumo financeiro,
      histórico de visitas, exames mais realizados e orçamentos.

    Otimizações aplicadas:
      - q_financeiro absorveu q_convenio (mesma tabela OSM, evita 2ª varredura).
      - ISNULL(sm.smm_sfat, '') <> 'C' substitui o OR com IS NULL (melhor plano).
      - TOP 50 em orcamentos para evitar retorno ilimitado.
    """
    q_identidade = f"""
    SELECT
        p.pac_reg,
        LTRIM(RTRIM(p.pac_nome))                    AS nome,
        CONVERT(varchar(10), p.pac_nasc, 120)        AS nascimento,
        p.pac_sexo                                   AS sexo,
        LTRIM(RTRIM(ISNULL(p.pac_fone,  '')))       AS fone,
        CONVERT(varchar(10), p.pac_dreg, 120)        AS data_cadastro,
        LTRIM(RTRIM(ISNULL(CAST(p.pac_obs AS varchar(max)),  '')))       AS obs1,
        LTRIM(RTRIM(ISNULL(CAST(p.pac_obs2 AS varchar(max)), '')))       AS obs2
    FROM PAC p WITH(NOLOCK)
    WHERE p.pac_reg = {pac_reg}
    """

    # 1. Estatísticas financeiras bases
    q_stats = f"""
    SELECT
        COUNT(DISTINCT o.osm_num)                                                              AS total_visitas,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0))                                         AS total_gasto,
        CONVERT(varchar(10), MIN(o.osm_dthr), 120)                                             AS primeira_visita,
        CONVERT(varchar(10), MAX(o.osm_dthr), 120)                                             AS ultima_visita,
        SUM(CASE WHEN c.cnv_caixa_fatura = 'C' THEN sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0) ELSE 0 END) AS valor_particular,
        SUM(CASE WHEN c.cnv_caixa_fatura = 'F' THEN sm.smm_vlr ELSE 0 END)                     AS valor_convenio
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    LEFT  JOIN CNV c  WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    WHERE o.osm_pac = {pac_reg}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND ISNULL(sm.smm_sfat, '') <> 'C'
    """

    # 2. Convênio Principal
    q_top_cnv = f"""
    SELECT TOP 1 ISNULL(LTRIM(RTRIM(c.cnv_nome)), 'Particular') AS nome
    FROM OSM o WITH(NOLOCK)
    LEFT JOIN CNV c WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    WHERE o.osm_pac = {pac_reg} AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    GROUP BY c.cnv_nome ORDER BY COUNT(*) DESC
    """

    # 3. Médico Principal
    q_top_med = f"""
    SELECT TOP 1 LTRIM(RTRIM(p.psv_nome)) AS nome
    FROM OSM o WITH(NOLOCK)
    INNER JOIN PSV p WITH(NOLOCK) ON o.osm_mreq = p.psv_cod
    WHERE o.osm_pac = {pac_reg} AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    GROUP BY p.psv_nome ORDER BY COUNT(*) DESC
    """

    # 4. Unidade Principal
    q_top_str = f"""
    SELECT TOP 1 LTRIM(RTRIM(s.str_nome)) AS nome
    FROM OSM o WITH(NOLOCK)
    INNER JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    WHERE o.osm_pac = {pac_reg} AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    GROUP BY s.str_nome ORDER BY COUNT(*) DESC
    """

    q_visitas = f"""
    SELECT TOP 20
        o.osm_num,
        CONVERT(varchar(19), o.osm_dthr, 120)     AS data,
        LTRIM(RTRIM(s.str_nome))                   AS unidade,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) AS valor,
        COUNT(sm.smm_num)                          AS qtd_exames
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s  WITH(NOLOCK) ON s.str_cod = o.osm_str
    WHERE o.osm_pac = {pac_reg}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND ISNULL(sm.smm_sfat, '') <> 'C'
    GROUP BY o.osm_num, o.osm_dthr, s.str_nome
    ORDER BY o.osm_dthr DESC
    """

    q_exames = f"""
    SELECT TOP 10
        LTRIM(RTRIM(k.smk_nome))  AS exame,
        SUM(sm.smm_qt)            AS frequencia,
        SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)) AS valor_total
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN SMK k  WITH(NOLOCK) ON k.smk_cod = sm.smm_cod AND k.smk_tipo = sm.smm_tpcod
    WHERE o.osm_pac = {pac_reg}
      AND (o.osm_status IS NULL OR o.osm_status <> 'C')
      AND ISNULL(sm.smm_sfat, '') <> 'C'
    GROUP BY k.smk_nome
    ORDER BY SUM(sm.smm_qt) DESC
    """

    q_orcamentos = f"""
    SELECT TOP 50
        r.ORP_NUM                                   AS orcamento_num,
        CONVERT(varchar(10), r.ORP_DTHR, 120)       AS data,
        r.ORP_STATUS                                AS status,
        r.ORP_OSM_NUM                               AS osm_num,
        CAST(r.ORP_OBS AS nvarchar(max))            AS observacao,
        SUM(i.IOP_VALOR)                            AS valor_total
    FROM ORP r WITH(NOLOCK)
    INNER JOIN IOP i WITH(NOLOCK) ON i.IOP_ORP_NUM = r.ORP_NUM
    WHERE r.ORP_PAC_REG = {pac_reg}
    GROUP BY r.ORP_NUM, r.ORP_DTHR, r.ORP_STATUS, r.ORP_OSM_NUM, CAST(r.ORP_OBS AS nvarchar(max))
    ORDER BY r.ORP_DTHR DESC
    """

    # Query adicional para taxa de conversão global do paciente
    q_conversoes = f"""
    SELECT 
        COUNT(*) as total_orc,
        SUM(CASE WHEN ORP_OSM_NUM IS NOT NULL THEN 1 ELSE 0 END) as convertidos
    FROM ORP WITH(NOLOCK)
    WHERE ORP_PAC_REG = {pac_reg}
    """

    parallel = {
        "identidade": q_identidade,
        "stats":      q_stats,
        "top_cnv":    q_top_cnv,
        "top_med":    q_top_med,
        "top_str":    q_top_str,
        "visitas":    q_visitas,
        "exames":     q_exames,
        "orcamentos": q_orcamentos,
        "conversoes": q_conversoes,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = {
            pool.submit(contextvars.copy_context().run, run_query_new_conn, q): key
            for key, q in parallel.items()
        }
        for f in as_completed(futures):
            results[futures[f]] = f.result()

    hoje = date.today()

    # --- Identidade ---
    id_row   = results["identidade"][0] if results["identidade"] else {}
    nasc_str = id_row.get("nascimento")
    idade    = None
    if nasc_str:
        try:
            nasc_dt = datetime.strptime(nasc_str[:10], "%Y-%m-%d").date()
            idade   = hoje.year - nasc_dt.year - ((hoje.month, hoje.day) < (nasc_dt.month, nasc_dt.day))
        except ValueError:
            pass
    cad_str       = id_row.get("data_cadastro")
    tempo_paciente = None
    if cad_str:
        try:
            tempo_paciente = (hoje - datetime.strptime(cad_str[:10], "%Y-%m-%d").date()).days
        except ValueError:
            pass

    o1 = id_row.get("obs1", "").strip()
    o2 = id_row.get("obs2", "").strip()
    observacoes = " ".join(filter(None, [o1, o2])) or None

    # --- Financeiro + convênio + insights ---
    fin_row       = results["stats"][0] if results["stats"] else {}
    total_visitas = int(fin_row.get("total_visitas") or 0)
    total_gasto   = float(fin_row.get("total_gasto") or 0)
    val_part      = float(fin_row.get("valor_particular") or 0)
    val_conv      = float(fin_row.get("valor_convenio") or 0)
    ticket_medio  = total_gasto / total_visitas if total_visitas > 0 else 0.0
    total_fin     = val_part + val_conv
    pct_part      = (val_part / total_fin * 100) if total_fin > 0 else 0.0
    
    # Orçamentos
    conv_row    = results["conversoes"][0] if results["conversoes"] else {}
    tot_orc     = int(conv_row.get("total_orc") or 0)
    tot_conv    = int(conv_row.get("convertidos") or 0)
    taxa_orc    = (tot_conv / tot_orc * 100) if tot_orc > 0 else 0.0

    primeira_visita = fin_row.get("primeira_visita")
    ultima_visita   = fin_row.get("ultima_visita")
    dias_sem_visita = 0
    frequencia_dias = None
    
    if ultima_visita and primeira_visita:
        try:
            d_ultima = datetime.strptime(ultima_visita[:10], "%Y-%m-%d").date()
            d_primeira = datetime.strptime(primeira_visita[:10], "%Y-%m-%d").date()
            dias_sem_visita = (hoje - d_ultima).days
            periodo_total = (d_ultima - d_primeira).days
            if total_visitas > 1 and periodo_total > 0:
                frequencia_dias = int(periodo_total / (total_visitas - 1))
        except ValueError:
            pass

    # Insights Principal
    cnv_row = results["top_cnv"][0] if results["top_cnv"] else {}
    med_row = results["top_med"][0] if results["top_med"] else {}
    str_row = results["top_str"][0] if results["top_str"] else {}

    identidade = {
        "pac_reg":                 id_row.get("pac_reg", pac_reg),
        "nome":                    id_row.get("nome", ""),
        "nascimento":              nasc_str,
        "idade":                   idade,
        "sexo":                    id_row.get("sexo"),
        "fone":                    id_row.get("fone") or None,
        "data_cadastro":           cad_str,
        "tempo_como_paciente_dias": tempo_paciente,
        "observacoes":             observacoes,
        "medico_principal":        med_row.get("nome") or "Sem Registro",
        "unidade_principal":       str_row.get("nome") or "Sem Registro",
    }

    financeiro = {
        "total_gasto":              round(total_gasto, 2),
        "ticket_medio":             round(ticket_medio, 2),
        "convenio_principal":       cnv_row.get("nome") or "Particular",
        "valor_particular":         round(val_part, 2),
        "valor_convenio":           round(val_conv, 2),
        "percent_particular":       round(pct_part, 2),
        "taxa_conversao_orcamento": round(taxa_orc, 2),
    }

    # --- Classificação (VIP tem prioridade) ---
    if total_gasto >= 5000:
        categoria = "VIP"
    elif total_visitas >= 6:
        categoria = "Fiel"
    elif total_visitas >= 2:
        categoria = "Recorrente"
    else:
        categoria = "Novo"

    classificacao = {
        "categoria":               categoria,
        "total_visitas":           total_visitas,
        "primeira_visita":         primeira_visita,
        "ultima_visita":           ultima_visita,
        "dias_sem_visita":         dias_sem_visita,
        "frequencia_visitas_dias": frequencia_dias,
    }

    # --- Histórico de visitas ---
    historico = [
        {
            "osm_num":    int(r["osm_num"]),
            "data":       str(r["data"])[:19],
            "unidade":    r["unidade"],
            "valor":      round(float(r["valor"] or 0), 2),
            "qtd_exames": int(r["qtd_exames"] or 0),
        }
        for r in results["visitas"]
    ]

    # --- Exames mais realizados ---
    exames = [
        {
            "exame":       r["exame"],
            "frequencia":  int(r["frequencia"] or 0),
            "valor_total": round(float(r["valor_total"] or 0), 2),
        }
        for r in results["exames"]
    ]

    # --- Orçamentos ---
    orcamentos = []
    for r in results["orcamentos"]:
        osm_raw     = r.get("osm_num")
        convertido  = osm_raw is not None
        data_orc    = str(r["data"])[:10]
        dias_aberto = None
        if not convertido and str(r.get("status", "")).strip() == "A":
            try:
                dias_aberto = (hoje - datetime.strptime(data_orc, "%Y-%m-%d").date()).days
            except ValueError:
                pass
        orcamentos.append({
            "orcamento_num":  int(r["orcamento_num"]),
            "data":           data_orc,
            "status":         str(r["status"]).strip(),
            "convertido":     convertido,
            "valor_total":    round(float(r["valor_total"] or 0), 2),
            "dias_em_aberto": dias_aberto,
            "observacao":     r.get("observacao")
        })

    return {
        "identidade":             identidade,
        "classificacao":          classificacao,
        "financeiro":             financeiro,
        "historico_visitas":      historico,
        "exames_mais_realizados": exames,
        "orcamentos":             orcamentos,
    }


# ---------------------------------------------------------------------------
# Listagem Estratégica (Período ou Full Scan)
# ---------------------------------------------------------------------------

def get_pacientes_estrategico(
    start_date: str = None, 
    end_date: str = None, 
    page: int = 1, 
    limit: int = 20, 
    full_scan: bool = False
) -> dict:
    """
    Retorna listagem de pacientes com métricas de LTV e classificação.
    Otimizado para 2 etapas: (1) identifica IDs da página, (2) calcula métricas apenas para esses 20 IDs.
    """
    offset = (page - 1) * limit
    unit_prefix = "01%" if current_db_id.get() == "1" else "04%"
    hoje = date.today()

    # 1. Identificar quem são os pacientes desta página (Baseado na atividade)
    if full_scan or (not start_date and not end_date):
        base_where = f"(o.osm_status IS NULL OR o.osm_status <> 'C') AND s.str_str_cod LIKE '{unit_prefix}'"
    else:
        base_where = f"(o.osm_status IS NULL OR o.osm_status <> 'C') AND o.osm_dthr BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59' AND s.str_str_cod LIKE '{unit_prefix}'"

    # Query para obter IDs da página
    id_query = f"""
        SELECT o.osm_pac AS pac_reg
        FROM OSM o WITH(NOLOCK)
        INNER JOIN STR s WITH(NOLOCK) ON s.str_cod = o.osm_str
        WHERE {base_where}
        GROUP BY o.osm_pac
        ORDER BY MAX(o.osm_dthr) DESC
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """

    # Query para obter o total real de pacientes únicos
    count_query = f"""
        SELECT COUNT(DISTINCT o.osm_pac) AS total
        FROM OSM o WITH(NOLOCK)
        INNER JOIN STR s WITH(NOLOCK) ON s.str_cod = o.osm_str
        WHERE {base_where}
    """

    # Executar de forma sequencial para estabilidade de conexão
    try:
        id_rows = run_query_new_conn(id_query)
        count_res = run_query_new_conn(count_query)
        total = count_res[0].get("total", 0) if count_res else 0
    except Exception as e:
        print(f"Erro ao buscar IDs/Count de pacientes: {e}")
        return {"total": 0, "page": page, "limit": limit, "items": [], "error": str(e)}

    if not id_rows:
        return {"total": total, "page": page, "limit": limit, "items": []}

    pac_regs = [str(r["pac_reg"]) for r in id_rows]
    regs_str = ",".join(pac_regs)

    # 2. Buscar/Calcular métricas pesadas APENAS PARA OS 20 IDs da página
    enrichment_query = f"""
    SELECT
        p.pac_reg,
        LTRIM(RTRIM(p.pac_nome))                   AS nome,
        CONVERT(varchar(10), p.pac_nasc, 120)       AS nascimento,
        p.pac_sexo                                 AS sexo,
        LTRIM(RTRIM(ISNULL(p.pac_fone,  '')))      AS fone,
        stats.total_gasto,
        stats.total_visitas,
        CONVERT(varchar(10), stats.ultima_visita, 120) AS ultima_visita
    FROM PAC p WITH(NOLOCK)
    CROSS APPLY (
        SELECT 
            ISNULL(SUM(sm.smm_vlr + ISNULL(sm.SMM_AJUSTE_VLR, 0)), 0) AS total_gasto,
            COUNT(DISTINCT o.osm_num)                                 AS total_visitas,
            MAX(o.osm_dthr)                                           AS ultima_visita
        FROM OSM o WITH(NOLOCK)
        INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
        INNER JOIN STR s WITH(NOLOCK) ON s.str_cod = o.osm_str
        WHERE o.osm_pac = p.pac_reg
          AND (o.osm_status IS NULL OR o.osm_status <> 'C')
          AND ISNULL(sm.smm_sfat, '') <> 'C'
          AND s.str_str_cod LIKE '{unit_prefix}'
    ) stats
    WHERE p.pac_reg IN ({regs_str})
    ORDER BY stats.ultima_visita DESC
    """

    rows = run_query_new_conn(enrichment_query)
    
    items = []
    for r in rows:
        total_gasto = float(r["total_gasto"] or 0)
        total_visitas = int(r["total_visitas"] or 0)
        ultima = r.get("ultima_visita")
        dias_sem = 0
        if ultima:
            try:
                dias_sem = (hoje - datetime.strptime(ultima[:10], "%Y-%m-%d").date()).days
            except ValueError:
                pass

        # Classificação baseada no histórico LTV
        if total_gasto >= 5000:
            categoria = "VIP"
        elif total_visitas >= 6:
            categoria = "Fiel"
        elif total_visitas >= 2:
            categoria = "Recorrente"
        else:
            categoria = "Novo"

        items.append({
            "pac_reg":               r["pac_reg"],
            "nome":                  r["nome"],
            "nascimento":            r.get("nascimento"),
            "sexo":                  r.get("sexo"),
            "fone":                  r.get("fone") or None,
            "ultima_visita":         ultima,
            "total_visitas":         total_visitas,
            "dias_sem_visita":       dias_sem,
            "total_gasto_historico": round(total_gasto, 2),
            "ticket_medio_historico": round(total_gasto / total_visitas, 2) if total_visitas > 0 else 0,
            "categoria":             categoria,
            "unidade_frequente":     None,  # Removido para otimização de velocidade na lista
            "observacoes":           None
        })

    return {"total": total, "page": page, "limit": limit, "items": items}
