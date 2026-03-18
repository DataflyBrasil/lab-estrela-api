"""
Serviço de perfil individual de paciente.

Endpoints suportados:
  - search_pacientes(nome, page, limit) → lista paginada
  - get_paciente_perfil(pac_reg)        → perfil completo em paralelo
"""
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

    # CTE pré-agrega OSM → 1 linha por paciente antes de juntar com PAC.
    # Evita o padrão "explode linhas → GROUP BY" que força o SQL Server a
    # processar e colapsar todo o volume de OSM para depois paginar.
    query = f"""
    WITH osm_pac AS (
        SELECT
            o.osm_pac,
            MAX(o.osm_dthr)           AS ultima_dthr,
            COUNT(DISTINCT o.osm_num) AS total_visitas
        FROM OSM o WITH(NOLOCK)
        INNER JOIN STR s WITH(NOLOCK) ON s.str_cod = o.osm_str
        WHERE o.osm_status <> 'C'
          AND s.str_str_cod LIKE '{unit_prefix}'
        GROUP BY o.osm_pac
    )
    SELECT
        p.pac_reg,
        LTRIM(RTRIM(p.pac_nome))                      AS nome,
        CONVERT(varchar(10), p.pac_nasc, 120)          AS nascimento,
        p.pac_sexo                                      AS sexo,
        LTRIM(RTRIM(ISNULL(p.pac_fone, '')))           AS fone,
        CONVERT(varchar(10), op.ultima_dthr, 120)      AS ultima_visita,
        op.total_visitas,
        COUNT(*) OVER()                                AS total_count
    FROM PAC p WITH(NOLOCK)
    INNER JOIN osm_pac op ON op.osm_pac = p.pac_reg
    WHERE p.pac_nome LIKE '%{nome_safe}%'
      AND p.pac_nome NOT LIKE 'teste%'
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
        items.append({
            "pac_reg":         r["pac_reg"],
            "nome":            r["nome"],
            "nascimento":      r.get("nascimento"),
            "sexo":            r.get("sexo"),
            "fone":            r.get("fone") or None,
            "ultima_visita":   ultima,
            "total_visitas":   int(r.get("total_visitas") or 0),
            "dias_sem_visita": dias_sem,
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
        CONVERT(varchar(10), p.pac_dreg, 120)        AS data_cadastro
    FROM PAC p WITH(NOLOCK)
    WHERE p.pac_reg = {pac_reg}
    """

    # Financeiro + convênio principal fundidos em uma única varredura de OSM.
    # A subquery do convênio principal roda sobre o mesmo conjunto restrito
    # (pac_reg + status), que o SQL Server costuma materializar em memória.
    q_financeiro = f"""
    SELECT
        COUNT(DISTINCT o.osm_num)                                                              AS total_visitas,
        SUM(sm.smm_vlr)                                                                        AS total_gasto,
        CONVERT(varchar(10), MIN(o.osm_dthr), 120)                                             AS primeira_visita,
        CONVERT(varchar(10), MAX(o.osm_dthr), 120)                                             AS ultima_visita,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'C') = 'C' THEN sm.smm_vlr ELSE 0 END)      AS valor_particular,
        SUM(CASE WHEN ISNULL(c.cnv_caixa_fatura, 'F') = 'F' THEN sm.smm_vlr ELSE 0 END)      AS valor_convenio,
        (
            SELECT TOP 1 ISNULL(LTRIM(RTRIM(c2.cnv_nome)), 'Particular')
            FROM OSM o2 WITH(NOLOCK)
            LEFT JOIN CNV c2 WITH(NOLOCK) ON o2.osm_cnv = c2.cnv_cod
            WHERE o2.osm_pac = {pac_reg}
              AND o2.osm_status <> 'C'
            GROUP BY c2.cnv_nome
            ORDER BY COUNT(*) DESC
        ) AS convenio_principal
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    LEFT  JOIN CNV c  WITH(NOLOCK) ON o.osm_cnv = c.cnv_cod
    WHERE o.osm_pac = {pac_reg}
      AND o.osm_status <> 'C'
      AND ISNULL(sm.smm_sfat, '') <> 'C'
    """

    q_visitas = f"""
    SELECT TOP 20
        o.osm_num,
        CONVERT(varchar(19), o.osm_dthr, 120)     AS data,
        LTRIM(RTRIM(s.str_nome))                   AS unidade,
        SUM(sm.smm_vlr)                            AS valor,
        COUNT(sm.smm_num)                          AS qtd_exames
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s  WITH(NOLOCK) ON s.str_cod = o.osm_str
    WHERE o.osm_pac = {pac_reg}
      AND o.osm_status <> 'C'
      AND ISNULL(sm.smm_sfat, '') <> 'C'
    GROUP BY o.osm_num, o.osm_dthr, s.str_nome
    ORDER BY o.osm_dthr DESC
    """

    q_exames = f"""
    SELECT TOP 10
        LTRIM(RTRIM(k.smk_nome))  AS exame,
        SUM(sm.smm_qt)            AS frequencia,
        SUM(sm.smm_vlr)           AS valor_total
    FROM OSM o WITH(NOLOCK)
    INNER JOIN SMM sm WITH(NOLOCK) ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN SMK k  WITH(NOLOCK) ON k.smk_cod = sm.smm_cod AND k.smk_tipo = sm.smm_tpcod
    WHERE o.osm_pac = {pac_reg}
      AND o.osm_status <> 'C'
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
        SUM(i.IOP_VALOR)                            AS valor_total
    FROM ORP r WITH(NOLOCK)
    INNER JOIN IOP i WITH(NOLOCK) ON i.IOP_ORP_NUM = r.ORP_NUM
    WHERE r.ORP_PAC_REG = {pac_reg}
    GROUP BY r.ORP_NUM, r.ORP_DTHR, r.ORP_STATUS, r.ORP_OSM_NUM
    ORDER BY r.ORP_DTHR DESC
    """

    parallel = {
        "identidade": q_identidade,
        "financeiro": q_financeiro,
        "visitas":    q_visitas,
        "exames":     q_exames,
        "orcamentos": q_orcamentos,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(run_query_new_conn, q): key for key, q in parallel.items()}
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

    identidade = {
        "pac_reg":                 id_row.get("pac_reg", pac_reg),
        "nome":                    id_row.get("nome", ""),
        "nascimento":              nasc_str,
        "idade":                   idade,
        "sexo":                    id_row.get("sexo"),
        "fone":                    id_row.get("fone") or None,
        "data_cadastro":           cad_str,
        "tempo_como_paciente_dias": tempo_paciente,
    }

    # --- Financeiro + convênio (mesma linha) ---
    fin_row       = results["financeiro"][0] if results["financeiro"] else {}
    total_visitas = int(fin_row.get("total_visitas") or 0)
    total_gasto   = float(fin_row.get("total_gasto") or 0)
    val_part      = float(fin_row.get("valor_particular") or 0)
    val_conv      = float(fin_row.get("valor_convenio") or 0)
    ticket_medio  = total_gasto / total_visitas if total_visitas > 0 else 0.0
    total_fin     = val_part + val_conv
    pct_part      = (val_part / total_fin * 100) if total_fin > 0 else 0.0

    primeira_visita = fin_row.get("primeira_visita")
    ultima_visita   = fin_row.get("ultima_visita")
    dias_sem_visita = 0
    if ultima_visita:
        try:
            dias_sem_visita = (hoje - datetime.strptime(ultima_visita[:10], "%Y-%m-%d").date()).days
        except ValueError:
            pass

    financeiro = {
        "total_gasto":        round(total_gasto, 2),
        "ticket_medio":       round(ticket_medio, 2),
        "convenio_principal": fin_row.get("convenio_principal") or "Particular",
        "valor_particular":   round(val_part, 2),
        "valor_convenio":     round(val_conv, 2),
        "percent_particular": round(pct_part, 2),
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
        "categoria":       categoria,
        "total_visitas":   total_visitas,
        "primeira_visita": primeira_visita,
        "ultima_visita":   ultima_visita,
        "dias_sem_visita": dias_sem_visita,
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
        })

    return {
        "identidade":             identidade,
        "classificacao":          classificacao,
        "financeiro":             financeiro,
        "historico_visitas":      historico,
        "exames_mais_realizados": exames,
        "orcamentos":             orcamentos,
    }
