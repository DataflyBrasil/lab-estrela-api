"""
Serviço de Almoxarifado — Labora Estrela
Querys baseadas nas tabelas MAT, LOT e v_lot_total do banco Smart.
Compatível com DB 1 (Sisal) e DB 2 (Paulo Afonso) via current_db_id.
"""
from collections import Counter
from typing import List, Optional
from datetime import datetime, date, timedelta
from ..database import current_db_id


# ---------------------------------------------------------------------------
# KPIs de Estoque — visão geral de saldo, valor e criticidade
# ---------------------------------------------------------------------------

def get_stock_kpis(cursor) -> dict:
    """
    Retorna os KPIs principais do estoque:
    - total de itens ativos
    - itens com saldo positivo
    - itens abaixo do ponto de ressuprimento
    - valor total do estoque (preço médio × saldo)
    - lotes próximos ao vencimento (30 dias)
    - lotes vencidos
    """
    query = """
    SELECT
        COUNT(*) AS total_itens,
        SUM(CASE WHEN MAT_DEL_LOGICA = 'N' THEN 1 ELSE 0 END) AS total_ativos,
        SUM(CASE WHEN ISNULL(MAT_QT_EST_ATUAL, 0) > 0 THEN 1 ELSE 0 END) AS com_saldo,
        SUM(CASE WHEN ISNULL(MAT_QT_EST_ATUAL, 0) = 0 AND MAT_DEL_LOGICA = 'N' THEN 1 ELSE 0 END) AS sem_saldo,
        SUM(CASE WHEN MAT_IND_CURVA_ABC = 'A' AND MAT_DEL_LOGICA = 'N' THEN 1 ELSE 0 END) AS curva_a,
        SUM(CASE WHEN MAT_IND_CONTROLADO = 'S' AND MAT_DEL_LOGICA = 'N' THEN 1 ELSE 0 END) AS controlados,
        SUM(CASE WHEN MAT_IND_PERECIVEL = 'S' AND MAT_DEL_LOGICA = 'N' THEN 1 ELSE 0 END) AS pereceveis,
        ISNULL(SUM(
            CASE 
                WHEN ISNULL(MAT_QT_EST_ATUAL, 0) > 0 
                THEN MAT_QT_EST_ATUAL * ISNULL(mat_vlr_pm, 0)
                ELSE 0
            END
        ), 0) AS valor_total_estoque,
        SUM(CASE 
            WHEN MAT_PT_RESSUPRIMENTO IS NOT NULL 
                AND ISNULL(MAT_QT_EST_ATUAL, 0) <= MAT_PT_RESSUPRIMENTO 
                AND MAT_DEL_LOGICA = 'N'
            THEN 1 ELSE 0 
        END) AS abaixo_ressuprimento
    FROM MAT WITH(NOLOCK)
    """
    cursor.execute(query)
    row = cursor.fetchone()
    
    if not row:
        return {}

    # Se o cursor foi criado com as_dict=True, row já é um dict
    if isinstance(row, dict):
        kpis = row
    else:
        cols = [d[0] for d in cursor.description]
        kpis = dict(zip(cols, row))

    # Lotes vencendo em 30 dias e lotes já vencidos
    # ... logic for dates ...
    hoje = date.today().strftime('%Y-%m-%d')
    daqui_30 = (date.today() + timedelta(days=30)).strftime('%Y-%m-%d')

    cursor.execute(f"""
        SELECT 
            COUNT(CASE WHEN LOT_DATA_VALIDADE < '{hoje}' AND LOT_STATUS = 'S' THEN 1 END) AS lotes_vencidos,
            COUNT(CASE WHEN LOT_DATA_VALIDADE BETWEEN '{hoje}' AND '{daqui_30}' AND LOT_STATUS = 'S' THEN 1 END) AS lotes_vencendo_30d
        FROM LOT WITH(NOLOCK)
        WHERE LOT_SALDO > 0
    """)
    row2 = cursor.fetchone()
    if row2:
        if isinstance(row2, dict):
            kpis.update(row2)
        else:
            cols2 = [d[0] for d in cursor.description]
            kpis.update(dict(zip(cols2, row2)))

    # Convert Decimals/None to safe types
    clean_kpis = {}
    for k, v in kpis.items():
        if v is None:
            clean_kpis[k] = 0
        else:
            try:
                # Trata Decimals e conversões numéricas
                clean_kpis[k] = float(v) if ('.' in str(v) or hasattr(v, 'to_eng_string')) else int(v)
            except (ValueError, TypeError):
                clean_kpis[k] = 0

    return clean_kpis


# ---------------------------------------------------------------------------
# Catálogo de Estoque — lista paginada de itens com saldo
# ---------------------------------------------------------------------------

_STATUS_CONDITIONS = {
    'critico': "ISNULL(MAT_QT_EST_ATUAL, 0) <= 0",
    'alerta': (
        "ISNULL(MAT_PT_SEGURANCA, 0) > 0 "
        "AND ISNULL(MAT_QT_EST_ATUAL, 0) > 0 "
        "AND ISNULL(MAT_QT_EST_ATUAL, 0) <= ISNULL(MAT_PT_SEGURANCA, 0)"
    ),
    'atencao': (
        "ISNULL(MAT_PT_RESSUPRIMENTO, 0) > 0 "
        "AND ISNULL(MAT_QT_EST_ATUAL, 0) > 0 "
        "AND ISNULL(MAT_QT_EST_ATUAL, 0) <= ISNULL(MAT_PT_RESSUPRIMENTO, 0) "
        "AND (ISNULL(MAT_PT_SEGURANCA, 0) = 0 OR ISNULL(MAT_QT_EST_ATUAL, 0) > ISNULL(MAT_PT_SEGURANCA, 0))"
    ),
    'ok': (
        "ISNULL(MAT_QT_EST_ATUAL, 0) > 0 "
        "AND (ISNULL(MAT_PT_RESSUPRIMENTO, 0) = 0 OR ISNULL(MAT_QT_EST_ATUAL, 0) > ISNULL(MAT_PT_RESSUPRIMENTO, 0))"
    ),
}


def get_stock_catalog(
    cursor,
    sba_cod: Optional[str] = None,
    curva_abc: Optional[str] = None,
    apenas_com_saldo: bool = False,
    page: int = 1,
    limit: int = 50,
    sort_by: Optional[str] = None,
    sort_dir: str = "ASC",
    status_estoque: Optional[str] = None,
) -> dict:
    """
    Retorna a lista paginada de materiais com seus saldos atuais e indicadores.
    """
    conditions = ["MAT_DEL_LOGICA = 'N'"]
    params: list = []

    if sba_cod:
        conditions.append("LTRIM(RTRIM(MAT_SBA_COD)) = ?")
        params.append(sba_cod.strip())
    if curva_abc:
        conditions.append("MAT_IND_CURVA_ABC = ?")
        params.append(curva_abc.strip())
    if apenas_com_saldo:
        conditions.append("ISNULL(MAT_QT_EST_ATUAL, 0) > 0")
    if status_estoque and status_estoque in _STATUS_CONDITIONS:
        conditions.append(_STATUS_CONDITIONS[status_estoque])

    where = " AND ".join(conditions)
    offset = (page - 1) * limit

    cursor.execute(f"SELECT COUNT(*) AS total FROM MAT WITH(NOLOCK) WHERE {where}", params)
    row_count = cursor.fetchone()
    total = (row_count['total'] if isinstance(row_count, dict) else row_count[0]) or 0

    sort_mapping = {
        'cod': 'MAT_COD',
        'descricao': 'MAT_DESC_COMPLETA',
        'saldo_atual': 'ISNULL(MAT_QT_EST_ATUAL, 0)',
        'curva_abc': 'MAT_IND_CURVA_ABC',
        'preco_medio': 'ISNULL(mat_vlr_pm, 0)',
        'valor_total': 'ROUND(ISNULL(MAT_QT_EST_ATUAL, 0) * ISNULL(mat_vlr_pm, 0), 2)',
        'status': 'ISNULL(MAT_QT_EST_ATUAL, 0)',
    }
    order_by = "MAT_IND_CURVA_ABC, ISNULL(MAT_QT_EST_ATUAL, 0) ASC"
    if sort_by and sort_by in sort_mapping:
        direction = "ASC" if sort_dir.upper() == "ASC" else "DESC"
        order_by = f"{sort_mapping[sort_by]} {direction}"

    cursor.execute(f"""
        SELECT
            MAT_COD AS cod,
            LTRIM(RTRIM(MAT_DESC_COMPLETA)) AS descricao,
            LTRIM(RTRIM(MAT_DESC_RESUMIDA)) AS descricao_resumida,
            ISNULL(MAT_QT_EST_ATUAL, 0) AS saldo_atual,
            ISNULL(MAT_ESTOQ_MAXIMO, 0) AS estoque_maximo,
            ISNULL(MAT_PT_RESSUPRIMENTO, 0) AS ponto_ressuprimento,
            ISNULL(MAT_PT_SEGURANCA, 0) AS ponto_seguranca,
            ISNULL(mat_vlr_pm, 0) AS preco_medio,
            ISNULL(MAT_PRC_ULT_ENTRADA, 0) AS preco_ult_entrada,
            LTRIM(RTRIM(ISNULL(MAT_SBA_COD, ''))) AS sub_almox,
            LTRIM(RTRIM(ISNULL(MAT_UNM_COD_SAIDA, ''))) AS unidade_medida,
            ISNULL(MAT_IND_CURVA_ABC, '') AS curva_abc,
            ISNULL(MAT_IND_PERECIVEL, 'N') AS perecivel,
            ISNULL(MAT_IND_CONTROLADO, 'N') AS controlado,
            ISNULL(MAT_IND_CRITICIDADE, 'N') AS criticidade,
            MAT_DTHR_ULT_ENTRADA AS ultima_entrada,
            MAT_DTHR_ULT_SAIDA AS ultima_saida,
            ISNULL(MAT_CONS_MEDIO, 0) AS consumo_medio,
            CASE
                WHEN ISNULL(MAT_QT_EST_ATUAL, 0) <= 0 THEN 'critico'
                WHEN ISNULL(MAT_PT_SEGURANCA, 0) > 0
                     AND ISNULL(MAT_QT_EST_ATUAL, 0) <= ISNULL(MAT_PT_SEGURANCA, 0) THEN 'alerta'
                WHEN ISNULL(MAT_PT_RESSUPRIMENTO, 0) > 0
                     AND ISNULL(MAT_QT_EST_ATUAL, 0) <= ISNULL(MAT_PT_RESSUPRIMENTO, 0) THEN 'atencao'
                ELSE 'ok'
            END AS status_estoque,
            ROUND(ISNULL(MAT_QT_EST_ATUAL, 0) * ISNULL(mat_vlr_pm, 0), 2) AS valor_total
        FROM MAT WITH(NOLOCK)
        WHERE {where}
        ORDER BY {order_by}
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """, params)

    rows = cursor.fetchall()
    if not rows:
        return {'total': total, 'page': page, 'limit': limit, 'items': []}

    is_dict = isinstance(rows[0], dict)
    cols = None if is_dict else [d[0] for d in cursor.description]

    items = []
    for row in rows:
        item = row if is_dict else dict(zip(cols, row))
        for k in ['saldo_atual', 'estoque_maximo', 'ponto_ressuprimento', 'ponto_seguranca',
                  'preco_medio', 'preco_ult_entrada', 'consumo_medio', 'valor_total']:
            v = item.get(k)
            item[k] = float(v) if v is not None else 0.0
        for k in ['ultima_entrada', 'ultima_saida']:
            v = item.get(k)
            item[k] = str(v)[:19] if v else None
        items.append(item)

    return {'total': total, 'page': page, 'limit': limit, 'items': items}


# ---------------------------------------------------------------------------
# Sub-almoxarifados disponíveis
# ---------------------------------------------------------------------------

def get_sub_almoxarifados(cursor) -> List[str]:
    cursor.execute("""
        SELECT DISTINCT LTRIM(RTRIM(MAT_SBA_COD)) AS sba 
        FROM MAT WITH(NOLOCK) 
        WHERE MAT_DEL_LOGICA = 'N' AND MAT_SBA_COD IS NOT NULL AND LTRIM(RTRIM(MAT_SBA_COD)) != ''
        ORDER BY sba
    """)
    rows = cursor.fetchall()
    if not rows:
        return []
    
    if isinstance(rows[0], dict):
        return [row['sba'] for row in rows]
    return [row[0] for row in rows]


# ---------------------------------------------------------------------------
# Histórico de Recebimento por Lotes
# ---------------------------------------------------------------------------

def get_lot_receiving_history(
    cursor,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    sba_cod: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
    sort_by: Optional[str] = None,
    sort_dir: str = "DESC"
) -> dict:
    """
    Histórico de entradas de material via lotes (NF-e de entrada).
    COUNT e dados em uma única query usando COUNT(*) OVER().
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    date_filter = f"LOT.LOT_DATA_ENTRADA BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    sba_filter = f"AND LTRIM(RTRIM(LOT.LOT_SBA_COD)) = '{sba_cod.strip()}'" if sba_cod else ""

    offset = (page - 1) * limit

    sort_mapping = {
        'data_entrada': 'LOT.LOT_DATA_ENTRADA',
        'material': 'M.MAT_DESC_RESUMIDA',
        'lote_num': 'LOT.LOT_NUM',
        'quantidade': 'LOT.LOT_QUANT',
        'saldo_lote': 'LOT.LOT_SALDO',
        'data_validade': 'LOT.LOT_DATA_VALIDADE',
        'nfe_num': 'LOT.lot_ine_nfe_num',
    }
    order_by = "LOT.LOT_DATA_ENTRADA DESC"
    if sort_by and sort_by in sort_mapping:
        direction = "ASC" if sort_dir.upper() == "ASC" else "DESC"
        order_by = f"{sort_mapping[sort_by]} {direction}"

    cursor.execute(f"""
        SELECT
            LOT.LOT_MAT_COD                          AS mat_cod,
            LTRIM(RTRIM(M.MAT_DESC_RESUMIDA))         AS material,
            LTRIM(RTRIM(LOT.LOT_SBA_COD))             AS sub_almox,
            LTRIM(RTRIM(LOT.LOT_NUM))                 AS lote_num,
            LOT.LOT_DATA_ENTRADA                      AS data_entrada,
            LOT.LOT_DATA_VALIDADE                     AS data_validade,
            LOT.LOT_QUANT                             AS quantidade,
            LOT.LOT_SALDO                             AS saldo_lote,
            LTRIM(RTRIM(LOT.LOT_PROCED))              AS procedencia,
            LOT.lot_ine_nfe_num                       AS nfe_num,
            LTRIM(RTRIM(LOT.LOT_STATUS))              AS status,
            LTRIM(RTRIM(M.MAT_UNM_COD_SAIDA))         AS unidade,
            COUNT(*) OVER ()                          AS total_count
        FROM LOT WITH(NOLOCK)
        INNER JOIN MAT M WITH(NOLOCK) ON M.MAT_COD = LOT.LOT_MAT_COD
        WHERE {date_filter} {sba_filter}
        ORDER BY {order_by}
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """)

    cols_info = cursor.description
    rows = cursor.fetchall()

    if not rows:
        return {'total': 0, 'page': page, 'limit': limit, 'items': []}

    is_dict = isinstance(rows[0], dict)
    cols = None if is_dict else [d[0] for d in cols_info]

    items = []
    total = 0
    for row in rows:
        item = row if is_dict else dict(zip(cols, row))
        total = int(item.pop('total_count', 0) or 0)
        for k in ['quantidade', 'saldo_lote']:
            v = item.get(k)
            item[k] = float(v) if v is not None else 0.0
        for k in ['data_entrada', 'data_validade']:
            v = item.get(k)
            item[k] = str(v)[:19] if v else None
        items.append(item)

    return {'total': total, 'page': page, 'limit': limit, 'items': items}


# ---------------------------------------------------------------------------
# Alertas de Vencimento de Lotes
# ---------------------------------------------------------------------------

def get_expiry_alerts(
    cursor, 
    days_ahead: int = 90,
    page: int = 1,
    limit: int = 10,
    sort_by: Optional[str] = None,
    sort_dir: str = "ASC"
) -> dict:
    """
    Retorna lotes vencidos e lotes próximos ao vencimento (dentro de `days_ahead` dias).
    Apenas lotes com saldo > 0 são considerados.
    """
    offset = (page - 1) * limit
    hoje = date.today().strftime('%Y-%m-%d')
    limite_data = (date.today() + timedelta(days=days_ahead)).strftime('%Y-%m-%d')

    # Sorting logic
    sort_mapping = {
        'material': 'M.MAT_DESC_RESUMIDA',
        'lote_num': 'LOT.LOT_NUM',
        'data_validade': 'LOT.LOT_DATA_VALIDADE',
        'saldo': 'LOT.LOT_SALDO',
        'dias_para_vencer': 'DATEDIFF(day, GETDATE(), LOT_DATA_VALIDADE)'
    }
    
    order_by = "LOT.LOT_DATA_VALIDADE ASC"
    if sort_by and sort_by in sort_mapping:
        direction = "ASC" if sort_dir.upper() == "ASC" else "DESC"
        order_by = f"{sort_mapping[sort_by]} {direction}"

    # Query única: dados paginados + total e resumo via window functions (OVER())
    # COUNT(*) OVER() e SUM() OVER() operam sobre todo o conjunto filtrado antes do OFFSET/FETCH
    cursor.execute(f"""
        SELECT
            LOT.LOT_MAT_COD                                                        AS mat_cod,
            LTRIM(RTRIM(M.MAT_DESC_RESUMIDA))                                       AS material,
            LTRIM(RTRIM(LOT.LOT_SBA_COD))                                           AS sub_almox,
            LTRIM(RTRIM(LOT.LOT_NUM))                                               AS lote_num,
            LOT.LOT_DATA_VALIDADE                                                   AS data_validade,
            LOT.LOT_SALDO                                                           AS saldo,
            LTRIM(RTRIM(M.MAT_UNM_COD_SAIDA))                                       AS unidade,
            CASE
                WHEN LOT.LOT_DATA_VALIDADE < '{hoje}'                              THEN 'vencido'
                WHEN LOT.LOT_DATA_VALIDADE <= DATEADD(day, 30, '{hoje}')           THEN 'critico'
                WHEN LOT.LOT_DATA_VALIDADE <= DATEADD(day, 60, '{hoje}')           THEN 'alerta'
                ELSE 'atencao'
            END                                                                    AS nivel_alerta,
            DATEDIFF(day, '{hoje}', LOT.LOT_DATA_VALIDADE)                          AS dias_para_vencer,
            COUNT(*) OVER ()                                                        AS total_count,
            SUM(CASE WHEN LOT.LOT_DATA_VALIDADE < '{hoje}' THEN 1 ELSE 0 END) OVER ()
                                                                                   AS cnt_vencidos,
            SUM(CASE WHEN LOT.LOT_DATA_VALIDADE >= '{hoje}'
                      AND LOT.LOT_DATA_VALIDADE <= DATEADD(day, 30, '{hoje}')
                 THEN 1 ELSE 0 END) OVER ()                                        AS cnt_criticos,
            SUM(CASE WHEN LOT.LOT_DATA_VALIDADE > DATEADD(day, 30, '{hoje}')
                      AND LOT.LOT_DATA_VALIDADE <= DATEADD(day, 60, '{hoje}')
                 THEN 1 ELSE 0 END) OVER ()                                        AS cnt_alertas,
            SUM(CASE WHEN LOT.LOT_DATA_VALIDADE > DATEADD(day, 60, '{hoje}')
                      AND LOT.LOT_DATA_VALIDADE <= DATEADD(day, 90, '{hoje}')
                 THEN 1 ELSE 0 END) OVER ()                                        AS cnt_atencao
        FROM LOT WITH(NOLOCK)
        INNER JOIN MAT M WITH(NOLOCK) ON M.MAT_COD = LOT.LOT_MAT_COD
        WHERE LOT.LOT_SALDO > 0
          AND LOT.LOT_DATA_VALIDADE <= '{limite_data}'
        ORDER BY {order_by}
        OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
    """)

    cols_info = cursor.description
    rows = cursor.fetchall()

    if not rows:
        resumo = {'vencidos': 0, 'criticos': 0, 'alertas': 0, 'atencao': 0}
        return {'total': 0, 'resumo': resumo, 'items': [], 'page': page, 'limit': limit}

    is_dict = isinstance(rows[0], dict)
    cols = None if is_dict else [d[0] for d in cols_info]

    items = []
    total = 0
    resumo = {'vencidos': 0, 'criticos': 0, 'alertas': 0, 'atencao': 0}
    for row in rows:
        item = row if is_dict else dict(zip(cols, row))
        if total == 0:
            total = int(item.get('total_count', 0) or 0)
            resumo = {
                'vencidos': int(item.get('cnt_vencidos', 0) or 0),
                'criticos': int(item.get('cnt_criticos', 0) or 0),
                'alertas':  int(item.get('cnt_alertas',  0) or 0),
                'atencao':  int(item.get('cnt_atencao',  0) or 0),
            }
        for k in ('total_count', 'cnt_vencidos', 'cnt_criticos', 'cnt_alertas', 'cnt_atencao'):
            item.pop(k, None)
        try:
            item['saldo'] = float(item.get('saldo', 0))
        except (ValueError, TypeError):
            item['saldo'] = 0.0
        try:
            item['dias_para_vencer'] = int(item.get('dias_para_vencer', 0))
        except (ValueError, TypeError):
            item['dias_para_vencer'] = 0
        v = item.get('data_validade')
        item['data_validade'] = str(v)[:10] if v else None
        items.append(item)

    return {'total': total, 'resumo': resumo, 'items': items, 'page': page, 'limit': limit}


# ---------------------------------------------------------------------------
# Solicitações de Materiais Pendentes
# ---------------------------------------------------------------------------

def get_pending_requests(
    cursor,
    data_de: Optional[str] = None,
    data_ate: Optional[str] = None,
) -> List[dict]:
    """
    Busca solicitações de materiais do almoxarifado no período informado.

    Query única com JOINs corretos entre SMA→STR, SMA→ISM, ISM→MAT.
    Elimina o bug anterior que usava IN(series) AND IN(nums) separados,
    causando produto cartesiano incorreto entre série e número.

    Mapeamento validado contra banco Smart (DB1/Serrinha):
      - sma_tipo: S0=Consumo Interno, ST=Transferência, D0=Devolução
      - ISM_PRI_COD: '1'=ALTA, '2'=MÉDIA, '3'=BAIXA, else=NORMAL
    """
    if not data_de:
        data_de = date.today().replace(day=1).strftime('%Y-%m-%d')
    if not data_ate:
        data_ate = date.today().strftime('%Y-%m-%d')

    cursor.execute(f"""
        SELECT
            SMA.SMA_SERIE                                               AS serie,
            SMA.SMA_NUM                                                 AS num,
            SMA.SMA_DATA                                                AS data,
            CASE
                WHEN SMA.sma_tipo = 'S0' THEN 'Solic. Consumo Interno'
                WHEN SMA.sma_tipo = 'ST' THEN 'Transferência'
                WHEN SMA.sma_tipo = 'D0' THEN 'Devolução'
                ELSE LTRIM(RTRIM(ISNULL(SMA.sma_tipo, 'Solicitação')))
            END                                                         AS tipo,
            LTRIM(RTRIM(S.str_nome))                                    AS setor,
            LTRIM(RTRIM(SMA.SMA_USR_LOGIN_SOL))                         AS solicitante,
            ISNULL(LTRIM(RTRIM(SMA.sma_status)), 'P')                   AS status,
            LTRIM(RTRIM(ISNULL(SMA.SMA_OBS, '')))                       AS observacao,
            LTRIM(RTRIM(ISNULL(SMA.sma_sba_cod, '')))                   AS sba_cod,
            I.ISM_MAT_COD                                               AS cod,
            LTRIM(RTRIM(M.MAT_DESC_COMPLETA))                           AS descricao,
            CAST(ISNULL(I.ISM_QTDE_SOLICITADA, 0) AS FLOAT)             AS qtde,
            LTRIM(RTRIM(ISNULL(M.MAT_UNM_COD_SAIDA, 'UND')))           AS unidade,
            CAST(ISNULL(I.ISM_QTDE_SOLICITADA, 0)
               - ISNULL(I.ISM_QTDE_BAIXA, 0) AS FLOAT)                 AS pendente,
            CASE LTRIM(RTRIM(ISNULL(CAST(I.ISM_PRI_COD AS VARCHAR), '')))
                WHEN '1' THEN 'ALTA'
                WHEN '2' THEN 'MÉDIA'
                WHEN '3' THEN 'BAIXA'
                ELSE 'NORMAL'
            END                                                         AS prioridade
        FROM SMA WITH(NOLOCK)
        INNER JOIN STR S WITH(NOLOCK)
            ON SMA.sma_str_cod = S.str_cod
        INNER JOIN ISM I WITH(NOLOCK)
            ON I.ISM_SMA_SERIE = SMA.SMA_SERIE
           AND I.ISM_SMA_NUM   = SMA.SMA_NUM
        INNER JOIN MAT M WITH(NOLOCK)
            ON I.ISM_MAT_COD = M.MAT_COD
        WHERE CAST(SMA.SMA_DATA AS DATE) >= '{data_de}'
          AND CAST(SMA.SMA_DATA AS DATE) <= '{data_ate}'
        ORDER BY SMA.SMA_DATA DESC, SMA.SMA_SERIE, SMA.SMA_NUM, I.ISM_MAT_COD
    """)

    rows = cursor.fetchall()
    if not rows:
        return []

    is_dict = isinstance(rows[0], dict)
    if not is_dict:
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in rows]

    # Agrupa por (serie, num) preservando a ordem de aparição
    headers: dict = {}
    for row in rows:
        key = (row['serie'], row['num'])
        if key not in headers:
            raw_data = row['data']
            headers[key] = {
                'serie':       row['serie'],
                'num':         row['num'],
                'data':        raw_data.strftime('%Y-%m-%d %H:%M:%S') if isinstance(raw_data, (datetime, date)) else str(raw_data),
                'tipo':        row['tipo'],
                'setor':       row['setor'],
                'solicitante': row['solicitante'],
                'status':      row['status'],
                'observacao':  row['observacao'],
                'sba_cod':     row['sba_cod'],
                'itens':       [],
            }
        if row.get('cod') is not None:
            headers[key]['itens'].append({
                'cod':       row['cod'],
                'descricao': row['descricao'],
                'qtde':      row['qtde'],
                'unidade':   row['unidade'],
                'pendente':  row['pendente'],
                'prioridade': row['prioridade'],
            })

    return list(headers.values())
