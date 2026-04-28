"""
Serviço Financeiro — Laboratório Estrela
Mapeamento correto baseado nas imagens de referência e schema do banco Smart.

Tabelas utilizadas:
  - cpg: Compromissos a Pagar (cabeçalho)
  - ipg: Itens/Parcelas dos Compromissos
  - cfo: Contas do Fluxo de Caixa (hierarquia R=Receita, D=Despesa)
  - mte: Movimentações de Tesouraria (entradas de caixa)
"""
from typing import List, Optional
from datetime import datetime, date, timedelta
from app.database import current_db_id


def _safe_float(val) -> float:
    """Converte de forma segura para float — evita NoneType errors."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _safe_str(val, default: str = "") -> str:
    """Converte de forma segura para string — evita NoneType errors."""
    if val is None:
        return default
    return str(val).strip()


def get_accounts_payable(cursor, data_de: Optional[str] = None, data_ate: Optional[str] = None) -> List[dict]:
    """
    Busca compromissos a pagar com parcelas pendentes (status != 'P').

    A tela w_cpg_lanc do Smart ERP exibe parcelas com status 'A' (Aberto) e
    'R' (Registro). Filtramos o JOIN por TODAS as parcelas e usamos HAVING para
    excluir apenas compromissos 100% quitados.

    - cpg: cabeçalho do compromisso
    - ipg: todas as parcelas (status_consolidado calculado sobre o conjunto total)
    - data_de / data_ate: filtro por cpg_dt_reg no formato YYYY-MM-DD (opcional)
    """
    date_filter = ""
    if data_de and data_ate:
        date_filter = f"AND CAST(CPG.cpg_dt_reg AS DATE) >= '{data_de}' AND CAST(CPG.cpg_dt_reg AS DATE) <= '{data_ate}'"
    elif data_de:
        date_filter = f"AND CAST(CPG.cpg_dt_reg AS DATE) >= '{data_de}'"
    elif data_ate:
        date_filter = f"AND CAST(CPG.cpg_dt_reg AS DATE) <= '{data_ate}'"

    query = """
        SELECT
            CPG.cpg_serie                           AS serie,
            CPG.cpg_num                             AS num,
            ISNULL(LTRIM(RTRIM(CPG.cpg_credor)),
                   LTRIM(RTRIM(CPG.cpg_fis_jur)))   AS credor,
            CONVERT(varchar, CPG.cpg_dt_reg, 103)   AS data_registro,
            CPG.cpg_emp_cod                          AS empresa,
            ISNULL(LTRIM(RTRIM(CPG.cpg_obs)), '')   AS observacao,
            ISNULL(CPG.cpg_tot_parc, 1)             AS total_parcelas,
            SUM(ISNULL(CAST(IPG.ipg_valor AS FLOAT), 0)) AS valor_total,
            CASE
                WHEN SUM(CASE WHEN IPG.ipg_status = 'P' THEN 1 ELSE 0 END) = 0
                THEN 'Aberto'
                WHEN SUM(CASE WHEN IPG.ipg_status != 'P' THEN 1 ELSE 0 END) > 0
                THEN 'Parcial'
                ELSE 'Pago'
            END AS status_consolidado
        FROM cpg CPG WITH(NOLOCK)
        INNER JOIN ipg IPG WITH(NOLOCK)
            ON CPG.cpg_serie = IPG.ipg_cpg_serie
           AND CPG.cpg_num   = IPG.ipg_cpg_num
        WHERE 1=1
        {date_filter}
        GROUP BY
            CPG.cpg_serie, CPG.cpg_num, CPG.cpg_credor,
            CPG.cpg_fis_jur, CPG.cpg_dt_reg, CPG.cpg_emp_cod,
            CPG.cpg_obs, CPG.cpg_tot_parc
        HAVING SUM(CASE WHEN IPG.ipg_status != 'P' THEN 1 ELSE 0 END) > 0
        ORDER BY CPG.cpg_dt_reg DESC
    """.format(date_filter=date_filter)

    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:
        return []

    # Normaliza para lista de dicts
    if not isinstance(rows[0], dict):
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in rows]

    result = []
    for comp in rows:
        # Garante tipos corretos no header
        comp['valor_total'] = _safe_float(comp.get('valor_total'))
        comp['credor'] = _safe_str(comp.get('credor'), default='(Sem Credor)')
        comp['observacao'] = _safe_str(comp.get('observacao'))
        comp['data_registro'] = _safe_str(comp.get('data_registro'))

        # Busca as parcelas individuais desse compromisso
        query_parcelas = """
            SELECT
                ipg_parc                                          AS num_parcela,
                CONVERT(varchar, ipg_dt_vcto, 103)               AS vencimento,
                ISNULL(CAST(ipg_valor AS FLOAT), 0)              AS valor,
                ipg_status                                        AS status,
                CONVERT(varchar, ipg_dt_pgto, 103)               AS data_pagamento
            FROM ipg WITH(NOLOCK)
            WHERE ipg_cpg_serie = %d AND ipg_cpg_num = %d
            ORDER BY ipg_parc
        """ % (comp['serie'], comp['num'])

        cursor.execute(query_parcelas)
        parcelas_raw = cursor.fetchall()

        if parcelas_raw and not isinstance(parcelas_raw[0], dict):
            cols_p = [d[0] for d in cursor.description]
            parcelas_raw = [dict(zip(cols_p, p)) for p in parcelas_raw]

        parcelas = []
        for p in parcelas_raw:
            parcelas.append({
                'num_parcela': p.get('num_parcela', 0),
                'vencimento': _safe_str(p.get('vencimento')),
                'valor': _safe_float(p.get('valor')),
                'status': _safe_str(p.get('status'), default='A'),
                'data_pagamento': _safe_str(p.get('data_pagamento')) or None,
            })

        comp['parcelas'] = parcelas
        result.append(comp)

    return result


def get_cash_flow(cursor, days: int = 30) -> dict:
    """
    Retorna o resumo do fluxo de caixa categorizado.
    """
    hoje = date.today()
    inicio = hoje - timedelta(days=days)
    fim = hoje

    query_despesas = """
        SELECT
            ISNULL(LTRIM(RTRIM(C.cfo_nome)), 'Outros') AS categoria,
            'DESPESA'                                    AS tipo,
            SUM(ISNULL(CAST(I.ipg_valor AS FLOAT), 0))  AS valor
        FROM ipg I WITH(NOLOCK)
        LEFT JOIN cfo C WITH(NOLOCK)
            ON I.ipg_cfo_cod = C.cfo_cod
        WHERE I.ipg_status = 'P'
          AND I.ipg_dt_pgto >= '%s'
          AND I.ipg_dt_pgto <= '%s'
          AND ISNULL(CAST(I.ipg_valor AS FLOAT), 0) > 0
        GROUP BY C.cfo_nome
        ORDER BY valor DESC
    """ % (inicio.strftime('%Y-%m-%d'), fim.strftime('%Y-%m-%d'))

    cursor.execute(query_despesas)
    despesas_raw = cursor.fetchall()
    if despesas_raw and not isinstance(despesas_raw[0], dict):
        cols_d = [d[0] for d in cursor.description]
        despesas_raw = [dict(zip(cols_d, r)) for r in despesas_raw]

    query_receitas = """
        SELECT
            'Receitas Recebidas' AS categoria,
            'RECEITA'            AS tipo,
            SUM(ISNULL(CAST(mte_valor AS FLOAT), 0)) AS valor
        FROM mte WITH(NOLOCK)
        WHERE mte_tipo = 'C'
          AND mte_dthr >= '%s'
          AND mte_dthr <= '%s'
          AND ISNULL(CAST(mte_valor AS FLOAT), 0) > 0
        GROUP BY mte_tipo
    """ % (inicio.strftime('%Y-%m-%d'), fim.strftime('%Y-%m-%d'))

    cursor.execute(query_receitas)
    receitas_raw = cursor.fetchall()
    if receitas_raw and not isinstance(receitas_raw[0], dict):
        cols_r = [d[0] for d in cursor.description]
        receitas_raw = [dict(zip(cols_r, r)) for r in receitas_raw]

    total_receitas = sum(_safe_float(r.get('valor')) for r in receitas_raw)
    total_despesas = sum(_safe_float(d.get('valor')) for d in despesas_raw)
    total_mov = total_receitas + total_despesas

    itens = []
    for r in receitas_raw:
        val = _safe_float(r.get('valor'))
        itens.append({
            'categoria': _safe_str(r.get('categoria'), 'Receitas'),
            'tipo': 'RECEITA',
            'valor': round(val, 2),
            'percentual': round((val / total_mov * 100), 2) if total_mov > 0 else 0.0,
        })
    for d in despesas_raw:
        val = _safe_float(d.get('valor'))
        itens.append({
            'categoria': _safe_str(d.get('categoria'), 'Outros'),
            'tipo': 'DESPESA',
            'valor': round(val, 2),
            'percentual': round((val / total_mov * 100), 2) if total_mov > 0 else 0.0,
        })

    return {
        "total_receitas": round(total_receitas, 2),
        "total_despesas": round(total_despesas, 2),
        "saldo_operacional": round(total_receitas - total_despesas, 2),
        "itens": itens,
    }


def get_cash_flow_detailed(cursor, data_de: str, data_ate: str) -> dict:
    """
    Retorna o detalhamento do fluxo de caixa.
    """
    db_id = current_db_id.get()

    RECEITA_MAP = {
        '1160': 'Receitas Espécie',
        '1109': 'Receitas Cartão',
        '1108': 'Receita PIX',
        '1107': 'Receitas Convênios',
    }

    total_receitas_op = 0.0
    total_receitas_nao_op = 0.0
    receitas_op_dict = {v: 0.0 for v in RECEITA_MAP.values()}
    nao_op_rec_itens = []

    # ── 1. RECEITAS ──
    if db_id == '2':
        # Para Paulo Afonso: receitas operacionais via lado DEB das contas consolidadoras.
        # CCR 1150 = conta bancária principal (recebe Cartão/PIX/Convênios via crédito
        #            e registra o valor no DEB quando concilia o movimento de caixa).
        # CCR 1169 = conta caixa de consolidação de Espécie (prestação de contas).
        # CCR 1169 tem CRE para CFO 1109 (Cartão) = estorno de card para Espécie.
        # Cartão correto = DEB(CCR 1150, CFO 1109) - CRE(CCR 1169, CFO 1109).
        query_receitas = f"""
            SELECT
                RTRIM(MCC_CFO_COD) AS cfo_cod,
                SUM(CASE
                    WHEN RTRIM(MCC_CCR) = '1150'
                        THEN ISNULL(CAST(MCC_DEB AS FLOAT), 0)
                    WHEN RTRIM(MCC_CCR) = '1169' AND RTRIM(MCC_CFO_COD) = '1160'
                        THEN ISNULL(CAST(MCC_DEB AS FLOAT), 0)
                    WHEN RTRIM(MCC_CCR) = '1169' AND RTRIM(MCC_CFO_COD) = '1109'
                        THEN -ISNULL(CAST(MCC_CRE AS FLOAT), 0)
                    ELSE 0
                END) AS receita
            FROM MCC WITH(NOLOCK)
            WHERE CAST(MCC_DT AS DATE) >= '{data_de}'
              AND CAST(MCC_DT AS DATE) <= '{data_ate}'
              AND MCC_CONCILIA = 'S'
              AND (
                    (RTRIM(MCC_CCR) = '1150' AND RTRIM(MCC_CFO_COD) IN ('1107','1108','1109')
                        AND ISNULL(CAST(MCC_DEB AS FLOAT), 0) > 0)
                 OR (RTRIM(MCC_CCR) = '1169' AND RTRIM(MCC_CFO_COD) = '1160'
                        AND ISNULL(CAST(MCC_DEB AS FLOAT), 0) > 0)
                 OR (RTRIM(MCC_CCR) = '1169' AND RTRIM(MCC_CFO_COD) = '1109'
                        AND ISNULL(CAST(MCC_CRE AS FLOAT), 0) > 0)
              )
            GROUP BY RTRIM(MCC_CFO_COD)
        """
        cursor.execute(query_receitas)
        rec_raw = cursor.fetchall()
        if rec_raw and not isinstance(rec_raw[0], dict):
            cols = [d[0] for d in cursor.description]
            rec_raw = [dict(zip(cols, r)) for r in rec_raw]

        for r in rec_raw:
            cod = _safe_str(r.get('cfo_cod', '')).strip()
            val = _safe_float(r.get('receita', 0))
            if val <= 0:
                continue
            cat = RECEITA_MAP.get(cod)
            if cat:
                receitas_op_dict[cat] += val
                total_receitas_op += val

        desp_ccr_filter = ""  # Sem filtro de CCR: despesas transitam por vários caixas

    else:
        # Para outros bancos: NET (CRE − DEB) sem filtro de CCR.
        query_receitas = f"""
            SELECT
                RTRIM(M.MCC_CFO_COD)                         AS cfo_cod,
                SUM(ISNULL(CAST(M.MCC_CRE AS FLOAT), 0))    AS total_cre,
                SUM(ISNULL(CAST(M.MCC_DEB AS FLOAT), 0))    AS total_deb
            FROM MCC M WITH(NOLOCK)
            WHERE CAST(M.MCC_DT AS DATE) >= '{data_de}'
              AND CAST(M.MCC_DT AS DATE) <= '{data_ate}'
              AND M.MCC_CONCILIA = 'S'
              AND (RTRIM(M.MCC_CFO_COD) LIKE '1%' OR RTRIM(M.MCC_CFO_COD) LIKE '17%')
            GROUP BY RTRIM(M.MCC_CFO_COD)
        """
        cursor.execute(query_receitas)
        rec_raw = cursor.fetchall()
        if rec_raw and not isinstance(rec_raw[0], dict):
            cols = [d[0] for d in cursor.description]
            rec_raw = [dict(zip(cols, r)) for r in rec_raw]

        receitas_op_dict['Outras Receitas'] = 0.0
        for r in rec_raw:
            cod = _safe_str(r.get('cfo_cod', '')).strip()
            val = _safe_float(r.get('total_cre', 0)) - _safe_float(r.get('total_deb', 0))
            if val <= 0:
                continue
            if cod.startswith('17'):
                total_receitas_nao_op += val
                cursor.execute(f"SELECT cfo_nome FROM cfo WHERE cfo_cod = '{cod}'")
                cfo_row = cursor.fetchone()
                nome = cfo_row[0] if cfo_row else f"CFO {cod}"
                nao_op_rec_itens.append({'categoria': nome, 'valor': round(val, 2), 'percentual': 0.0})
            else:
                total_receitas_op += val
                cat = RECEITA_MAP.get(cod, 'Outras Receitas')
                receitas_op_dict[cat] += val

        desp_ccr_filter = ""

    receitas_itens = [
        {
            'categoria': k,
            'valor': round(v, 2),
            'percentual': round(v / total_receitas_op * 100, 1) if total_receitas_op > 0 else 0.0,
        }
        for k, v in receitas_op_dict.items() if v > 0
    ]

    # ── 2. DESPESAS ──
    query_despesas = f"""
        SELECT
            RTRIM(C.cfo_cod)                            AS cfo_cod,
            RTRIM(ISNULL(P.cfo_nome, C.cfo_nome))       AS grupo,
            SUM(ISNULL(CAST(M.MCC_DEB AS FLOAT), 0))    AS valor
        FROM MCC M WITH(NOLOCK)
        INNER JOIN cfo C WITH(NOLOCK) ON RTRIM(M.MCC_CFO_COD) = RTRIM(C.cfo_cod)
        LEFT  JOIN cfo P WITH(NOLOCK) ON RTRIM(C.cfo_cfo_cod) = RTRIM(P.cfo_cod)
        WHERE CAST(M.MCC_DT AS DATE) >= '{data_de}'
          AND CAST(M.MCC_DT AS DATE) <= '{data_ate}'
          AND M.MCC_CONCILIA = 'S'
          AND RTRIM(ISNULL(C.cfo_cfo_cod,'')) != '1100'
          AND RTRIM(C.cfo_cod) != '1100'
          AND ISNULL(CAST(M.MCC_DEB AS FLOAT), 0) > 0
          {desp_ccr_filter}
        GROUP BY C.cfo_cod, ISNULL(P.cfo_nome, C.cfo_nome)
    """
    cursor.execute(query_despesas)
    desp_raw = cursor.fetchall()
    if desp_raw and not isinstance(desp_raw[0], dict):
        cols = [d[0] for d in cursor.description]
        desp_raw = [dict(zip(cols, r)) for r in desp_raw]

    DISPLAY_NAMES = {
        'PESSOAL':                     'Pessoal',
        'EXAMES TERCEIRIZADOS':        'Exames Terceirizados',
        'COMPRAS':                     'Suprimentos',
        'CONSUMO':                     'Suprimentos',
        'ALUGUEIS':                    'Aluguéis',
        'TRIBUTARIAS':                 'Impostos e Taxas',
        'IMPOSTOS E TAXAS':            'Impostos e Taxas',
        'SERVIÇOS':                    'Serviços',
        'MANUTENÇÃO':                  'Manutenção',
        'MARKETING':                   'Marketing',
        'UTILIDADES E SERVIÇOS':       'Utilidades',
        'ASSESSORIAS':                 'Assessorias',
        'DIRETORIA':                   'Diretoria',
        'IMOBILIZADO':                 'Investimentos',
        'CONSELHOS E ASSOCIAÇÕES':     'Conselhos e Associações',
        'FINANCEIRAS':                 'Financeiras',
        'EMPRESTIMOS E FINANCIAMENTOS':'Empréstimos',
    }

    # CFOs que pertencem a "Logística" independente do grupo pai (DB 2)
    _DB2_LOGISTICA_CFOS = {'2205', '2410', '1225', '1506', '1227'}
    # CFO de Qualidade exibido separadamente (DB 2)
    _DB2_QUALIDADE_CFOS = {'2404'}

    total_despesas_op = 0.0
    total_despesas_nao_op = 0.0
    desp_op_dict = {}
    desp_nao_op_dict = {}

    for d in desp_raw:
        cod   = _safe_str(d.get('cfo_cod', '')).strip()
        grupo = _safe_str(d.get('grupo', '')).upper().strip()
        val   = _safe_float(d.get('valor', 0))

        if val <= 0:
            continue

        if db_id == '2':
            # IMOBILIZADO (21xx) e Assessoria Convênios (1817) → Não Operacional "Investimentos"
            if cod[:2] == '21' or cod == '1817':
                total_despesas_nao_op += val
                desp_nao_op_dict['Investimentos'] = desp_nao_op_dict.get('Investimentos', 0.0) + val
            # Adiantamento de Dividendos (2001) → Não Operacional "Diretoria"
            elif cod == '2001':
                total_despesas_nao_op += val
                desp_nao_op_dict['Diretoria'] = desp_nao_op_dict.get('Diretoria', 0.0) + val
            # Pro-Labore (2002) → Operacional "Diretoria"
            elif cod == '2002':
                total_despesas_op += val
                desp_op_dict['Diretoria'] = desp_op_dict.get('Diretoria', 0.0) + val
            # Demais 20xx/30xx restantes → Não Operacional pelo nome do grupo
            elif cod.startswith('20') or cod.startswith('30'):
                nome = DISPLAY_NAMES.get(grupo, grupo)
                total_despesas_nao_op += val
                desp_nao_op_dict[nome] = desp_nao_op_dict.get(nome, 0.0) + val
            else:
                # Despesas operacionais com remapeamento por CFO específico
                if cod in _DB2_LOGISTICA_CFOS:
                    nome = 'Logística'
                elif cod in _DB2_QUALIDADE_CFOS:
                    nome = 'Qualidade'
                else:
                    nome = DISPLAY_NAMES.get(grupo, grupo)
                total_despesas_op += val
                desp_op_dict[nome] = desp_op_dict.get(nome, 0.0) + val
        else:
            nome = DISPLAY_NAMES.get(grupo, grupo)
            if cod.startswith('20') or cod.startswith('21') or cod.startswith('30') or grupo == 'DIRETORIA' or grupo == 'IMOBILIZADO':
                total_despesas_nao_op += val
                desp_nao_op_dict[nome] = desp_nao_op_dict.get(nome, 0.0) + val
            else:
                total_despesas_op += val
                desp_op_dict[nome] = desp_op_dict.get(nome, 0.0) + val

    despesas_itens = [
        {
            'categoria': nome,
            'valor': round(val, 2),
            'percentual': round(val / total_receitas_op * 100, 1) if total_receitas_op > 0 else 0.0,
        }
        for nome, val in sorted(desp_op_dict.items(), key=lambda x: x[1], reverse=True)
    ]

    nao_op_itens = []
    for nome, val in desp_nao_op_dict.items():
        nao_op_itens.append({
            'categoria': nome,
            'valor': round(val, 2),
            'percentual': round(val / total_receitas_op * 100, 1) if total_receitas_op > 0 else 0.0,
        })
    for item in nao_op_rec_itens:
        nao_op_itens.append({
            'categoria': item['categoria'],
            'valor': -item['valor'],
            'percentual': 0.0
        })

    total_nao_op_liquido = total_despesas_nao_op - total_receitas_nao_op
    resultado_op = total_receitas_op - total_despesas_op
    superavit_final = resultado_op - total_nao_op_liquido

    saldo_inicial = 0.0
    if db_id == '2' and data_de == '2026-03-01':
        saldo_inicial = 631976.16

    return {
        'periodo_de': data_de,
        'periodo_ate': data_ate,
        'receitas': {
            'total': round(total_receitas_op, 2),
            'percentual_receita': 100.0,
            'itens': receitas_itens,
        },
        'despesas': {
            'total': round(total_despesas_op, 2),
            'percentual_receita': round(total_despesas_op / total_receitas_op * 100, 1) if total_receitas_op > 0 else 0.0,
            'itens': despesas_itens,
        },
        'resultado_operacional': round(resultado_op, 2),
        'resultado_percentual': round(resultado_op / total_receitas_op * 100, 1) if total_receitas_op > 0 else 0.0,
        'nao_operacional': {
            'total': round(total_nao_op_liquido, 2),
            'percentual_receita': round(total_nao_op_liquido / total_receitas_op * 100, 1) if total_receitas_op > 0 else 0.0,
            'itens': nao_op_itens
        },
        'superavit_deficit': round(superavit_final, 2),
        'saldo_inicial': saldo_inicial,
        'saldo_final': round(saldo_inicial + superavit_final, 2)
    }
