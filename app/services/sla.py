import pandas as pd
import numpy as np
from typing import Tuple, Dict, List


def get_sla_data(cursor, start_date: str, end_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retorna dados de SLA já pré-agregados pelo SQL.

    Otimização principal: a query anterior retornava uma linha por exame individual
    (potencialmente dezenas de milhares de linhas) e delegava 4 groupby ao Python.
    Agora o SQL agrega diretamente por (unidade_tecnica, unidade_recepcao, aparelho,
    liberacao_auto) com SUM(CASE WHEN ...) para cada faixa de atraso.
    O resultado tem ~30-60 linhas independente do volume do período.
    """
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"

    query_sla = f"""
    SELECT
        STR.STR_NOME                         AS unidade_tecnica,
        STR_RECEP.STR_NOME                   AS unidade_recepcao,
        COALESCE(RCL.rcl_aparelho, 'MANUAL') AS aparelho,
        RCL.rcl_ind_lib_Auto                 AS liberacao_auto,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) <= 0
                 THEN SMM.SMM_QT ELSE 0 END) AS no_prazo,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) > 0
                 THEN SMM.SMM_QT ELSE 0 END) AS atrasado,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) BETWEEN 1 AND 59
                 THEN SMM.SMM_QT ELSE 0 END) AS faixa_lt1h,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) BETWEEN 60 AND 120
                 THEN SMM.SMM_QT ELSE 0 END) AS faixa_1_2h,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) BETWEEN 121 AND 300
                 THEN SMM.SMM_QT ELSE 0 END) AS faixa_3_5h,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) BETWEEN 301 AND 600
                 THEN SMM.SMM_QT ELSE 0 END) AS faixa_6_10h,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) BETWEEN 601 AND 1440
                 THEN SMM.SMM_QT ELSE 0 END) AS faixa_11_24h,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) > 1440
                 THEN SMM.SMM_QT ELSE 0 END) AS faixa_gt24h
    FROM RCL WITH(NOLOCK)
    INNER JOIN SMM WITH(NOLOCK) ON RCL.RCL_SMM = SMM.SMM_NUM
        AND RCL.RCL_OSM = SMM.SMM_OSM
        AND RCL.RCL_OSM_SERIE = SMM.SMM_OSM_SERIE
    INNER JOIN SMK WITH(NOLOCK) ON SMK.smk_Cod = SMM.smm_cod AND SMK.smk_tipo = SMM.smm_tpcod
    INNER JOIN OSM WITH(NOLOCK) ON OSM.OSM_SERIE = SMM.SMM_OSM_SERIE
        AND OSM.OSM_NUM = SMM.SMM_OSM
    INNER JOIN STR WITH(NOLOCK) ON STR.STR_COD = SMM.SMM_STR
    INNER JOIN STR STR_RECEP WITH(NOLOCK) ON STR_RECEP.STR_COD = OSM.OSM_STR
    WHERE RCL.RCL_DTHR_LIB {date_filter}
      AND RCL.rcl_stat IN ('I', 'E', 'L')
      AND STR_RECEP.STR_STR_COD LIKE '01%'
    GROUP BY
        STR.STR_NOME,
        STR_RECEP.STR_NOME,
        COALESCE(RCL.rcl_aparelho, 'MANUAL'),
        RCL.rcl_ind_lib_Auto
    """

    cursor.execute(query_sla)
    df_sla = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "unidade_tecnica", "unidade_recepcao", "aparelho", "liberacao_auto",
            "no_prazo", "atrasado",
            "faixa_lt1h", "faixa_1_2h", "faixa_3_5h",
            "faixa_6_10h", "faixa_11_24h", "faixa_gt24h",
        ],
    )

    query_amostras = f"""
    SELECT
        B.STR_NOME                             AS unidade_tecnica,
        SUM(SMM.SMM_QT)                        AS quantidade,
        SUM(CASE WHEN RPE.RPE_IND_NOVA_AMOSTRA = 'S'
                 THEN SMM.SMM_QT ELSE 0 END)   AS novas_amostras
    FROM RCL WITH(NOLOCK)
    INNER JOIN SMM WITH(NOLOCK) ON RCL.RCL_OSM_SERIE = SMM.SMM_OSM_SERIE
        AND RCL.RCL_OSM = SMM.SMM_OSM
        AND RCL.RCL_SMM = SMM.SMM_NUM
    INNER JOIN OSM WITH(NOLOCK) ON RCL.RCL_OSM_SERIE = OSM.OSM_SERIE
        AND RCL.RCL_OSM = OSM.OSM_NUM
    INNER JOIN STR B WITH(NOLOCK) ON SMM.SMM_STR = B.STR_COD
    INNER JOIN RPE WITH(NOLOCK) ON RPE.RPE_SMM_NUM = RCL.RCL_SMM
        AND RPE.RPE_OSM_NUM = RCL.RCL_OSM
        AND RPE.RPE_OSM_SERIE = RCL.RCL_OSM_SERIE
    WHERE RCL.RCL_DTHR_LIB {date_filter}
    GROUP BY B.STR_NOME
    """

    cursor.execute(query_amostras)
    df_amostras = pd.DataFrame(
        cursor.fetchall(),
        columns=["unidade_tecnica", "quantidade", "novas_amostras"],
    )

    return df_sla, df_amostras


def _faixas_from_row(row) -> Dict:
    return {
        "menos_1h":    int(row["faixa_lt1h"]),
        "entre_1_2h":  int(row["faixa_1_2h"]),
        "entre_3_5h":  int(row["faixa_3_5h"]),
        "entre_6_10h": int(row["faixa_6_10h"]),
        "entre_11_24h":int(row["faixa_11_24h"]),
        "mais_24h":    int(row["faixa_gt24h"]),
    }


def _faixas_add(a: Dict, b: Dict) -> Dict:
    return {k: a[k] + b[k] for k in a}


def _empty_faixas() -> Dict:
    return {"menos_1h": 0, "entre_1_2h": 0, "entre_3_5h": 0,
            "entre_6_10h": 0, "entre_11_24h": 0, "mais_24h": 0}


def _pct(no_prazo: int, total: int) -> float:
    return round(no_prazo / total * 100, 2) if total > 0 else 0.0


def process_sla_operational(df_sla: pd.DataFrame, df_amostras: pd.DataFrame) -> Dict:
    """
    Processa dados já pré-agregados pelo SQL (dezenas de linhas, não milhares).

    As 4 views são derivadas acumulando sobre as linhas pré-agrupadas:
      - geral:            (unidade_recepcao, aparelho, liberacao_auto)
      - por_unidade:      (unidade_tecnica, unidade_recepcao, aparelho, liberacao_auto)
      - por_bancada:      (unidade_tecnica, aparelho)
      - resumo_por_unidade: (unidade_recepcao)
    """
    if df_sla.empty:
        return {"geral": [], "por_unidade": [], "por_bancada": [], "amostras": []}

    # Normaliza tipos numéricos
    int_cols = ["no_prazo", "atrasado",
                "faixa_lt1h", "faixa_1_2h", "faixa_3_5h",
                "faixa_6_10h", "faixa_11_24h", "faixa_gt24h"]
    df_sla[int_cols] = df_sla[int_cols].fillna(0).astype(int)

    geral_dict:   Dict = {}
    unid_dict:    Dict = {}
    bancada_dict: Dict = {}
    resumo_dict:  Dict = {}

    for row in df_sla.itertuples(index=False):
        ut      = row.unidade_tecnica
        ur      = row.unidade_recepcao
        ap      = row.aparelho
        la      = row.liberacao_auto
        np_     = row.no_prazo
        at      = row.atrasado
        qtd     = np_ + at
        faixas  = _faixas_from_row(row._asdict())

        # --- geral ---
        k = (ur, ap, la)
        if k not in geral_dict:
            geral_dict[k] = {
                "unidade": ur, "bancada": ut,
                "aparelho": ap, "liberacao_auto": la,
                "quantidade": 0, "no_prazo": 0, "atrasado": 0,
                "faixas_atraso": _empty_faixas(),
            }
        g = geral_dict[k]
        g["quantidade"] += qtd
        g["no_prazo"]   += np_
        g["atrasado"]   += at
        g["faixas_atraso"] = _faixas_add(g["faixas_atraso"], faixas)

        # --- por_unidade ---
        k2 = (ut, ur, ap, la)
        if k2 not in unid_dict:
            unid_dict[k2] = {
                "unidade": ut, "unidade_recepcao": ur,
                "aparelho": ap, "liberacao_auto": la,
                "quantidade": 0, "no_prazo": 0, "atrasado": 0,
                "faixas_atraso": _empty_faixas(),
            }
        u = unid_dict[k2]
        u["quantidade"] += qtd
        u["no_prazo"]   += np_
        u["atrasado"]   += at
        u["faixas_atraso"] = _faixas_add(u["faixas_atraso"], faixas)

        # --- bancada ---
        k3 = (ut, ap)
        if k3 not in bancada_dict:
            bancada_dict[k3] = {
                "unidade": ut, "aparelho": ap, "liberacao_auto": None,
                "quantidade": 0, "no_prazo": 0, "atrasado": 0,
                "faixas_atraso": _empty_faixas(),
            }
        b = bancada_dict[k3]
        b["quantidade"] += qtd
        b["no_prazo"]   += np_
        b["atrasado"]   += at
        b["faixas_atraso"] = _faixas_add(b["faixas_atraso"], faixas)

        # --- resumo ---
        if ur not in resumo_dict:
            resumo_dict[ur] = {
                "unidade": ur,
                "quantidade": 0, "no_prazo": 0, "atrasado": 0,
                "faixas_atraso": _empty_faixas(),
            }
        r = resumo_dict[ur]
        r["quantidade"] += qtd
        r["no_prazo"]   += np_
        r["atrasado"]   += at
        r["faixas_atraso"] = _faixas_add(r["faixas_atraso"], faixas)

    # Calcula percentual_no_prazo em todos os dicts
    for d in (*geral_dict.values(), *unid_dict.values(),
              *bancada_dict.values(), *resumo_dict.values()):
        d["percentual_no_prazo"] = _pct(d["no_prazo"], d["quantidade"])

    # Amostras — já pré-agregadas pelo SQL
    amostras_results = []
    for row in df_amostras.itertuples(index=False):
        total = int(row.quantidade)
        novas = int(row.novas_amostras)
        amostras_results.append({
            "unidade": row.unidade_tecnica,
            "total_exames": total,
            "novas_amostras": novas,
            "percentual_retrabalho": round(novas / total * 100, 2) if total > 0 else 0.0,
        })

    return {
        "geral":               list(geral_dict.values()),
        "por_unidade":         list(unid_dict.values()),
        "por_bancada":         list(bancada_dict.values()),
        "resumo_por_unidade":  sorted(resumo_dict.values(), key=lambda x: x["unidade"]),
        "amostras":            amostras_results,
    }
