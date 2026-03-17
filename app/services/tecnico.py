import pandas as pd


_EMPTY_COLS = ["dia", "quantidade", "valor", "no_prazo", "atrasado"]


def get_laudos_comparativo_data(cursor, ano_atual: int, mes: int):
    """
    Busca laudos liberados dia a dia para dois períodos:
      - Mês informado do ano_atual
      - Mesmo mês do ano anterior (ano_atual - 1)

    Retorna dois DataFrames: (df_atual, df_anterior)
    Colunas: dia, quantidade, valor, no_prazo, atrasado

    Prazo: DATEDIFF(minute, SMM_DT_RESULT, RCL_DTHR_LIB) <= 0 → no prazo
    (mesma lógica do /operacional/sla)
    """
    ano_anterior = ano_atual - 1

    query = f"""
    SELECT
        YEAR(RCL.RCL_DTHR_LIB)  AS ano,
        DAY(RCL.RCL_DTHR_LIB)   AS dia,
        SUM(SMM.SMM_QT)          AS quantidade,
        SUM(SMM.smm_vlr)         AS valor,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) <= 0
                 THEN SMM.SMM_QT ELSE 0 END) AS no_prazo,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) > 0
                 THEN SMM.SMM_QT ELSE 0 END) AS atrasado
    FROM RCL WITH(NOLOCK)
    INNER JOIN SMM WITH(NOLOCK)
        ON RCL.RCL_SMM        = SMM.SMM_NUM
        AND RCL.RCL_OSM       = SMM.SMM_OSM
        AND RCL.RCL_OSM_SERIE = SMM.SMM_OSM_SERIE
    WHERE MONTH(RCL.RCL_DTHR_LIB) = {mes}
      AND YEAR(RCL.RCL_DTHR_LIB)  IN ({ano_atual}, {ano_anterior})
      AND RCL.rcl_stat IN ('I', 'E', 'L')
    GROUP BY YEAR(RCL.RCL_DTHR_LIB), DAY(RCL.RCL_DTHR_LIB)
    ORDER BY YEAR(RCL.RCL_DTHR_LIB), DAY(RCL.RCL_DTHR_LIB)
    """
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=["ano", "dia", "quantidade", "valor", "no_prazo", "atrasado"])

    if df.empty:
        empty = pd.DataFrame(columns=_EMPTY_COLS)
        return empty, empty

    df["quantidade"] = df["quantidade"].fillna(0).astype(int)
    df["valor"]      = df["valor"].fillna(0.0).astype(float).round(2)
    df["no_prazo"]   = df["no_prazo"].fillna(0).astype(int)
    df["atrasado"]   = df["atrasado"].fillna(0).astype(int)

    df_atual    = df[df["ano"] == ano_atual   ][_EMPTY_COLS].reset_index(drop=True)
    df_anterior = df[df["ano"] == ano_anterior][_EMPTY_COLS].reset_index(drop=True)

    return df_atual, df_anterior


def _totais(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"quantidade": 0, "valor": 0.0, "no_prazo": 0, "atrasado": 0}
    return {
        "quantidade": int(df["quantidade"].sum()),
        "valor":      round(float(df["valor"].sum()), 2),
        "no_prazo":   int(df["no_prazo"].sum()),
        "atrasado":   int(df["atrasado"].sum()),
    }


def build_laudos_comparativo(df_atual: pd.DataFrame, df_anterior: pd.DataFrame) -> dict:
    """
    Monta a resposta final alinhando os dias dos dois períodos.
    """
    atual_map    = df_atual.set_index("dia").to_dict(orient="index")    if not df_atual.empty    else {}
    anterior_map = df_anterior.set_index("dia").to_dict(orient="index") if not df_anterior.empty else {}

    _zero = {"quantidade": 0, "valor": 0.0, "no_prazo": 0, "atrasado": 0}

    todos_dias = sorted(set(atual_map) | set(anterior_map))
    dias = []
    for d in todos_dias:
        a = atual_map.get(d,    _zero)
        p = anterior_map.get(d, _zero)
        dias.append({
            "dia":      d,
            "atual":    {"quantidade": a["quantidade"], "valor": a["valor"],
                         "no_prazo": a["no_prazo"], "atrasado": a["atrasado"]},
            "anterior": {"quantidade": p["quantidade"], "valor": p["valor"],
                         "no_prazo": p["no_prazo"], "atrasado": p["atrasado"]},
        })

    return {
        "dias":             dias,
        "totais_atual":     _totais(df_atual),
        "totais_anterior":  _totais(df_anterior),
    }
