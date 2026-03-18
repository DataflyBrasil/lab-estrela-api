import pandas as pd
from datetime import datetime, timedelta


_EMPTY_COLS = ["data", "quantidade", "valor", "no_prazo", "atrasado"]


def get_laudos_comparativo_data(cursor, start_date: str, end_date: str):
    """
    Busca laudos liberados dia a dia para dois períodos:
      - Período informado (start_date a end_date)
      - Mesmo período do ano anterior

    Retorna dois DataFrames: (df_atual, df_anterior)
    Colunas: data (YYYY-MM-DD), quantidade, valor, no_prazo, atrasado
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt   = datetime.strptime(end_date,   '%Y-%m-%d')

    try:
        start_anterior = start_dt.replace(year=start_dt.year - 1)
        end_anterior   = end_dt.replace(year=end_dt.year - 1)
    except ValueError:
        # Fallback para 29/02 em anos não bissextos
        start_anterior = start_dt - timedelta(days=365)
        end_anterior   = end_dt   - timedelta(days=365)

    query = f"""
    SELECT
        CONVERT(varchar(10), RCL.RCL_DTHR_LIB, 120) AS data,
        SUM(SMM.SMM_QT)   AS quantidade,
        SUM(SMM.smm_vlr)  AS valor,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) <= 0
                 THEN SMM.SMM_QT ELSE 0 END) AS no_prazo,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) > 0
                 THEN SMM.SMM_QT ELSE 0 END) AS atrasado,
        'atual' AS periodo
    FROM RCL WITH(NOLOCK)
    INNER JOIN SMM WITH(NOLOCK)
        ON RCL.RCL_SMM        = SMM.SMM_NUM
       AND RCL.RCL_OSM        = SMM.SMM_OSM
       AND RCL.RCL_OSM_SERIE  = SMM.SMM_OSM_SERIE
    INNER JOIN SMK WITH(NOLOCK) ON SMK.smk_Cod = SMM.smm_cod AND SMK.smk_tipo = SMM.smm_tpcod
    INNER JOIN OSM WITH(NOLOCK) ON OSM.OSM_SERIE = SMM.SMM_OSM_SERIE AND OSM.OSM_NUM = SMM.SMM_OSM
    INNER JOIN STR STR_RECEP WITH(NOLOCK) ON STR_RECEP.STR_COD = OSM.OSM_STR
    WHERE RCL.RCL_DTHR_LIB BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
      AND RCL.rcl_stat IN ('I', 'E', 'L')
      AND STR_RECEP.STR_STR_COD LIKE '01%'
      AND SMM.SMM_DT_RESULT IS NOT NULL
    GROUP BY CONVERT(varchar(10), RCL.RCL_DTHR_LIB, 120)

    UNION ALL

    SELECT
        CONVERT(varchar(10), RCL.RCL_DTHR_LIB, 120) AS data,
        SUM(SMM.SMM_QT)   AS quantidade,
        SUM(SMM.smm_vlr)  AS valor,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) <= 0
                 THEN SMM.SMM_QT ELSE 0 END) AS no_prazo,
        SUM(CASE WHEN DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) > 0
                 THEN SMM.SMM_QT ELSE 0 END) AS atrasado,
        'anterior' AS periodo
    FROM RCL WITH(NOLOCK)
    INNER JOIN SMM WITH(NOLOCK)
        ON RCL.RCL_SMM        = SMM.SMM_NUM
       AND RCL.RCL_OSM        = SMM.SMM_OSM
       AND RCL.RCL_OSM_SERIE  = SMM.SMM_OSM_SERIE
    INNER JOIN SMK WITH(NOLOCK) ON SMK.smk_Cod = SMM.smm_cod AND SMK.smk_tipo = SMM.smm_tpcod
    INNER JOIN OSM WITH(NOLOCK) ON OSM.OSM_SERIE = SMM.SMM_OSM_SERIE AND OSM.OSM_NUM = SMM.SMM_OSM
    INNER JOIN STR STR_RECEP WITH(NOLOCK) ON STR_RECEP.STR_COD = OSM.OSM_STR
    WHERE RCL.RCL_DTHR_LIB BETWEEN '{start_anterior.strftime('%Y-%m-%d')} 00:00:00' AND '{end_anterior.strftime('%Y-%m-%d')} 23:59:59'
      AND RCL.rcl_stat IN ('I', 'E', 'L')
      AND STR_RECEP.STR_STR_COD LIKE '01%'
      AND SMM.SMM_DT_RESULT IS NOT NULL
    GROUP BY CONVERT(varchar(10), RCL.RCL_DTHR_LIB, 120)
    """
    cursor.execute(query)
    df = pd.DataFrame(
        cursor.fetchall(),
        columns=["data", "quantidade", "valor", "no_prazo", "atrasado", "periodo"]
    )

    if df.empty:
        empty = pd.DataFrame(columns=_EMPTY_COLS)
        return empty, empty

    df["quantidade"] = df["quantidade"].fillna(0).astype(int)
    df["valor"]      = df["valor"].fillna(0.0).astype(float).round(2)
    df["no_prazo"]   = df["no_prazo"].fillna(0).astype(int)
    df["atrasado"]   = df["atrasado"].fillna(0).astype(int)

    df_atual    = df[df["periodo"] == "atual"   ][_EMPTY_COLS].reset_index(drop=True)
    df_anterior = df[df["periodo"] == "anterior"][_EMPTY_COLS].reset_index(drop=True)

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
    Anterior é mapeado deslocando +1 ano para coincidir com as datas do atual.
    """
    atual_map = (
        df_atual.set_index("data").to_dict(orient="index")
        if not df_atual.empty else {}
    )

    # Desloca datas do anterior +1 ano para alinhar com o período atual
    anterior_map = {}
    if not df_anterior.empty:
        for _, row in df_anterior.iterrows():
            d = datetime.strptime(row["data"], '%Y-%m-%d')
            try:
                shifted = d.replace(year=d.year + 1).strftime('%Y-%m-%d')
            except ValueError:
                shifted = (d + timedelta(days=365)).strftime('%Y-%m-%d')
            anterior_map[shifted] = row.to_dict()

    _zero = {"quantidade": 0, "valor": 0.0, "no_prazo": 0, "atrasado": 0}

    def _as_metrics(row):
        return {
            "quantidade": int(row.get("quantidade", 0) or 0),
            "valor":      float(row.get("valor", 0.0) or 0.0),
            "no_prazo":   int(row.get("no_prazo", 0) or 0),
            "atrasado":   int(row.get("atrasado", 0) or 0),
        }

    todas_datas = sorted(set(atual_map) | set(anterior_map))
    dias = []
    for data in todas_datas:
        a = atual_map.get(data, _zero)
        p = anterior_map.get(data, _zero)
        dias.append({
            "data":     data,
            "atual":    _as_metrics(a),
            "anterior": _as_metrics(p),
        })

    return {
        "dias":            dias,
        "totais_atual":    _totais(df_atual),
        "totais_anterior": _totais(df_anterior),
    }
