import pandas as pd
from typing import Tuple, Dict, List, Optional
from ..models.base import BudgetMetrics, BudgetUnitItem, BudgetUserItem, BudgetSynthetic

def get_budget_data(cursor, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetches budget data (Orçamentos) within the specified date range.
    Filters:
    - ORP_DTHR between start_date and end_date
    - ORP_STATUS in ('A', 'P') where A=Aberto, P=Processado/Convertido
    - STR_STR_COD LIKE '01%' (Active Units)
    - PAC_NOME not like 'teste%'
    """
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    query = f"""
    SELECT 
        ORP.ORP_NUM,
        ORP.ORP_DTHR as data_cadastro,
        ORP.ORP_STATUS,
        ORP.ORP_USR_LOGIN_LANC as usuario,
        STR.STR_NOME as unidade,
        STR.STR_COD as unidade_cod,
        SUM(IOP.IOP_VALOR) as valor_total
    FROM ORP WITH(NOLOCK)
    INNER JOIN IOP WITH(NOLOCK) ON IOP.IOP_ORP_NUM = ORP.ORP_NUM
    INNER JOIN STR WITH(NOLOCK) ON STR.STR_COD = ORP.ORP_STR_SOLIC
    INNER JOIN PAC WITH(NOLOCK) ON PAC.PAC_REG = ORP.ORP_PAC_REG
    WHERE ORP.ORP_DTHR {date_filter}
    AND ORP.ORP_STATUS IN ('A', 'P')
    AND STR.STR_STR_COD LIKE '01%'
    AND PAC.PAC_NOME NOT LIKE 'teste%'
    GROUP BY 
        ORP.ORP_NUM,
        ORP.ORP_DTHR,
        ORP.ORP_STATUS,
        ORP.ORP_USR_LOGIN_LANC,
        STR.STR_NOME,
        STR.STR_COD
    """
    
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    
    # Convert manually to list of dicts to ensure column mapping
    data = []
    for row in results:
        data.append(dict(zip(columns, row)))
        
    return pd.DataFrame(data)

def process_budget_metrics(df: pd.DataFrame) -> BudgetMetrics:
    """
    Processes raw budget data into aggregated metrics
    """
    if df.empty:
        return BudgetMetrics(
            sintetico_geral=BudgetSynthetic(
                quantidade_total=0,
                valor_total=0.0,
                quantidade_convertidos=0,
                valor_convertidos=0.0,
                quantidade_abertos=0,
                valor_abertos=0.0,
                taxa_conversao=0.0
            ),
            por_unidade=[],
            por_usuario=[]
        )

    # Ensure numeric types
    df['valor_total'] = pd.to_numeric(df['valor_total'], errors='coerce').fillna(0.0)

    # 1. Sintético Geral
    total_qtd = len(df)
    total_val = df['valor_total'].sum()
    
    df_conv = df[df['ORP_STATUS'] == 'P']
    conv_qtd = len(df_conv)
    conv_val = df_conv['valor_total'].sum()
    
    df_aberto = df[df['ORP_STATUS'] == 'A']
    aberto_qtd = len(df_aberto)
    aberto_val = df_aberto['valor_total'].sum()
    
    taxa_conv = (conv_qtd / total_qtd * 100) if total_qtd > 0 else 0.0
    
    sintetico = BudgetSynthetic(
        quantidade_total=total_qtd,
        valor_total=round(total_val, 2),
        quantidade_convertidos=conv_qtd,
        valor_convertidos=round(conv_val, 2),
        quantidade_abertos=aberto_qtd,
        valor_abertos=round(aberto_val, 2),
        taxa_conversao=round(taxa_conv, 2)
    )
    
    # 2. Por Unidade
    unidade_metrics = []
    if not df.empty:
        unidade_grouped = df.groupby('unidade')
        for unidade, group in unidade_grouped:
            u_total_qtd = len(group)
            u_total_val = group['valor_total'].sum()
            
            u_conv = group[group['ORP_STATUS'] == 'P']
            u_conv_qtd = len(u_conv)
            u_conv_val = u_conv['valor_total'].sum()
            
            u_aberto = group[group['ORP_STATUS'] == 'A']
            u_aberto_qtd = len(u_aberto)
            u_aberto_val = u_aberto['valor_total'].sum()
            
            u_taxa = (u_conv_qtd / u_total_qtd * 100) if u_total_qtd > 0 else 0.0
            
            unidade_metrics.append(BudgetUnitItem(
                unidade=unidade,
                quantidade_total=u_total_qtd,
                valor_total=round(u_total_val, 2),
                quantidade_convertidos=u_conv_qtd,
                valor_convertidos=round(u_conv_val, 2),
                quantidade_abertos=u_aberto_qtd,
                valor_abertos=round(u_aberto_val, 2),
                taxa_conversao=round(u_taxa, 2)
            ))
            
    # Sort by unit name
    unidade_metrics.sort(key=lambda x: x.unidade)
    
    # 3. Por Usuário
    usuario_metrics = []
    if not df.empty:
        # Fill None users
        df['usuario'] = df['usuario'].fillna('SISTEMA')
        user_grouped = df.groupby(['unidade', 'usuario'])
        
        for (unidade, usuario), group in user_grouped:
            usr_total_qtd = len(group)
            usr_total_val = group['valor_total'].sum()
            
            usr_conv = group[group['ORP_STATUS'] == 'P']
            usr_conv_qtd = len(usr_conv)
            usr_conv_val = usr_conv['valor_total'].sum()
            
            usr_aberto = group[group['ORP_STATUS'] == 'A']
            usr_aberto_qtd = len(usr_aberto)
            usr_aberto_val = usr_aberto['valor_total'].sum()
            
            usr_taxa = (usr_conv_qtd / usr_total_qtd * 100) if usr_total_qtd > 0 else 0.0
            
            usuario_metrics.append(BudgetUserItem(
                unidade=unidade,
                usuario=usuario,
                quantidade_total=usr_total_qtd,
                valor_total=round(usr_total_val, 2),
                quantidade_convertidos=usr_conv_qtd,
                valor_convertidos=round(usr_conv_val, 2),
                quantidade_abertos=usr_aberto_qtd,
                valor_abertos=round(usr_aberto_val, 2),
                taxa_conversao=round(usr_taxa, 2)
            ))
            
    # Sort by unit then user
    usuario_metrics.sort(key=lambda x: (x.unidade, x.usuario))
    
    return BudgetMetrics(
        sintetico_geral=sintetico,
        por_unidade=unidade_metrics,
        por_usuario=usuario_metrics
    )
