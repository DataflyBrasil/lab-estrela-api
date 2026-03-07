import pandas as pd
import numpy as np
from typing import Tuple, Dict, List

def get_sla_data(cursor, start_date: str, end_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches consolidated operational SLA data with a single optimized query.
    Returns raw data for exam releases and sample retests.
    """
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # Consolidated query for all SLA metrics
    query_sla = f"""
    SELECT 
        STR.STR_NOME as unidade_tecnica,
        STR.STR_COD as unidade_tecnica_cod,
        STR_RECEP.STR_NOME as unidade_recepcao,
        STR_RECEP.STR_COD as unidade_recepcao_cod,
        STR_RECEP.STR_STR_COD as str_cod_recepcao,
        COALESCE(RCL.rcl_aparelho, 'MANUAL') as aparelho,
        RCL.rcl_ind_lib_Auto as liberacao_auto,
        SMM.SMM_QT as quantidade,
        DATEDIFF(minute, SMM.SMM_DT_RESULT, RCL.RCL_DTHR_LIB) as minutos_atraso,
        RCL.rcl_stat as status_rcl
    FROM RCL WITH(NOLOCK)
    INNER JOIN SMM WITH(NOLOCK) ON RCL.RCL_SMM = SMM.SMM_NUM 
        AND RCL.RCL_OSM = SMM.SMM_OSM 
        AND RCL.RCL_OSM_SERIE = SMM.SMM_OSM_SERIE
    INNER JOIN SMK WITH(NOLOCK) ON SMK.smk_Cod = SMM.smm_cod
    INNER JOIN OSM WITH(NOLOCK) ON OSM.OSM_SERIE = SMM.SMM_OSM_SERIE 
        AND OSM.OSM_NUM = SMM.SMM_OSM
    INNER JOIN STR WITH(NOLOCK) ON STR.STR_COD = SMM.SMM_STR
    INNER JOIN STR STR_RECEP WITH(NOLOCK) ON STR_RECEP.STR_COD = OSM.OSM_STR
    INNER JOIN CTF WITH(NOLOCK) ON CTF.ctf_cod = SMK.smk_ctf
    WHERE RCL.RCL_DTHR_LIB {date_filter}
    AND RCL.rcl_stat IN ('I', 'E', 'L')
    AND STR_RECEP.STR_STR_COD LIKE '01%'
    """
    
    cursor.execute(query_sla)
    df_sla = pd.DataFrame(cursor.fetchall())
    
    # Query for sample retests (amostras)
    query_amostras = f"""
    SELECT 
        B.STR_NOME as unidade_tecnica,
        SMM.SMM_QT as quantidade,
        RPE.RPE_IND_NOVA_AMOSTRA as nova_amostra
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
    """
    
    cursor.execute(query_amostras)
    df_amostras = pd.DataFrame(cursor.fetchall())
    
    return df_sla, df_amostras

def classify_delay_bucket(minutos: float) -> str:
    """Classifies delay into time buckets"""
    if pd.isna(minutos) or minutos <= 0:
        return None
    if minutos <= 59:
        return '< 1h'
    horas = minutos / 60
    if horas <= 2:
        return '1-2h'
    if horas <= 5:
        return '3-5h'
    if horas <= 10:
        return '6-10h'
    if horas <= 24:
        return '11-24h'
    return '>24h'

def aggregate_sla_faixas(df_group: pd.DataFrame) -> Dict:
    """Aggregates delay buckets from a grouped DataFrame"""
    faixas = df_group['faixa'].value_counts().to_dict() if not df_group.empty else {}
    return {
        'menos_1h': faixas.get('< 1h', 0),
        'entre_1_2h': faixas.get('1-2h', 0),
        'entre_3_5h': faixas.get('3-5h', 0),
        'entre_6_10h': faixas.get('6-10h', 0),
        'entre_11_24h': faixas.get('11-24h', 0),
        'mais_24h': faixas.get('>24h', 0)
    }

def process_sla_operational(df_sla: pd.DataFrame, df_amostras: pd.DataFrame) -> Dict:
    """
    Processes raw SLA data into aggregated metrics.
    Uses pandas for efficient multi-dimensional grouping.
    """
    
    if df_sla.empty:
        return {
            'geral': [],
            'por_unidade': [],
            'por_bancada': [],
            'amostras': []
        }
    
    # Classify delay status and buckets
    df_sla['status_atraso'] = df_sla['minutos_atraso'].apply(
        lambda x: 'NO PRAZO' if x <= 0 else 'ATRASADO'
    )
    df_sla['faixa'] = df_sla['minutos_atraso'].apply(classify_delay_bucket)
    
    # 1. Geral (by reception unit, equipment, and auto-release)
    geral_grouped = df_sla.groupby(['unidade_recepcao', 'aparelho', 'liberacao_auto', 'status_atraso'])
    geral_results = []
    
    for (unidade, aparelho, lib_auto, status), group in geral_grouped:
        base_key = (unidade, aparelho, lib_auto)
        existing = next((item for item in geral_results if 
                        (item['unidade'], item['aparelho'], item['liberacao_auto']) == base_key), None)
        
        if existing is None:
            # Get the most common technical unit (bancada) for this group
            bancada_mais_comum = group['unidade_tecnica'].mode()[0] if not group.empty else None
            
            existing = {
                'unidade': unidade,
                'unidade_recepcao': None,
                'bancada': bancada_mais_comum,
                'aparelho': aparelho,
                'liberacao_auto': lib_auto,
                'quantidade': 0,
                'no_prazo': 0,
                'atrasado': 0,
                'faixas_atraso': {'menos_1h': 0, 'entre_1_2h': 0, 'entre_3_5h': 0, 
                                 'entre_6_10h': 0, 'entre_11_24h': 0, 'mais_24h': 0}
            }
            geral_results.append(existing)
        
        qtd = int(group['quantidade'].sum())
        existing['quantidade'] += qtd
        
        if status == 'NO PRAZO':
            existing['no_prazo'] += qtd
        else:
            existing['atrasado'] += qtd
            # Add delay buckets
            faixas = aggregate_sla_faixas(group)
            for key in faixas:
                existing['faixas_atraso'][key] += faixas[key]
    
    # Calculate percentages
    for item in geral_results:
        total = item['quantidade']
        item['percentual_no_prazo'] = round((item['no_prazo'] / total * 100) if total > 0 else 0.0, 2)
    
    # 2. Por Unidade (by reception unit)
    unidade_results = []
    unidade_grouped = df_sla.groupby(['unidade_recepcao', 'unidade_tecnica', 'aparelho', 'liberacao_auto', 'status_atraso'])
    
    for (recep, tecnica, aparelho, lib_auto, status), group in unidade_grouped:
        base_key = (recep, tecnica, aparelho, lib_auto)
        existing = next((item for item in unidade_results if 
                        (item['unidade'], item.get('unidade_recepcao'), item['aparelho'], item['liberacao_auto']) == 
                        (tecnica, recep, aparelho, lib_auto)), None)
        
        if existing is None:
            existing = {
                'unidade': tecnica,
                'unidade_recepcao': recep,
                'aparelho': aparelho,
                'liberacao_auto': lib_auto,
                'quantidade': 0,
                'no_prazo': 0,
                'atrasado': 0,
                'faixas_atraso': {'menos_1h': 0, 'entre_1_2h': 0, 'entre_3_5h': 0, 
                                 'entre_6_10h': 0, 'entre_11_24h': 0, 'mais_24h': 0}
            }
            unidade_results.append(existing)
        
        qtd = int(group['quantidade'].sum())
        existing['quantidade'] += qtd
        
        if status == 'NO PRAZO':
            existing['no_prazo'] += qtd
        else:
            existing['atrasado'] += qtd
            faixas = aggregate_sla_faixas(group)
            for key in faixas:
                existing['faixas_atraso'][key] += faixas[key]
    
    for item in unidade_results:
        total = item['quantidade']
        item['percentual_no_prazo'] = round((item['no_prazo'] / total * 100) if total > 0 else 0.0, 2)
    
    # 3. Por Bancada (by technical unit and equipment only)
    bancada_results = []
    bancada_grouped = df_sla.groupby(['unidade_tecnica', 'aparelho', 'status_atraso'])
    
    for (unidade, aparelho, status), group in bancada_grouped:
        base_key = (unidade, aparelho)
        existing = next((item for item in bancada_results if 
                        (item['unidade'], item['aparelho']) == base_key), None)
        
        if existing is None:
            existing = {
                'unidade': unidade,
                'aparelho': aparelho,
                'liberacao_auto': None,
                'quantidade': 0,
                'no_prazo': 0,
                'atrasado': 0,
                'faixas_atraso': {'menos_1h': 0, 'entre_1_2h': 0, 'entre_3_5h': 0, 
                                 'entre_6_10h': 0, 'entre_11_24h': 0, 'mais_24h': 0}
            }
            bancada_results.append(existing)
        
        qtd = int(group['quantidade'].sum())
        existing['quantidade'] += qtd
        
        if status == 'NO PRAZO':
            existing['no_prazo'] += qtd
        else:
            existing['atrasado'] += qtd
            faixas = aggregate_sla_faixas(group)
            for key in faixas:
                existing['faixas_atraso'][key] += faixas[key]
    
    for item in bancada_results:
        total = item['quantidade']
        item['percentual_no_prazo'] = round((item['no_prazo'] / total * 100) if total > 0 else 0.0, 2)
    
    # 4. Resumo Consolidado por Unidade (SIMPLIFICADO - apenas unidade de recepção)
    resumo_unidade_results = []
    resumo_grouped = df_sla.groupby(['unidade_recepcao', 'status_atraso'])
    
    for (unidade, status), group in resumo_grouped:
        existing = next((item for item in resumo_unidade_results if item['unidade'] == unidade), None)
        
        if existing is None:
            existing = {
                'unidade': unidade,
                'quantidade': 0,
                'no_prazo': 0,
                'atrasado': 0,
                'faixas_atraso': {'menos_1h': 0, 'entre_1_2h': 0, 'entre_3_5h': 0, 
                                 'entre_6_10h': 0, 'entre_11_24h': 0, 'mais_24h': 0}
            }
            resumo_unidade_results.append(existing)
        
        qtd = int(group['quantidade'].sum())
        existing['quantidade'] += qtd
        
        if status == 'NO PRAZO':
            existing['no_prazo'] += qtd
        else:
            existing['atrasado'] += qtd
            faixas = aggregate_sla_faixas(group)
            for key in faixas:
                existing['faixas_atraso'][key] += faixas[key]
    
    for item in resumo_unidade_results:
        total = item['quantidade']
        item['percentual_no_prazo'] = round((item['no_prazo'] / total * 100) if total > 0 else 0.0, 2)
    
    # Sort by unit name
    resumo_unidade_results = sorted(resumo_unidade_results, key=lambda x: x['unidade'])
    
    # 5. Amostras (sample retests)
    amostras_results = []
    if not df_amostras.empty:
        amostras_grouped = df_amostras.groupby('unidade_tecnica')
        for unidade, group in amostras_grouped:
            total_exames = int(group['quantidade'].sum())
            novas_amostras = int(group[group['nova_amostra'] == 'S']['quantidade'].sum())
            
            amostras_results.append({
                'unidade': unidade,
                'total_exames': total_exames,
                'novas_amostras': novas_amostras,
                'percentual_retrabalho': round((novas_amostras / total_exames * 100) if total_exames > 0 else 0.0, 2)
            })
    
    return {
        'geral': geral_results,
        'por_unidade': unidade_results,
        'por_bancada': bancada_results,
        'resumo_por_unidade': resumo_unidade_results,
        'amostras': amostras_results
    }
