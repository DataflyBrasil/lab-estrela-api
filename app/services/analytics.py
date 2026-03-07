import pandas as pd
import numpy as np

def get_unit_revenue_data(cursor, start_date, end_date):
    """Busca dados brutos filtrados por data para processamento em Python."""
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # 1. Buscar OSM e Unidades
    query_osm = f"""
    SELECT 
        o.osm_num, o.osm_serie, o.osm_cnv, s.str_nome as unidade
    FROM osm o
    INNER JOIN str s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    """
    cursor.execute(query_osm)
    df_osm = pd.DataFrame(cursor.fetchall())
    
    if df_osm.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # 2. Buscar MTE (Particular) - Filtrando por data via JOIN com OSM para performance
    query_mte = f"""
    SELECT 
        m.mte_osm, m.mte_osm_serie, 
        (ISNULL(m.mte_valor, 0)) as valor_liquido -- USANDO BRUTO PARA ALINHAR COM ESTRATEGICO
    FROM mte m
    INNER JOIN osm o ON m.mte_osm = o.osm_num AND m.mte_osm_serie = o.osm_serie
    WHERE o.osm_dthr {date_filter}
    """
    cursor.execute(query_mte)
    df_mte = pd.DataFrame(cursor.fetchall())
    
    # 3. Buscar IPC (Convênio) - Filtrando por data via JOIN com OSM
    query_ipc = f"""
    SELECT 
        i.IPC_OSM_NUM, i.IPC_OSM_SERIE, ISNULL(i.IPC_VALOR, 0) as valor
    FROM IPC i
    INNER JOIN osm o ON i.IPC_OSM_NUM = o.osm_num AND i.IPC_OSM_SERIE = o.osm_serie
    WHERE o.osm_dthr {date_filter}
    AND (i.IPC_STATUS IS NULL OR i.IPC_STATUS <> 'C')
    """
    cursor.execute(query_ipc)
    df_ipc = pd.DataFrame(cursor.fetchall())

    # 4. Buscar SMM (Itens) para Rateio de MNS
    # Apenas itens de Convênio (CNV <> 1 ou nome <> PARTICULAR)
    # Assumindo query baseada em OSM para filtrar datas
    query_smm = f"""
    SELECT 
        o.osm_num, s.str_nome as unidade, s.str_cod, sm.smm_vlr
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN STR s ON o.osm_str = s.str_cod
    LEFT JOIN CNV c ON o.osm_cnv = c.cnv_cod
    WHERE o.osm_dthr {date_filter}
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    AND sm.smm_motivo_cancela IS NULL
    AND (c.cnv_nome NOT LIKE '%PARTIC%' AND c.cnv_nome NOT LIKE '%SUI%')
    """
    cursor.execute(query_smm)
    df_smm_rateio = pd.DataFrame(cursor.fetchall())

    # 5. Buscar Total MNS (Global) para o período
    query_mns_total = f"""
    SELECT SUM(mns_vlr) as total_mns
    FROM MNS
    WHERE MNS_DT {date_filter}
    AND mns_ind_liberado = 'S'
    """
    cursor.execute(query_mns_total)
    row_mns = cursor.fetchone()
    total_mns = float(row_mns['total_mns']) if row_mns and row_mns['total_mns'] else 0.0
    
    return df_osm, df_mte, df_ipc, df_smm_rateio, total_mns

def aggregate_unit_revenue_python(df_osm, df_mte, df_ipc, df_smm_rateio, total_mns):
    """Realiza o join e agregação via Pandas com rateio de MNS."""
    if df_osm.empty:
        return []

    # Map OSM -> Unidade (Chave Composta)
    # Map OSM -> Unidade (Chave Composta)
    # Normalize unit names to avoid duplicates
    df_osm['unidade'] = df_osm['unidade'].astype(str).str.strip()
    osm_unit_map = df_osm.set_index(['osm_num', 'osm_serie'])['unidade'].to_dict()
    
    # 1. MTE por Unidade (Direto)
    mte_unit = {}
    if not df_mte.empty:
        for _, row in df_mte.iterrows():
            key = (row['mte_osm'], row['mte_osm_serie'])
            u = osm_unit_map.get(key, 'DESCONHECIDO')
            # Using 'valor_liquido' which is now GROSS in SQL
            mte_unit[u] = mte_unit.get(u, 0.0) + float(row['valor_liquido'])

    # 2. IPC por Unidade (Direto)
    ipc_unit = {}
    if not df_ipc.empty:
        for _, row in df_ipc.iterrows():
            key = (row['IPC_OSM_NUM'], row['IPC_OSM_SERIE'])
            u = osm_unit_map.get(key, 'DESCONHECIDO')
            ipc_unit[u] = ipc_unit.get(u, 0.0) + float(row['valor'])
            
    # 3. MNS Rateado (Proporcional ao SMM Convênio da Unidade)
    mns_unit = {}
    if not df_smm_rateio.empty and total_mns > 0:
        # Columns: smm_vlr by unidade
        smm_agg = df_smm_rateio.groupby('unidade')['smm_vlr'].sum()
        total_smm = smm_agg.sum()
        
        if total_smm > 0:
            for u, val in smm_agg.items():
                u_str = str(u).strip()
                share = val / total_smm
                mns_unit[u_str] = share * total_mns

    # Consolidar
    all_units = set(df_osm['unidade'].unique()) | set(mns_unit.keys()) | set(mte_unit.keys())
    final_data = []
    
    osm_counts = df_osm['unidade'].value_counts().to_dict()
    
    for u in all_units:
        u_str = str(u).strip()
        
        rev_mte = mte_unit.get(u_str, 0.0)
        rev_ipc = ipc_unit.get(u_str, 0.0)
        rev_mns = mns_unit.get(u_str, 0.0)
        
        total_rev = rev_mte + rev_ipc + rev_mns
        
        final_data.append({
            'unidade': u_str,
            'faturamento': total_rev,
            'atendimentos': int(osm_counts.get(u_str, 0))
        })
        
    return sorted(final_data, key=lambda x: x['faturamento'], reverse=True)

def get_exam_sla_data(cursor, start_date, end_date, filter_type='particular'):
    """Busca dados de exames e prazos para cálculo de SLA."""
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    # Corrigindo filtro: osm_cnv '1' é particular. Usando TRIM para segurança.
    cnv_filter = "LTRIM(RTRIM(o.osm_cnv)) = '1'" if filter_type == 'particular' else "LTRIM(RTRIM(o.osm_cnv)) <> '1'"
    
    query = f"""
    SELECT 
        s.smm_osm, s.smm_osm_serie, s.smm_cod, s.smm_tpcod, 
        s.SMM_DT_RESULT, o.osm_dthr, o.osm_dt_result, st.str_nome as unidade,
        k.smk_prazo, k.SMK_ELD_HORAS
    FROM smm s
    INNER JOIN osm o ON s.smm_osm = o.osm_num AND s.smm_osm_serie = o.osm_serie
    INNER JOIN str st ON o.osm_str = st.str_cod
    LEFT JOIN smk k ON s.smm_cod = k.smk_cod AND s.smm_tpcod = k.smk_tipo
    WHERE o.osm_dthr {date_filter}
    AND {cnv_filter}
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    """
    cursor.execute(query)
    return pd.DataFrame(cursor.fetchall())

def calculate_exam_sla_python(df):
    """Calcula o SLA (Dentro do Prazo) baseado nos dados brutos."""
    if df.empty:
        return []

    # Garantir formatos de data
    df['osm_dthr'] = pd.to_datetime(df['osm_dthr'])
    df['osm_dt_result'] = pd.to_datetime(df['osm_dt_result'])
    df['SMM_DT_RESULT'] = pd.to_datetime(df['SMM_DT_RESULT'])
    
    # Calcular data limite
    def calc_deadline(row):
        # Prioridade 1: Previsão calculada pelo sistema e salva na OS
        if pd.notnull(row['osm_dt_result']):
            return row['osm_dt_result']
            
        # Prioridade 2: Cálculo teórico baseado em smk_prazo ou ELD_HORAS
        base_time = row['osm_dthr']
        hours = row['SMK_ELD_HORAS'] if pd.notnull(row['SMK_ELD_HORAS']) and row['SMK_ELD_HORAS'] > 0 else 0
        days = row['smk_prazo'] if pd.notnull(row['smk_prazo']) and row['smk_prazo'] > 0 else 0
        
        if hours > 0:
            return base_time + pd.Timedelta(hours=hours)
        elif days > 0:
            return base_time + pd.Timedelta(days=days)
        else:
            return base_time + pd.Timedelta(days=3) # Fallback padrão

    df['data_limite'] = df.apply(calc_deadline, axis=1)
    
    # Calcular a duração do prazo em dias (para exibição)
    df['prazo_dias'] = (df['data_limite'] - df['osm_dthr']).dt.total_seconds() / (24 * 3600)
    
    # Verificar se está no prazo
    def check_on_time(row):
        if pd.isnull(row['SMM_DT_RESULT']):
            return np.nan # Ignorar exames sem resultado para o KPI de performance de entrega
        
        # Consideramos No Prazo se entregue até o limite (incluindo o mesmo segundo)
        return 1 if row['SMM_DT_RESULT'] <= row['data_limite'] else 0

    df['no_prazo'] = df.apply(check_on_time, axis=1)
    df_valid = df.dropna(subset=['no_prazo'])
    
    if df_valid.empty:
        return []

    # Agregação por Unidade
    result = df_valid.groupby('unidade').agg({
        'no_prazo': ['sum', 'count'],
        'prazo_dias': 'mean'
    }).reset_index()
    
    result.columns = ['unidade', 'no_prazo_count', 'total_exames', 'prazo_medio_dias']
    result['atrasados'] = result['total_exames'] - result['no_prazo_count']
    result['percentual_no_prazo'] = (result['no_prazo_count'] / result['total_exames']) * 100
    
    # Converter para tipos nativos do Python
    final_data = []
    for _, row in result.iterrows():
        final_data.append({
            'unidade': str(row['unidade']).strip(),
            'percentual_no_prazo': float(row['percentual_no_prazo']),
            'total_exames': int(row['total_exames']),
            'no_prazo': int(row['no_prazo_count']),
            'atrasados': int(row['atrasados']),
            'prazo_medio_dias': float(row['prazo_medio_dias'])
        })
    
    return sorted(final_data, key=lambda x: x['percentual_no_prazo'], reverse=True)

def get_clients_analytics_data(cursor, start_date, end_date):
    """Busca dados de pacientes que tiveram atendimento no período."""
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    query = f"""
    SELECT DISTINCT
        p.pac_reg, p.pac_sexo, p.pac_nasc, p.pac_cid, p.pac_est_civil, p.pac_dreg
    FROM pac p
    INNER JOIN osm o ON p.pac_reg = o.osm_pac
    WHERE o.osm_dthr {date_filter}
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    """
    cursor.execute(query)
    return pd.DataFrame(cursor.fetchall())

def process_clients_analytics_python(df, start_date, end_date):
    """Processa métricas demográficas e de CRM via Pandas."""
    if df.empty:
        return None

    # 1. Total Clientes Únicos
    total_unique = int(df['pac_reg'].nunique())

    # 2. Gênero
    gender_counts = df['pac_sexo'].value_counts().to_dict()
    # Mapear para labels amigáveis
    gender_map = {'M': 'Masculino', 'F': 'Feminino'}
    gender_final = {gender_map.get(k, 'Outros'): int(v) for k, v in gender_counts.items()}

    # 3. Faixa Etária
    df['pac_nasc'] = pd.to_datetime(df['pac_nasc'])
    today = pd.Timestamp.now()
    df['idade'] = (today - df['pac_nasc']).dt.days // 365
    
    bins = [0, 12, 18, 30, 45, 60, 120]
    labels = ['Infantil (0-12)', 'Jovem (13-18)', 'Adulto Jovem (19-30)', 'Adulto (31-45)', 'Adulto Madure (46-60)', 'Sênior (60+)']
    df['faixa_etaria'] = pd.cut(df['idade'], bins=bins, labels=labels, right=False)
    age_counts = df['faixa_etaria'].value_counts().to_dict()
    age_final = {str(k): int(v) for k, v in age_counts.items()}

    # 3.1 Faixa Etária por Gênero (Cruzamento)
    # Mapear gênero no dataframe para labels
    df['genero_label'] = df['pac_sexo'].map(gender_map).fillna('Outros')
    cross_tab = pd.crosstab(df['faixa_etaria'], df['genero_label']).to_dict()
    
    # Formatação amigável: {faixa: {Masculino: X, Feminino: Y}}
    age_gender_final = {}
    for label in labels:
        age_gender_final[label] = {
            gender: int(cross_tab.get(gender, {}).get(label, 0))
            for gender in gender_map.values()
        }

    # 4. Cidades (Top 10) - Limpeza e Consolidação
    df['cidade_clean'] = df['pac_cid'].str.strip().str.upper()
    
    # Filtrar nomes que são apenas números (códigos perdidos no campo de texto)
    df_cities = df[~df['cidade_clean'].str.contains(r'^\d+$', na=False)].copy()
    
    city_counts = df_cities['cidade_clean'].value_counts().head(10).reset_index()
    city_counts.columns = ['cidade', 'quantidade']
    cities_final = city_counts.to_dict(orient='records')

    # 5. Estado Civil
    civil_map = {
        'S': 'Solteiro(a)', 'C': 'Casado(a)', 'V': 'Viúvo(a)', 
        'D': 'Divorciado(a)', 'O': 'Outros', ' ': 'Não Informado'
    }
    civil_counts = df['pac_est_civil'].fillna(' ').value_counts().to_dict()
    civil_final = {civil_map.get(str(k).strip(), 'Outros'): int(v) for k, v in civil_counts.items()}

    # 6. Novos Clientes (Cadastrados no período)
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    df['pac_dreg'] = pd.to_datetime(df['pac_dreg'])
    novos = int(df[df['pac_dreg'].between(start_dt, end_dt)]['pac_reg'].nunique())

    return {
        "total_clientes": total_unique,
        "novos_clientes": novos,
        "genero": gender_final,
        "faixa_etaria": age_final,
        "faixa_etaria_por_genero": age_gender_final,
        "cidades": cities_final,
        "estado_civil": civil_final
    }

def get_financial_analytics_data(cursor, start_date, end_date):
    """
    Busca dados de faturamento (Total Bruto) usando a lógica Oficial do BI do Cliente.
    Fonte: OSM + SMM + CNV + STR.
    Filtros: cnv_caixa_fatura='C', str_str_cod='01', smm_sfat <> 'C'.
    """
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # 1. Faturamento Total, Custo e Ranking por Convênio (Baseado na query Oficial - CAIXA)
    # Total Bruto: SUM(smm_vlr)
    # Custo: SUM(SMM_AJUSTE_VLR)
    query_gross = f"""
    SELECT 
        SUM(sm.smm_vlr) as total_bruto,
        SUM(ISNULL(sm.SMM_AJUSTE_VLR, 0)) as total_custo,
        c.cnv_nome as convenio
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'C'
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    GROUP BY c.cnv_nome
    """
    cursor.execute(query_gross)
    df_gross = pd.DataFrame(cursor.fetchall())
    
    # 1.2 Faturamento CONVÊNIO (Fatura 'F')
    # Adicionando query para pegar o montante de 'F' (Convênios faturados)
    query_fatura = f"""
    SELECT 
        SUM(sm.smm_vlr) as total_fatura
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'F'
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    """
    cursor.execute(query_fatura)
    row_fatura = cursor.fetchone()
    valor_fatura_convenio = float(row_fatura['total_fatura']) if row_fatura and row_fatura['total_fatura'] else 0.0

    # 1.3 Faturamento por UNIDADE (Líquido: Bruto + Ajuste) - Apenas 'C' conforme SQL do usuario
    # Quebrando em Faturado e Custo para calculo de margem
    query_units = f"""
    SELECT 
        s.str_nome as unidade,
        SUM(sm.smm_vlr) as faturado,
        SUM(ISNULL(sm.SMM_AJUSTE_VLR, 0)) as custo
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'C'
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    GROUP BY s.str_nome
    ORDER BY faturado DESC
    """
    cursor.execute(query_units)
    df_units = pd.DataFrame(cursor.fetchall())
    
    # 1.4 Faturamento por UNIDADE - CONVÊNIO (Tipo 'F')
    query_units_convenio = f"""
    SELECT 
        s.str_nome as unidade,
        SUM(sm.smm_vlr) as faturado_convenio
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura = 'F'
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    GROUP BY s.str_nome
    ORDER BY faturado_convenio DESC
    """
    cursor.execute(query_units_convenio)
    df_units_convenio = pd.DataFrame(cursor.fetchall())
    
    # 2. Recebimentos e Glosas (BXA) - Mantendo lógica original de caixa
    query_bxa = f"""
    SELECT 
        b.BXA_VALOR_RECEB,
        b.BXA_VALOR_GLOSA
    FROM BXA b
    WHERE b.BXA_DTHR {date_filter}
    AND b.BXA_STATUS <> 'C'
    """
    cursor.execute(query_bxa)
    df_caixa = pd.DataFrame(cursor.fetchall())
    
    # 3. Total de Atendimentos (para Ticket Médio)
    query_osm_count = f"SELECT COUNT(*) as total FROM osm WHERE osm_dthr {date_filter} AND (osm_status IS NULL OR osm_status <> 'C')"
    cursor.execute(query_osm_count)
    total_atendimentos = cursor.fetchone()['total']
    
    return df_gross, df_caixa, total_atendimentos, valor_fatura_convenio, df_units, df_units_convenio

def process_financial_analytics_python(df_gross, df_caixa, total_atendimentos, valor_fatura_convenio=0.0, df_units=None, df_units_convenio=None):
    """Processa métricas financeiras usando a lógica oficial."""
    
    # 1. Faturamento Total Bruto e Custo (CAIXA)
    if df_gross.empty:
        faturado_caixa = 0.0
        custo_caixa = 0.0
        top_cnv_final = []
    else:
        # Soma total
        faturado_caixa = float(df_gross['total_bruto'].sum())
        custo_caixa = float(df_gross['total_custo'].sum())
        
        # Ranking de Convênios
        df_top_cnv = df_gross.groupby('convenio')['total_bruto'].sum().reset_index()
        df_top_cnv['total_bruto'] = df_top_cnv['total_bruto'].astype(float).round(2)
        df_top_cnv = df_top_cnv.sort_values(by='total_bruto', ascending=False).head(10)
        top_cnv_final = df_top_cnv.rename(columns={'convenio': 'convenio', 'total_bruto': 'faturado'}).to_dict(orient='records')
        
    # 1.2 Cálculo do Total Geral (Caixa Líquido + Fatura Bruto)
    # Caixa Líquido = Bruto + Ajustes (que são negativos)
    caixa_liquido = faturado_caixa + custo_caixa
    total_geral = caixa_liquido + valor_fatura_convenio

    # 1.3 Breakdown por Unidade (merging Type C and Type F)
    if df_units is not None and not df_units.empty:
        # Calcular campos derivados Type C
        df_units['faturado'] = df_units['faturado'].astype(float)
        df_units['custo'] = df_units['custo'].astype(float)
        df_units['liquido'] = df_units['faturado'] + df_units['custo']
        
        # Margem = (Liquido / Faturado) * 100
        df_units['margem'] = df_units.apply(lambda row: (row['liquido'] / row['faturado'] * 100) if row['faturado'] > 0 else 0.0, axis=1)

        # Merge Type F data
        if df_units_convenio is not None and not df_units_convenio.empty:
            df_units_convenio['faturado_convenio'] = df_units_convenio['faturado_convenio'].astype(float)
            df_units = df_units.merge(df_units_convenio, on='unidade', how='left')
        
        # Fill NaN with 0 for units without Type F revenue
        df_units['faturado_convenio'] = df_units['faturado_convenio'].fillna(0.0)

        # Rounding
        df_units['faturado'] = df_units['faturado'].round(2)
        df_units['custo'] = df_units['custo'].round(2)
        df_units['liquido'] = df_units['liquido'].round(2)
        df_units['margem'] = df_units['margem'].round(2)
        df_units['faturado_convenio'] = df_units['faturado_convenio'].round(2)
        
        units_final = df_units.to_dict(orient='records')
    else:
        units_final = []

    # 2. Recebimento e Glosa
    recebido_total = float(df_caixa['BXA_VALOR_RECEB'].sum()) if not df_caixa.empty else 0.0
    glosa_total = float(df_caixa['BXA_VALOR_GLOSA'].sum()) if not df_caixa.empty else 0.0
    
    percentual_glosa = 0.0
    if (recebido_total + glosa_total) > 0:
        percentual_glosa = (glosa_total / (recebido_total + glosa_total)) * 100
        
    # Ticket Médio Global (Usando Total Geral ou só Caixa? Usualmente sobre Produção Total)
    # Vamos usar Total Geral para Ticket Médio ficar realista sobre todo o volume
    ticket_medio = (total_geral / total_atendimentos) if total_atendimentos > 0 else 0.0
    
    return {
        "faturado_total": round(faturado_caixa, 2), # Mantendo nome legado para compatibilidade (mas é Caixa Bruto)
        "faturado_convenio": round(valor_fatura_convenio, 2), # Novo
        "total_geral": round(total_geral, 2), # Novo
        "custo_total": round(custo_caixa, 2),
        "recebido_total": round(recebido_total, 2),
        "glosa_total": round(glosa_total, 2),
        "percentual_glosa": round(percentual_glosa, 2),
        "ticket_medio_global": round(ticket_medio, 2),
        "faturamento_por_convenio": top_cnv_final,
        "faturamento_por_unidade": units_final # Novo
    }

def get_commercial_analytics_data(cursor, start_date, end_date):
    """Busca dados de produção por médico solicitante."""
    
    query = f"""
    SELECT 
        p.psv_nome,
        p.psv_crm,
        p.psv_uf,
        COUNT(DISTINCT o.osm_num) as qtd_pedidos,
        SUM(i.ipc_valor) as valor_total
    FROM osm o
    INNER JOIN psv p ON o.osm_mreq = p.psv_cod
    INNER JOIN ipc i ON o.osm_serie = i.ipc_osm_serie AND o.osm_num = i.ipc_osm_num
    WHERE o.osm_dthr BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    GROUP BY p.psv_nome, p.psv_crm, p.psv_uf
    ORDER BY valor_total DESC
    """
    
    cursor.execute(query)
    return pd.DataFrame(cursor.fetchall())

def process_commercial_analytics_python(df_medicos):
    """Processa o ranking de médicos."""
    if df_medicos.empty:
        return []
        
    results = []
    for _, row in df_medicos.iterrows():
        qtd = int(row['qtd_pedidos'])
        valor = float(row['valor_total'])
        ticket = (valor / qtd) if qtd > 0 else 0.0
        
        results.append({
            "nome": str(row['psv_nome']).strip(),
            "crm": int(row['psv_crm']) if pd.notnull(row['psv_crm']) and row['psv_crm'] != 0 else None,
            "uf": str(row['psv_uf']).strip() if pd.notnull(row['psv_uf']) else None,
            "qtd_pedidos": qtd,
            "valor_total": round(valor, 2),
            "ticket_medio": round(ticket, 2)
        })
        
    return results

def get_detailed_finance_data(cursor, start_date, end_date):
    """Busca dados para o relatorio financeiro detalhado."""
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # 1. Faturamento Particular (Bruto e Descontos)
    # Reutilizando logica do MTE mas focando nos descontos tambem
    query_mte = f"""
    SELECT 
        SUM(ISNULL(m.mte_valor, 0)) as bruto,
        SUM(ISNULL(m.mte_desconto, 0)) as desconto
    FROM MTE m
    INNER JOIN OSM o ON m.mte_osm = o.osm_num AND m.mte_osm_serie = o.osm_serie
    WHERE o.osm_dthr {date_filter}
    """
    cursor.execute(query_mte)
    mte_totals = cursor.fetchone()
    
    # 2. Pagamentos (Tentativa de Classificacao via MCC)
    # Vincular MTE -> MCC e olhar OBS
    query_payments = f"""
    SELECT 
        mc.MCC_OBS, 
        SUM(mc.MCC_CRE) as valor
    FROM MTE m
    INNER JOIN OSM o ON m.mte_osm = o.osm_num AND m.mte_osm_serie = o.osm_serie
    INNER JOIN MCC mc ON m.mte_mcc_serie_caixa = mc.MCC_SERIE AND m.mte_mcc_lote_caixa = mc.MCC_LOTE
    WHERE o.osm_dthr {date_filter}
    GROUP BY mc.MCC_OBS
    """
    cursor.execute(query_payments)
    df_payments = pd.DataFrame(cursor.fetchall())
    
    # 3. Pacientes (Novos vs Recorrentes)
    # Novos: DREG dentro do periodo
    # Recorrentes: DREG antes do periodo
    query_pac = f"""
    SELECT 
        CASE 
            WHEN p.pac_dreg {date_filter} THEN 'NOVO'
            ELSE 'RECORRENTE'
        END as tipo_paciente,
        COUNT(DISTINCT p.pac_reg) as qtd
    FROM PAC p
    INNER JOIN OSM o ON p.pac_reg = o.osm_pac
    WHERE o.osm_dthr {date_filter}
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    GROUP BY 
        CASE 
            WHEN p.pac_dreg {date_filter} THEN 'NOVO'
            ELSE 'RECORRENTE'
        END
    """
    cursor.execute(query_pac)
    df_patients = pd.DataFrame(cursor.fetchall())
    
    return mte_totals, df_payments, df_patients

def process_detailed_finance_python(mte_totals, df_payments, df_patients):
    """Processa os dados brutos para o formato do relatorio."""
    
    # 1. Totais MTE
    bruto = float(mte_totals['bruto']) if mte_totals and mte_totals['bruto'] else 0.0
    desconto = float(mte_totals['desconto']) if mte_totals and mte_totals['desconto'] else 0.0
    liquido = bruto - desconto
    indice_desconto = (desconto / bruto * 100) if bruto > 0 else 0.0
    
    # 2. Classificacao Pagamentos (Heuristica por Texto)
    pagamento_map = {'PIX': 0.0, 'CARTAO': 0.0, 'ESPECIE': 0.0, 'OUTROS': 0.0}
    
    if not df_payments.empty:
        for _, row in df_payments.iterrows():
            obs = str(row['MCC_OBS']).upper()
            val = float(row['valor'])
            
            if 'PIX' in obs:
                pagamento_map['PIX'] += val
            elif any(x in obs for x in ['CARTAO', 'CARD', 'VISA', 'MASTER', 'DEBITO', 'CREDITO']):
                pagamento_map['CARTAO'] += val
            elif any(x in obs for x in ['DINHEIRO', 'ESP', 'ESPECIE']):
                pagamento_map['ESPECIE'] += val
            else:
                pagamento_map['OUTROS'] += val
                
    # 3. Pacientes
    pacientes = {'novos': 0, 'recorrentes': 0, 'total': 0}
    if not df_patients.empty:
        for _, row in df_patients.iterrows():
            tipo = row['tipo_paciente']
            qtd = int(row['qtd'])
            if tipo == 'NOVO':
                pacientes['novos'] = qtd
            else:
                pacientes['recorrentes'] = qtd
    pacientes['total'] = pacientes['novos'] + pacientes['recorrentes']
    
    return {
        'faturamento': {
            'bruto': bruto,
            'desconto': desconto,
            'liquido': liquido,
            'indice_desconto': round(indice_desconto, 2)
        },
        'pagamentos': pagamento_map,
        'pacientes': pacientes,
        'meta': { # Placeholder
            'projetada': 0.0,
            'realizado_percent': 0.0
        }
    }
