import pandas as pd
import numpy as np

def get_unit_revenue_data(cursor, start_date, end_date):
    """
    Busca dados de faturamento por unidade usando a lógica Oficial do BI.
    Alinhado com get_financial_analytics_data.
    Retorna: (df_faturamento, df_atendimentos)
    """

    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"

    # Query consolidada: tipos C e F em um único scan com agregação condicional.
    # Elimina o round-trip duplicado que antes escanava OSM+SMM+CNV+STR duas vezes.
    query_faturamento = f"""
    SELECT
        s.str_nome as unidade,
        SUM(CASE WHEN c.cnv_caixa_fatura = 'C' THEN sm.smm_vlr ELSE 0 END)                   as bruto_c,
        SUM(CASE WHEN c.cnv_caixa_fatura = 'C' THEN ISNULL(sm.SMM_AJUSTE_VLR, 0) ELSE 0 END) as ajuste_c,
        SUM(CASE WHEN c.cnv_caixa_fatura = 'F' THEN sm.smm_vlr ELSE 0 END)                   as bruto_f
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c  ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s  ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura IN ('C', 'F')
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    GROUP BY s.str_nome
    """
    cursor.execute(query_faturamento)
    df_faturamento = pd.DataFrame(cursor.fetchall())

    query_osm_count = f"""
    SELECT
        s.str_nome as unidade,
        COUNT(DISTINCT o.osm_num) as atendimentos
    FROM OSM o
    INNER JOIN STR s ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (o.osm_status IS NULL OR o.osm_status <> 'C')
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    GROUP BY s.str_nome
    """
    cursor.execute(query_osm_count)
    df_atendimentos = pd.DataFrame(cursor.fetchall())

    return df_faturamento, df_atendimentos

def aggregate_unit_revenue_python(df_faturamento, df_atendimentos):
    """
    Consolida o faturamento por unidade.
    Faturamento = (Bruto Caixa + Ajuste Caixa) + Bruto Fatura
    Recebe df_faturamento com colunas: unidade, bruto_c, ajuste_c, bruto_f.
    """
    if df_atendimentos.empty:
        return []

    faturamento_map = {}
    if not df_faturamento.empty:
        df_fat = df_faturamento.copy()
        df_fat['unidade'] = df_fat['unidade'].str.strip()
        # ALINHAMENTO COM VALIDAÇÃO DO USUÁRIO: Apenas Faturamento Caixa BRUTO
        df_fat['total'] = df_fat['bruto_c'].astype(float)
        faturamento_map = df_fat.set_index('unidade')['total'].to_dict()

    df_ate = df_atendimentos.copy()
    df_ate['unidade'] = df_ate['unidade'].str.strip()
    df_ate['faturamento'] = df_ate['unidade'].map(faturamento_map).fillna(0.0).round(2)
    df_ate['atendimentos'] = df_ate['atendimentos'].astype(int)

    return (
        df_ate[['unidade', 'faturamento', 'atendimentos']]
        .sort_values('faturamento', ascending=False)
        .to_dict(orient='records')
    )

def get_exam_sla_data(cursor, start_date, end_date, filter_type='particular'):
    """Busca dados de exames e prazos para cálculo de SLA."""
    
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    # Comparação direta em CHAR: SQL Server ignora trailing spaces em igualdade,
    # permitindo uso de índice em osm_cnv (LTRIM/RTRIM bloqueava o índice).
    cnv_filter = "o.osm_cnv = '1'" if filter_type == 'particular' else "o.osm_cnv <> '1'"
    
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

    df['osm_dthr']      = pd.to_datetime(df['osm_dthr'])
    df['osm_dt_result'] = pd.to_datetime(df['osm_dt_result'])
    df['SMM_DT_RESULT'] = pd.to_datetime(df['SMM_DT_RESULT'])

    # Deadline vetorizado com máscaras — substitui df.apply(calc_deadline, axis=1)
    # Prioridade 1: data prevista já calculada pelo sistema e salva na OS
    df['data_limite'] = df['osm_dt_result']

    # Prioridade 2: base + horas ELD (onde osm_dt_result é nulo)
    has_horas = df['data_limite'].isna() & df['SMK_ELD_HORAS'].notna() & (df['SMK_ELD_HORAS'] > 0)
    df.loc[has_horas, 'data_limite'] = (
        df.loc[has_horas, 'osm_dthr'] +
        pd.to_timedelta(df.loc[has_horas, 'SMK_ELD_HORAS'], unit='h')
    )

    # Prioridade 3: base + dias smk_prazo
    has_dias = df['data_limite'].isna() & df['smk_prazo'].notna() & (df['smk_prazo'] > 0)
    df.loc[has_dias, 'data_limite'] = (
        df.loc[has_dias, 'osm_dthr'] +
        pd.to_timedelta(df.loc[has_dias, 'smk_prazo'], unit='D')
    )

    # Prioridade 4: fallback 3 dias
    mask_default = df['data_limite'].isna()
    df.loc[mask_default, 'data_limite'] = df.loc[mask_default, 'osm_dthr'] + pd.Timedelta(days=3)

    # Duração do prazo em dias (para exibição)
    df['prazo_dias'] = (df['data_limite'] - df['osm_dthr']).dt.total_seconds() / (24 * 3600)

    # On-time vetorizado — substitui df.apply(check_on_time, axis=1)
    # Exames sem resultado são excluídos do KPI de entrega (NaN)
    df['no_prazo'] = np.where(
        df['SMM_DT_RESULT'].isna(),
        np.nan,
        (df['SMM_DT_RESULT'] <= df['data_limite']).astype(float)
    )
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

    # Conversão vetorizada — substitui o iterrows() final
    result['unidade'] = result['unidade'].str.strip()
    result = result.rename(columns={'no_prazo_count': 'no_prazo'})
    result['no_prazo']    = result['no_prazo'].astype(int)
    result['total_exames'] = result['total_exames'].astype(int)
    result['atrasados']   = result['atrasados'].astype(int)

    return (
        result[['unidade', 'percentual_no_prazo', 'total_exames', 'no_prazo', 'atrasados', 'prazo_medio_dias']]
        .sort_values('percentual_no_prazo', ascending=False)
        .to_dict(orient='records')
    )

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
    Busca dados de faturamento usando a lógica Oficial do BI do Cliente.
    As 4 queries originais sobre OSM+SMM+CNV+STR foram consolidadas em 1,
    eliminando 3 round-trips redundantes. A divisão por tipo (C/F) e por
    dimensão (convenio, unidade) é feita em Python após o fetch.
    """

    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"

    # Query consolidada: um único scan agrupa por unidade + convenio + tipo.
    query_faturamento = f"""
    SELECT
        s.str_nome      as unidade,
        c.cnv_nome      as convenio,
        c.cnv_caixa_fatura as tipo,
        SUM(sm.smm_vlr)                   as faturado,
        SUM(ISNULL(sm.SMM_AJUSTE_VLR, 0)) as ajuste
    FROM OSM o
    INNER JOIN SMM sm ON o.osm_num = sm.smm_osm AND o.osm_serie = sm.smm_osm_serie
    INNER JOIN CNV c  ON o.osm_cnv = c.cnv_cod
    INNER JOIN STR s  ON o.osm_str = s.str_cod
    WHERE o.osm_dthr {date_filter}
    AND (sm.smm_sfat IS NULL OR sm.smm_sfat <> 'C')
    AND c.cnv_caixa_fatura IN ('C', 'F')
    AND (s.str_str_cod LIKE '01%' OR s.str_str_cod LIKE '04%')
    GROUP BY s.str_nome, c.cnv_nome, c.cnv_caixa_fatura
    """
    cursor.execute(query_faturamento)
    df_all = pd.DataFrame(cursor.fetchall())

    # Derivar os 4 subconjuntos originais sem queries adicionais
    if not df_all.empty:
        df_c = df_all[df_all['tipo'] == 'C']
        df_f = df_all[df_all['tipo'] == 'F']

        # Ranking por convênio (type C)
        df_gross = (
            df_c.groupby('convenio', as_index=False)
            .agg(total_bruto=('faturado', 'sum'), total_custo=('ajuste', 'sum'))
        )
        # Total fatura convênio (type F)
        valor_fatura_convenio = float(df_f['faturado'].sum())
        # Breakdown por unidade (type C) para cálculo de margem
        df_units = (
            df_c.groupby('unidade', as_index=False)
            .agg(faturado=('faturado', 'sum'), custo=('ajuste', 'sum'))
        )
        # Breakdown por unidade (type F)
        df_units_convenio = (
            df_f.groupby('unidade', as_index=False)
            .agg(faturado_convenio=('faturado', 'sum'))
        )
    else:
        df_gross = pd.DataFrame()
        valor_fatura_convenio = 0.0
        df_units = pd.DataFrame()
        df_units_convenio = pd.DataFrame()

    # Recebimentos e Glosas (BXA) — tabela diferente, query separada mantida
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

    # Total de Atendimentos (para Ticket Médio)
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
        
        # Ranking de Convênios — df_gross já vem agrupado por convenio, groupby redundante removido
        df_top_cnv = df_gross[['convenio', 'total_bruto']].copy()
        df_top_cnv['total_bruto'] = df_top_cnv['total_bruto'].astype(float).round(2)
        df_top_cnv = df_top_cnv.sort_values(by='total_bruto', ascending=False).head(10)
        top_cnv_final = df_top_cnv.rename(columns={'total_bruto': 'faturado'}).to_dict(orient='records')
        
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
        
        # Margem = (Liquido / Faturado) * 100 — vetorizado, sem apply() row-by-row
        df_units['margem'] = np.where(
            df_units['faturado'] > 0,
            df_units['liquido'] / df_units['faturado'] * 100,
            0.0
        )

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
