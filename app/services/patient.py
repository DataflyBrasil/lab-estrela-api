import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Optional
from ..models.base import (
    PatientIntelligenceResponse, PatientDemographics, PatientSocioEconomic, 
    PatientPersona, PatientAdvancedAnalytics, ProcedureByAgeGroup, 
    GeoMarketingConfig, ChurnRiskPatient
)

def get_patient_data(cursor, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetches consolidated patient data (Demographics, Financials, Exams) for metrics.
    """
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    query = f"""
    SELECT 
        P.pac_reg,
        P.pac_nome,
        P.pac_sexo,
        P.pac_nasc,
        P.pac_cid,
        O.osm_num,
        O.osm_dthr,
        S.smm_vlr,
        K.smk_nome as exame_nome
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    INNER JOIN SMM S WITH(NOLOCK) ON O.osm_num = S.smm_osm AND O.osm_serie = S.smm_osm_serie
    INNER JOIN SMK K WITH(NOLOCK) ON S.smm_cod = K.smk_cod
    WHERE O.osm_dthr {date_filter}
    AND P.pac_nome NOT LIKE 'teste%'
    AND S.smm_vlr > 0
    """
    
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    results = cursor.fetchall()
    
    data = []
    for row in results:
        data.append(dict(zip(columns, row)))
        
    return pd.DataFrame(data)

def calculate_age(born: date, reference_date: date = date.today()) -> int:
    if not born:
        return 0
    return reference_date.year - born.year - ((reference_date.month, reference_date.day) < (born.month, born.day))

def get_age_group(age: int) -> str:
    if age <= 5: return "0-5"
    if age <= 12: return "6-12"
    if age <= 18: return "13-18"
    if age <= 29: return "19-29"
    if age <= 49: return "30-49"
    if age <= 69: return "50-69"
    return "70+"

def process_patient_intelligence(df: pd.DataFrame) -> PatientIntelligenceResponse:
    if df.empty:
        return PatientIntelligenceResponse(
            demographics=PatientDemographics(
                total_pacientes=0,
                sexo_distribuicao={},
                faixa_etaria_distribuicao={},
                top_cidades={}
            ),
            socioeconomic=PatientSocioEconomic(
                ticket_medio_geral=0.0,
                top_pacientes_vip=[],
                fidelidade_recorrencia={}
            ),
            persona=PatientPersona(descricao="Sem dados suficientes"),
            advanced=PatientAdvancedAnalytics(
                top_procedimentos_por_idade=[],
                recencia_media_dias=0.0,
                geomarketing=[],
                risco_churn=[]
            )
        )

    # --- Pre-processing ---
    # Convert dates
    df['osm_dthr'] = pd.to_datetime(df['osm_dthr'])
    df['pac_nasc'] = pd.to_datetime(df['pac_nasc'], errors='coerce')
    df['smm_vlr'] = pd.to_numeric(df['smm_vlr'], errors='coerce').fillna(0.0)
    
    # Calculate Age
    now = datetime.now()
    df['idade'] = df['pac_nasc'].apply(lambda x: calculate_age(x, now.date()) if pd.notnull(x) else 0)
    df['faixa_etaria'] = df['idade'].apply(get_age_group)
    
    # --- 1. Demographics ---
    unique_patients = df.drop_duplicates('pac_reg')
    total_pacientes = len(unique_patients)
    
    # Sex distribution
    sexo_dist = unique_patients['pac_sexo'].value_counts(normalize=True).to_dict()
    sexo_dist = {k: round(v * 100, 1) for k, v in sexo_dist.items() if k}
    
    # Age distribution
    age_dist = unique_patients['faixa_etaria'].value_counts(normalize=True).to_dict()
    age_dist = {k: round(v * 100, 1) for k, v in age_dist.items()}
    
    # Top Cities
    city_dist = unique_patients['pac_cid'].value_counts().head(5).to_dict()
    
    demographics = PatientDemographics(
        total_pacientes=total_pacientes,
        sexo_distribuicao=sexo_dist,
        faixa_etaria_distribuicao=age_dist,
        top_cidades=city_dist
    )
    
    # --- 2. Socioeconomic (LTV/Value) ---
    # Group by patient to get total spent in period
    patient_value = df.groupby(['pac_reg', 'pac_nome'])['smm_vlr'].sum().reset_index()
    ticket_medio = patient_value['smm_vlr'].mean()
    
    # Top VIPs
    top_vips = patient_value.sort_values('smm_vlr', ascending=False).head(20).to_dict('records')
    top_vips_fmt = [{"nome": r['pac_nome'], "valor_total": round(r['smm_vlr'], 2)} for r in top_vips]
    
    # Loyalty (Frequency)
    visit_counts = df.groupby('pac_reg')['osm_num'].nunique()
    recurrence = {
        "unicos": int((visit_counts == 1).sum()),
        "retornaram": int((visit_counts > 1).sum()),
        "fieis_3plus": int((visit_counts >= 3).sum())
    }
    
    socioeconomic = PatientSocioEconomic(
        ticket_medio_geral=round(ticket_medio, 2),
        top_pacientes_vip=top_vips_fmt,
        fidelidade_recorrencia=recurrence
    )
    
    # --- 3. Persona ---
    # Determine dominant profiles
    dom_sex = unique_patients['pac_sexo'].mode()[0] if not unique_patients['pac_sexo'].empty else "N/A"
    dom_age = unique_patients['faixa_etaria'].mode()[0] if not unique_patients['faixa_etaria'].empty else "N/A"
    dom_city = unique_patients['pac_cid'].mode()[0] if not unique_patients['pac_cid'].empty else "N/A"
    
    persona_text = f"O perfil predominante é {dom_sex}, faixa etária {dom_age}, residente em {dom_city}, com ticket médio de R$ {round(ticket_medio, 2)}."
    persona = PatientPersona(descricao=persona_text)
    
    # --- 4. Advanced Analytics ---
    
    # Top Procedures by Age Group
    age_groups = df.groupby('faixa_etaria')
    procs_by_age = []
    
    for age_group, group in age_groups:
        top_exams = group['exame_nome'].value_counts().head(3).index.tolist()
        procs_by_age.append(ProcedureByAgeGroup(
            faixa_etaria=age_group,
            top_exames=top_exams
        ))
    
    # Recency (Average days between visits for recurring patients)
    # We need patients with > 1 visit
    multi_visit_patients = df[df['pac_reg'].isin(visit_counts[visit_counts > 1].index)]
    avg_days_between = 0.0
    
    if not multi_visit_patients.empty:
        # Sort by patient and date
        multi_visit_patients = multi_visit_patients.sort_values(['pac_reg', 'osm_dthr'])
        # Calculate difference between visits
        # We group by patient, taking distinct dates (one visit per day)
        visits_dates = multi_visit_patients.groupby('pac_reg')['osm_dthr'].apply(lambda x: x.dt.date.unique())
        
        diffs = []
        for dates in visits_dates:
            if len(dates) > 1:
                # Calculate diffs between consecutive visits
                sorted_dates = sorted(dates)
                d_diffs = [(sorted_dates[i] - sorted_dates[i-1]).days for i in range(1, len(sorted_dates))]
                diffs.extend(d_diffs)
        
        if diffs:
            avg_days_between = sum(diffs) / len(diffs)

    # Geomarketing (Ticket by City)
    geo_value = df.groupby('pac_cid')['smm_vlr'].mean().reset_index().sort_values('smm_vlr', ascending=False)
    geomarketing = [
        GeoMarketingConfig(cidade=row['pac_cid'], ticket_medio=round(row['smm_vlr'], 2)) 
        for _, row in geo_value.head(10).iterrows() if row['pac_cid']
    ]

    # Churn Risk (Placeholder logic for V1: VIPs who haven't visited in last 90 days of the selected period?)
    # A true churn analysis needs a broader dataset. 
    # For now, let's identify "Slipping Away" - Patients with high frequency early in period but 0 in late period?
    # Or simplified: List Top 5 VIPs who we haven't seen in the last 60 days of the selected range.
    
    range_end = df['osm_dthr'].max()
    churn_threshold_date = range_end - pd.Timedelta(days=60)
    
    # VIPs (Top 50)
    top_50_regs = patient_value.sort_values('smm_vlr', ascending=False).head(50)['pac_reg'].tolist()
    
    # Check their last visit
    last_visits = df.groupby('pac_reg')['osm_dthr'].max()
    at_risk = []
    
    for reg in top_50_regs:
        last = last_visits[reg]
        if last < churn_threshold_date:
            # Has slipped away
            p_name = unique_patients[unique_patients['pac_reg'] == reg]['pac_nome'].iloc[0]
            days_absent = (range_end - last).days
            at_risk.append(ChurnRiskPatient(
                paciente=p_name,
                dias_sem_visita=days_absent,
                valor_historico=round(patient_value[patient_value['pac_reg'] == reg]['smm_vlr'].values[0], 2)
            ))
            
    advanced = PatientAdvancedAnalytics(
        top_procedimentos_por_idade=procs_by_age,
        recencia_media_dias=round(avg_days_between, 1),
        geomarketing=geomarketing,
        risco_churn=at_risk[:10] # Top 10 at risk
    )
    
    return PatientIntelligenceResponse(
        success=True,
        demographics=demographics,
        socioeconomic=socioeconomic,
        persona=persona,
        advanced=advanced
    )

# --- Optimized SQL Functions for Micro-Endpoints ---

def get_demographics_sql(cursor, start_date: str, end_date: str) -> PatientDemographics:
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # 1. Total & Sexo (Single Pass)
    query_sex = f"""
    SELECT 
        COUNT(DISTINCT P.pac_reg) as total,
        P.pac_sexo,
        COUNT(DISTINCT P.pac_reg) as qtd
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    WHERE O.osm_dthr {date_filter}
    AND P.pac_nome NOT LIKE 'teste%'
    GROUP BY P.pac_sexo
    """
    cursor.execute(query_sex)
    results_sex = cursor.fetchall()
    
    total_pacientes = 0
    sexo_dist = {}
    
    # Process Sex Results
    if results_sex:
        for row in results_sex:
            sex = row[1] if row[1] else "N/A"
            qtd = row[2]
            # Accumulate total from groups to ensure consistency
            if sex:
                sexo_dist[sex] = qtd
                total_pacientes += qtd
            
    # Normalize percentages
    sexo_dist_pct = {k: round(v / total_pacientes * 100, 1) for k, v in sexo_dist.items()} if total_pacientes > 0 else {}

    # 2. Age Groups (Server-Side Calculation)
    query_age = f"""
    SELECT 
        CASE 
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 5 THEN '0-5'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 12 THEN '6-12'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 18 THEN '13-18'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 29 THEN '19-29'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 49 THEN '30-49'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 69 THEN '50-69'
            ELSE '70+'
        END as faixa,
        COUNT(DISTINCT P.pac_reg) as qtd
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    WHERE O.osm_dthr {date_filter}
    AND P.pac_nome NOT LIKE 'teste%'
    GROUP BY 
        CASE 
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 5 THEN '0-5'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 12 THEN '6-12'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 18 THEN '13-18'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 29 THEN '19-29'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 49 THEN '30-49'
            WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 69 THEN '50-69'
            ELSE '70+'
        END
    """
    cursor.execute(query_age)
    results_age = cursor.fetchall()
    age_dist = {row[0]: row[1] for row in results_age}
    age_dist_pct = {k: round(v / total_pacientes * 100, 1) for k, v in age_dist.items()} if total_pacientes > 0 else {}

    # 3. Top Cities
    query_city = f"""
    SELECT TOP 5
        P.pac_cid,
        COUNT(DISTINCT P.pac_reg) as qtd
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    WHERE O.osm_dthr {date_filter}
    AND P.pac_nome NOT LIKE 'teste%'
    GROUP BY P.pac_cid
    ORDER BY qtd DESC
    """
    cursor.execute(query_city)
    results_city = cursor.fetchall()
    city_dist = {row[0]: row[1] for row in results_city if row[0]}

    return PatientDemographics(
        total_pacientes=total_pacientes,
        sexo_distribuicao=sexo_dist_pct,
        faixa_etaria_distribuicao=age_dist_pct,
        top_cidades=city_dist
    )

def get_financial_sql(cursor, start_date: str, end_date: str) -> PatientSocioEconomic:
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # 1. Ticket Médio & Totais
    query_totals = f"""
    SELECT 
        SUM(S.smm_vlr) as total_revenue,
        COUNT(DISTINCT P.pac_reg) as total_patients
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    INNER JOIN SMM S WITH(NOLOCK) ON O.osm_num = S.smm_osm AND O.osm_serie = S.smm_osm_serie
    WHERE O.osm_dthr {date_filter}
    AND P.pac_nome NOT LIKE 'teste%'
    AND S.smm_vlr > 0
    """
    cursor.execute(query_totals)
    row_totals = cursor.fetchone()
    total_rev = row_totals[0] if row_totals and row_totals[0] else 0.0
    total_pat = row_totals[1] if row_totals and row_totals[1] else 1
    
    ticket_medio = total_rev / total_pat if total_pat > 0 else 0.0
    
    # 2. Top VIPs
    query_vips = f"""
    SELECT TOP 20
        P.pac_nome,
        SUM(S.smm_vlr) as total_gasto
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    INNER JOIN SMM S WITH(NOLOCK) ON O.osm_num = S.smm_osm AND O.osm_serie = S.smm_osm_serie
    WHERE O.osm_dthr {date_filter}
    AND P.pac_nome NOT LIKE 'teste%'
    GROUP BY P.pac_reg, P.pac_nome
    ORDER BY total_gasto DESC
    """
    cursor.execute(query_vips)
    vips = [{"nome": row[0], "valor_total": round(row[1], 2)} for row in cursor.fetchall()]
    
    # 3. Recorrência (Frequency Buckets)
    query_freq = f"""
    SELECT 
        freq_bucket,
        COUNT(*) as qtd_pacientes
    FROM (
        SELECT 
            CASE 
                WHEN COUNT(DISTINCT O.osm_num) = 1 THEN 'unicos'
                WHEN COUNT(DISTINCT O.osm_num) >= 3 THEN 'fieis_3plus'
                ELSE 'retornaram'
            END as freq_bucket
        FROM OSM O WITH(NOLOCK)
        INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
        WHERE O.osm_dthr {date_filter}
        AND P.pac_nome NOT LIKE 'teste%'
        GROUP BY P.pac_reg
    ) as Sub
    GROUP BY freq_bucket
    """
    cursor.execute(query_freq)
    freq_data = cursor.fetchall()
    freq_results = {row[0]: row[1] for row in freq_data} if freq_data else {}
    
    recurrence = {
        "unicos": freq_results.get("unicos", 0),
        "retornaram": freq_results.get("retornaram", 0) + freq_results.get("fieis_3plus", 0),
        "fieis_3plus": freq_results.get("fieis_3plus", 0)
    }

    return PatientSocioEconomic(
        ticket_medio_geral=round(ticket_medio, 2),
        top_pacientes_vip=vips,
        fidelidade_recorrencia=recurrence
    )

def get_advanced_sql(cursor, start_date: str, end_date: str) -> PatientAdvancedAnalytics:
    date_filter = f"BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'"
    
    # 1. Top Procedures by Age Group
    query_proc = f"""
    WITH RankedExams AS (
        SELECT 
            CASE 
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 12 THEN '0-12'
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 18 THEN '13-18'
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 49 THEN '19-49'
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 69 THEN '50-69'
                ELSE '70+'
            END as faixa,
            K.smk_nome as exame,
            COUNT(*) as qtd,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    CASE 
                        WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 12 THEN '0-12'
                        WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 18 THEN '13-18'
                        WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 49 THEN '19-49'
                        WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 69 THEN '50-69'
                        ELSE '70+'
                    END 
                ORDER BY COUNT(*) DESC
            ) as rank_id
        FROM OSM O WITH(NOLOCK)
        INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
        INNER JOIN SMM S WITH(NOLOCK) ON O.osm_num = S.smm_osm AND O.osm_serie = S.smm_osm_serie
        INNER JOIN SMK K WITH(NOLOCK) ON S.smm_cod = K.smk_cod
        WHERE O.osm_dthr {date_filter}
        GROUP BY 
             CASE 
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 12 THEN '0-12'
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 18 THEN '13-18'
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 49 THEN '19-49'
                WHEN DATEDIFF(year, P.pac_nasc, GETDATE()) <= 69 THEN '50-69'
                ELSE '70+'
            END,
            K.smk_nome
    )
    SELECT faixa, exame FROM RankedExams WHERE rank_id <= 3
    """
    cursor.execute(query_proc)
    proc_results = cursor.fetchall()
    
    procs_map = {}
    if proc_results:
        for row in proc_results:
            faixa, exame = row
            if faixa not in procs_map: procs_map[faixa] = []
            procs_map[faixa].append(exame)
        
    procs_fmt = [ProcedureByAgeGroup(faixa_etaria=k, top_exames=v) for k, v in procs_map.items()]

    # 2. Geomarketing
    query_geo = f"""
    SELECT TOP 10 
        P.pac_cid, 
        AVG(S.smm_vlr) as ticket_medio 
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    INNER JOIN SMM S WITH(NOLOCK) ON O.osm_num = S.smm_osm AND O.osm_serie = S.smm_osm_serie
    WHERE O.osm_dthr {date_filter}
    GROUP BY P.pac_cid
    ORDER BY ticket_medio DESC
    """
    cursor.execute(query_geo)
    geomarketing = [
        GeoMarketingConfig(cidade=row[0], ticket_medio=round(row[1], 2)) 
        for row in cursor.fetchall() if row[0]
    ]

    # 3. Recency & Churn (Hybrid)
    query_rec = f"""
    SELECT P.pac_reg, P.pac_nome, O.osm_dthr, SUM(S.smm_vlr) as val
    FROM OSM O WITH(NOLOCK)
    INNER JOIN PAC P WITH(NOLOCK) ON O.osm_pac = P.pac_reg
    INNER JOIN SMM S WITH(NOLOCK) ON O.osm_num = S.smm_osm AND O.osm_serie = S.smm_osm_serie
    WHERE O.osm_dthr {date_filter}
    GROUP BY P.pac_reg, P.pac_nome, O.osm_dthr
    """
    cursor.execute(query_rec)
    df_rec = pd.DataFrame(cursor.fetchall(), columns=['pac_reg', 'pac_nome', 'osm_dthr', 'val'])
    
    avg_days_between = 0.0
    at_risk = []
    
    if not df_rec.empty:
        df_rec['osm_dthr'] = pd.to_datetime(df_rec['osm_dthr'])
        
        # Recency
        visit_counts = df_rec.groupby('pac_reg').size()
        multi_visit_series = visit_counts[visit_counts > 1]
        
        if not multi_visit_series.empty:
             multi_visit_regs = multi_visit_series.index
             multi_visit = df_rec[df_rec['pac_reg'].isin(multi_visit_regs)].sort_values(['pac_reg', 'osm_dthr'])
             
             visits_dates = multi_visit.groupby('pac_reg')['osm_dthr'].apply(lambda x: x.dt.date.unique())
             diffs = []
             for dates in visits_dates:
                 if len(dates) > 1:
                    sorted_dates = sorted(dates)
                    diffs.extend([(sorted_dates[i] - sorted_dates[i-1]).days for i in range(1, len(sorted_dates))])
            
             if diffs:
                avg_days_between = sum(diffs) / len(diffs)
            
        # Churn
        pat_vals = df_rec.groupby(['pac_reg', 'pac_nome'])['val'].sum().reset_index().sort_values('val', ascending=False).head(50)
        range_end = df_rec['osm_dthr'].max()
        churn_thresh = range_end - pd.Timedelta(days=60)
        last_visits = df_rec.groupby('pac_reg')['osm_dthr'].max()
        
        for _, row in pat_vals.iterrows():
            last = last_visits[row['pac_reg']]
            if last < churn_thresh:
                at_risk.append(ChurnRiskPatient(
                    paciente=row['pac_nome'],
                    dias_sem_visita=(range_end - last).days,
                    valor_historico=round(row['val'], 2)
                ))

    return PatientAdvancedAnalytics(
        top_procedimentos_por_idade=procs_fmt,
        recencia_media_dias=round(avg_days_between, 1),
        geomarketing=geomarketing,
        risco_churn=at_risk[:10]
    )
