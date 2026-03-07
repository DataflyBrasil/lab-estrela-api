from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import date, datetime, timedelta
import pandas as pd
import logging

from .database import get_db_connection, test_connection, current_db_id
from .models.base import (
    HealthResponse, UnitRevenueResponse, UnitRevenueItem, 
    SLAResponse, SLAItem, ClientsResponse, ClientsMetrics,
    FinancialResponse, FinancialMetrics,
    DoctorRankingItem, CommercialResponse,
    DetailedResponse, SLAMetrics, SLAOperacionalResponse,
    BudgetResponse,
    PatientIntelligenceResponse,
    PatientDemographics, PatientSocioEconomic, PatientAdvancedAnalytics,
    StrategicIndicatorsResponse, UnitsResponse
)
from .services.analytics import (
    get_unit_revenue_data, 
    aggregate_unit_revenue_python,
    get_exam_sla_data,
    calculate_exam_sla_python,
    get_clients_analytics_data,
    process_clients_analytics_python,
    get_financial_analytics_data,
    process_financial_analytics_python,
    get_commercial_analytics_data,
    process_commercial_analytics_python,
    get_detailed_finance_data,
    process_detailed_finance_python
)
from .services.strategic import get_strategic_indicators, get_units
from .ai.api.router import router as ai_router

app = FastAPI(title="Laboratório Estrela API", version="2.0.0")

# Habilitar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ai_router, prefix="/ai", tags=["AI"])

@app.middleware("http")
async def database_selector_middleware(request: Request, call_next):
    """
    Middleware que intercepta o header 'x-database-id' e define no ContextVar
    para que as conexões subsequentes usem o banco correto.
    """
    db_id = request.headers.get("x-database-id", "1")
    
    # Valida se é um ID conhecido (opcional, mas recomendado)
    if db_id not in ["1", "2"]:
        print(f"⚠️ Header 'x-database-id' inválido ou ausente: {db_id}. Usando default '1'.")
        db_id = "1"
        
    print(f"🔗 Selecionando Banco de Dados ID: {db_id} para a rota: {request.url.path}")
    token = current_db_id.set(db_id)
    try:
        response = await call_next(request)
        return response
    finally:
        current_db_id.reset(token)

@app.get("/health", response_model=HealthResponse)
async def health_check():
    success, version = test_connection()
    if success:
        return HealthResponse(status="ok", database="connected", version=version)
    return HealthResponse(status="error", database="disconnected")

@app.get("/management/indicators", response_model=StrategicIndicatorsResponse, tags=["Management"])
def management_indicators(
    start_date: str = Query(..., description="Data inicial (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Data final (YYYY-MM-DD)"),
    unidade: Optional[str] = Query(None, description="Nome da unidade específica")
):
    """
    Retorna os indicadores estratégicos baseados na aba PA CAPA do Excel.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        indicators = get_strategic_indicators(cursor, start_date, end_date, unidade)
        
        conn.close()
        return StrategicIndicatorsResponse(success=True, data=indicators)
    except Exception as e:
        return StrategicIndicatorsResponse(success=False, error=str(e))

@app.get("/unidades", response_model=UnitsResponse, tags=["Management"])
def list_units():
    """
    Lista todas as unidades encontradas no banco de dados ativo.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        units = get_units(cursor)
        
        conn.close()
        return UnitsResponse(success=True, data=units)
    except Exception as e:
        return UnitsResponse(success=False, error=str(e))

@app.get("/unidades/faturamento", response_model=UnitRevenueResponse, tags=["Financeiro"])
def get_unit_revenue(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Filtros de data
        if not start_date:
            start_date = (datetime.now() - timedelta(days=14)).date()
        if not end_date:
            end_date = datetime.now().date()

        # Busca dados brutos
        # NOTE: get_unit_revenue_data now returns df_smm_rateio and total_mns
        df_osm, df_mte, df_ipc, df_smm_rateio, total_mns = get_unit_revenue_data(cursor, start_date, end_date)
        conn.close()

        if df_osm.empty:
            return UnitRevenueResponse(success=True, data=[])
        
        # Agrega via Python
        analytics_result = aggregate_unit_revenue_python(df_osm, df_mte, df_ipc, df_smm_rateio, total_mns)
        
        data = [
            UnitRevenueItem(
                unidade=str(r['unidade']).strip(),
                faturamento=float(r['faturamento']),
                atendimentos=int(r['atendimentos'])
            ) for r in analytics_result
        ]
        
        return UnitRevenueResponse(success=True, data=data)

    except Exception as e:
        return UnitRevenueResponse(success=False, error=str(e))

@app.get("/exames/prazo/particular", response_model=SLAResponse)
def get_exam_sla_particular(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    return handle_exam_sla_request(start_date, end_date, 'particular')

@app.get("/exames/prazo/convenio", response_model=SLAResponse)
def get_exam_sla_convenio(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    return handle_exam_sla_request(start_date, end_date, 'convenio')

def handle_exam_sla_request(start_date, end_date, filter_type):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Filtros de data
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        # Busca dados brutos
        df = get_exam_sla_data(cursor, start_date, end_date, filter_type)
        conn.close()

        if df.empty:
            return SLAResponse(success=True, data=[])
        
        # Agrega via Python
        analytics_result = calculate_exam_sla_python(df)
        
        data = [
            SLAItem(
                unidade=r['unidade'],
                percentual_no_prazo=r['percentual_no_prazo'],
                total_exames=r['total_exames'],
                no_prazo=r['no_prazo'],
                atrasados=r['atrasados'],
                prazo_medio_dias=r['prazo_medio_dias']
            ) for r in analytics_result
        ]
        
        return SLAResponse(success=True, data=data)

    except Exception as e:
        return SLAResponse(success=False, error=str(e))

@app.get("/clients", response_model=ClientsResponse)
def get_clients_analytics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Filtros de data
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        # Busca dados brutos
        df = get_clients_analytics_data(cursor, start_date, end_date)
        conn.close()

        if df.empty:
            return ClientsResponse(success=True, data=None)
        
        # Agrega via Python
        analytics_result = process_clients_analytics_python(df, start_date, end_date)
        
        return ClientsResponse(success=True, data=analytics_result)

    except Exception as e:
        return ClientsResponse(success=False, error=str(e))

@app.get("/financeiro/estrategico", response_model=FinancialResponse)
def get_financial_strategic_analytics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        # Busca dados financeiros
        df_faturamento, df_caixa, total_atendimentos, valor_mte_final, valor_ipc_final, df_units_convenio = get_financial_analytics_data(cursor, start_date, end_date)
        conn.close()

        # Processa via Python
        analytics_result = process_financial_analytics_python(df_faturamento, df_caixa, total_atendimentos, valor_mte_final, valor_ipc_final, df_units_convenio)
        
        return FinancialResponse(success=True, data=analytics_result)

    except Exception as e:
        return FinancialResponse(success=False, error=str(e))

@app.get("/comercial/medicos", response_model=CommercialResponse)
def get_commercial_doctors_analytics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        # Busca dados de produção médica
        df_medicos = get_commercial_analytics_data(cursor, start_date, end_date)
        conn.close()

        # Processa via Python
        analytics_result = process_commercial_analytics_python(df_medicos)
        
        return CommercialResponse(success=True, data=analytics_result)

    except Exception as e:
        return CommercialResponse(success=False, error=str(e))

@app.get("/financeiro/detalhado", response_model=DetailedResponse)
def get_detailed_finance_analytics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        # Busca dados detalhados
        mte_totals, df_payments, df_patients = get_detailed_finance_data(cursor, start_date, end_date)
        conn.close()

        # Processa via Python
        analytics_result = process_detailed_finance_python(mte_totals, df_payments, df_patients)
        
        return DetailedResponse(success=True, data=analytics_result)

    except Exception as e:
        return DetailedResponse(success=False, error=str(e))

@app.get("/operacional/sla", response_model=SLAOperacionalResponse)
def get_sla_operacional(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    """
    Endpoint para análise de SLA (Service Level Agreement) de exames.
    Retorna métricas de atraso de liberação de resultados por unidade, bancada e aparelho.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Default: último mês
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()
        
        # Import SLA utilities
        from .services.sla import get_sla_data, process_sla_operational
        
        # Fetch raw data
        df_sla, df_amostras = get_sla_data(cursor, str(start_date), str(end_date))
        conn.close()
        
        # Process aggregations
        analytics_result = process_sla_operational(df_sla, df_amostras)
        
        return SLAOperacionalResponse(
            success=True,
            data=SLAMetrics(**analytics_result)
        )
        
    except Exception as e:
        return SLAOperacionalResponse(success=False, error=str(e))

@app.get("/comercial/orcamentos", response_model=BudgetResponse)
def get_budgets(start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
                end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Default: último mês
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date().strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().date().strftime('%Y-%m-%d')

        
        from .services.budget import get_budget_data, process_budget_metrics
        
        # 1. Buscar dados consolidados
        df_budgets = get_budget_data(cursor, start_date, end_date)
        conn.close()
        
        # 2. Processar métricas
        metrics = process_budget_metrics(df_budgets)
        
        return {
            "success": True, 
            "data": metrics
        }
    except Exception as e:
        print(f"Erro no endpoint de orçamentos: {e}")
        return {"success": False, "error": str(e)}

@app.get("/inteligencia/pacientes", response_model=PatientIntelligenceResponse)
def get_patient_intelligence(start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
                             end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")):
    """
    Endpoint de Inteligencia de Pacientes (Personas & LTV).
    Retorna métricas demográficas, socioeconômicas e comportamentais.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Default: último ano para ter volume de dados relevante para demografia
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).date().strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().date().strftime('%Y-%m-%d')
            
        from .services.patient import get_patient_data, process_patient_intelligence
        
        # 1. Fetch Data
        df = get_patient_data(cursor, start_date, end_date)
        conn.close()
        
        # 2. Process Intelligence
        intelligence = process_patient_intelligence(df)
        
        return intelligence
        
    except Exception as e:
        print(f"Erro no endpoint de inteligência de pacientes: {e}")
        return {"success": False, "error": str(e)}

# --- Micro-Endpoints for Skeleton Loading ---

@app.get("/inteligencia/demografia", response_model=PatientDemographics)
def get_demographics(start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
                     end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).date().strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().date().strftime('%Y-%m-%d')
            
        from .services.patient import get_demographics_sql
        data = get_demographics_sql(cursor, start_date, end_date)
        conn.close()
        return data
    except Exception as e:
        print(f"Erro no endpoint demografia: {e}")
        # Return empty/safe structure on error or let FastAPI handle 500
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/inteligencia/financeiro", response_model=PatientSocioEconomic)
def get_financial(start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
                  end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).date().strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().date().strftime('%Y-%m-%d')
            
        from .services.patient import get_financial_sql
        data = get_financial_sql(cursor, start_date, end_date)
        conn.close()
        return data
    except Exception as e:
        print(f"Erro no endpoint financeiro: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/inteligencia/avancado", response_model=PatientAdvancedAnalytics)
def get_advanced(start_date: Optional[str] = Query(None, description="Data inicial (YYYY-MM-DD)"),
                 end_date: Optional[str] = Query(None, description="Data final (YYYY-MM-DD)")):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).date().strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().date().strftime('%Y-%m-%d')
            
        from .services.patient import get_advanced_sql
        data = get_advanced_sql(cursor, start_date, end_date)
        conn.close()
        return data
    except Exception as e:
        print(f"Erro no endpoint avançado: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
