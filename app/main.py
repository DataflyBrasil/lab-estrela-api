from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
from datetime import date, datetime, timedelta
import pandas as pd
import logging

from .database import get_db_connection, release_connection, test_connection, current_db_id
from .models.base import (
    HealthResponse, UnitRevenueResponse, UnitRevenueItem,
    SLAResponse, SLAItem, ClientsResponse, ClientsMetrics,
    FinancialResponse, FinancialMetrics,
    DoctorRankingItem, CommercialResponse,
    DetailedResponse, SLAMetrics, SLAOperacionalResponse,
    BudgetResponse, OrcamentosResponse, OrcamentoUnidadeResponse,
    PatientIntelligenceResponse,
    PatientDemographics, PatientSocioEconomic, PatientAdvancedAnalytics,
    StrategicIndicatorsResponse, UnitsResponse,
    LaudosComparativoResponse, LaudosComparativoData,
    PacienteListResponse, PacienteListItem,
    PacientePerfilResponse, PacientePerfilData,
    PacienteIdentidade, PacienteClassificacao, PacienteResumoFinanceiro,
    PacienteVisita, PacienteExame, PacienteOrcamento,
    ModularComparisonResponse, DiscoveryResponse,
    UnitComparativeResponse, RankingComparisonResponse, ProjectionResponse,
    ExamDetailResponse, ExamDetailData, ExamDetailSummary, ExamInsightItem, ExamPatientItem,
    PacientePeriodoResponse, PacientePeriodoItem,
    MonthlyExecutionResponse
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
from .services.cache import analytics_cache
from .services.comparison import (
    get_comparison_metadata, get_laudos_comparison_v2,
    get_orcamentos_comparison, get_financeiro_comparison,
    get_unit_comparative_dashboard, get_ranking_comparison,
    get_performance_projections
)
from .services.metas import get_monthly_execution
from .ai.api.router import router as ai_router

app = FastAPI(title="Laboratório Estrela API", version="2.0.0")

# Habilitar CORS
app.add_middleware(
    CORSMiddleware,
    # allow_origins=["http://localhost:3000", "http://localhost:3001", "*"],
    allow_origins=["https://labestrelabi.com.br", "https://app.labestrelabi.com.br"],
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
        # Seleção automática de banco se unidade for Paulo Afonso
        db_id = current_db_id.get()
        if unidade and "PAULO AFONSO" in unidade.upper():
            db_id = "2"
            current_db_id.set(db_id)

        cache_key = f"management_indicators_{db_id}_{start_date}_{end_date}_{unidade}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        indicators = get_strategic_indicators(cursor, start_date, end_date, unidade)
        
        release_connection(conn)
        response = StrategicIndicatorsResponse(success=True, data=indicators)
        analytics_cache[cache_key] = response
        return response
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
        
        release_connection(conn)
        return UnitsResponse(success=True, data=units)
    except Exception as e:
        return UnitsResponse(success=False, error=str(e))

@app.get("/metas/execucao", response_model=MonthlyExecutionResponse, tags=["Management"])
def metas_execucao(
    unidade: Optional[str] = Query(None, description="Nome da unidade específica")
):
    """
    Retorna a execução real mensal da unidade (faturamento, pacientes) no ano atual.
    """
    try:
        db_id = current_db_id.get()
        # Auto-switch de região baseado no nome da unidade (se estiver no contexto errado)
        if unidade and "PAULO AFONSO" in unidade.upper():
            db_id = "2"
            current_db_id.set("2")
            print(f"Auto-switch: Detectado Paulo Afonso na unidade '{unidade}'. Mudando para DB 2.")
        
        # Caching
        cache_key = f"metas_execucao_{db_id}_{unidade}"
        if cache_key in analytics_cache:
            return MonthlyExecutionResponse(success=True, data=analytics_cache[cache_key])

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        execution = get_monthly_execution(cursor, unidade)
        
        release_connection(conn)
        
        # Guardar no cache
        analytics_cache[cache_key] = execution
        
        return MonthlyExecutionResponse(success=True, data=execution)
    except Exception as e:
        print(f"Erro em /metas/execucao: {e}")
        return MonthlyExecutionResponse(success=False, error=str(e))

@app.get("/metas/execucao/diaria", response_model=MonthlyExecutionResponse, tags=["Management"])
def metas_execucao_diaria(
    unidade: Optional[str] = Query(None, description="Nome da unidade específica")
):
    """
    Retorna a execução real diária da unidade no mês atual.
    """
    try:
        db_id = current_db_id.get()
        if unidade and "PAULO AFONSO" in unidade.upper():
            db_id = "2"
            current_db_id.set("2")

        cache_key = f"metas_execucao_diaria_{db_id}_{unidade}"
        if cache_key in analytics_cache:
            return MonthlyExecutionResponse(success=True, data=analytics_cache[cache_key])

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        
        from .services.metas import get_daily_execution
        execution = get_daily_execution(cursor, unidade)
        
        release_connection(conn)
        analytics_cache[cache_key] = execution
        
        return MonthlyExecutionResponse(success=True, data=execution)
    except Exception as e:
        print(f"Erro em /metas/execucao/diaria: {e}")
        return MonthlyExecutionResponse(success=False, error=str(e))

@app.get("/unidades/faturamento", response_model=UnitRevenueResponse, tags=["Financeiro"])
def get_unit_revenue(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
):
    try:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=14)).date()
        if not end_date:
            end_date = datetime.now().date()

        cache_key = f"unit_revenue_{current_db_id.get()}_{start_date}_{end_date}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        # Queries rodam em paralelo internamente (ver analytics.py)
        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        df_faturamento, df_atendimentos = get_unit_revenue_data(cursor, start_date, end_date)
        release_connection(conn)

        if df_atendimentos.empty:
            return UnitRevenueResponse(success=True, data=[])

        analytics_result = aggregate_unit_revenue_python(df_faturamento, df_atendimentos)

        data = [
            UnitRevenueItem(
                unidade=str(r['unidade']).strip(),
                faturamento=float(r['faturamento']),
                faturamento_convenio=float(r['faturamento_convenio']),
                atendimentos=int(r['atendimentos'])
            ) for r in analytics_result
        ]

        response = UnitRevenueResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response

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
        # Filtros de data
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        cache_key = f"sla_exame_{current_db_id.get()}_{filter_type}_{start_date}_{end_date}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)

        # Busca dados brutos
        df = get_exam_sla_data(cursor, start_date, end_date, filter_type)
        release_connection(conn)

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

        response = SLAResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response

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
        release_connection(conn)

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
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)

        # Busca dados financeiros
        df_faturamento, df_caixa, total_atendimentos, valor_mte_final, valor_ipc_final, df_units_convenio, df_diario = get_financial_analytics_data(cursor, start_date, end_date)
        release_connection(conn)

        # Processa via Python
        analytics_result = process_financial_analytics_python(df_faturamento, df_caixa, total_atendimentos, valor_mte_final, valor_ipc_final, df_units_convenio, df_diario)

        response = FinancialResponse(success=True, data=analytics_result)
        return response

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
        release_connection(conn)

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
        mte_totals, df_payments, df_patients, valor_convenio_faturado = get_detailed_finance_data(cursor, start_date, end_date)
        release_connection(conn)

        # Processa via Python
        analytics_result = process_detailed_finance_python(mte_totals, df_payments, df_patients, valor_convenio_faturado)
        
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
        # Default: último mês
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        cache_key = f"operacional_sla_{current_db_id.get()}_{start_date}_{end_date}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)

        from .services.sla import get_sla_data, process_sla_operational

        df_sla, df_amostras = get_sla_data(cursor, str(start_date), str(end_date))
        release_connection(conn)

        analytics_result = process_sla_operational(df_sla, df_amostras)

        response = SLAOperacionalResponse(
            success=True,
            data=SLAMetrics(**analytics_result)
        )
        analytics_cache[cache_key] = response
        return response

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
        release_connection(conn)
        
        # 2. Processar métricas
        metrics = process_budget_metrics(df_budgets)
        
        return {
            "success": True, 
            "data": metrics
        }
    except Exception as e:
        print(f"Erro no endpoint de orçamentos: {e}")
        return {"success": False, "error": str(e)}

@app.get("/orcamentos/pacientes", tags=["Comercial"])
@app.get("/orcamentos-pacientes", tags=["Comercial"])
def get_orcamentos_pacientes_list(
    start_date: Optional[date] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Data final (YYYY-MM-DD)"),
):
    """
    Retorna a lista de orçamentos do período com os dados de cada paciente.
    """
    try:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        conn = get_db_connection()
        cursor = conn.cursor()

        from .services.budget import get_orcamentos_pacientes as _get_orcamentos
        items = _get_orcamentos(cursor, str(start_date), str(end_date))
        release_connection(conn)

        # Usando dicionário direto para evitar problemas de validação de modelo no 404
        return {
            "success": True,
            "total": len(items),
            "data": items
        }

    except Exception as e:
        print(f"Erro no endpoint /orcamentos/pacientes: {e}")
        return {"success": False, "total": 0, "error": str(e)}


@app.get("/orcamentos/unidade", response_model=OrcamentoUnidadeResponse, tags=["Comercial"])
def get_orcamentos_por_unidade(
    unidade: str = Query(..., description="Nome exato da unidade"),
    start_date: Optional[date] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Data final (YYYY-MM-DD)"),
):
    """
    Retorna os orçamentos emitidos para uma unidade específica no período,
    com dados completos do paciente e indicação se o orçamento foi convertido em OS.
    """
    try:
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).date()
        if not end_date:
            end_date = datetime.now().date()

        conn = get_db_connection()
        cursor = conn.cursor()

        from .services.budget import get_orcamentos_por_unidade as _get_orcamentos_unidade
        items = _get_orcamentos_unidade(cursor, unidade, str(start_date), str(end_date))
        release_connection(conn)

        return OrcamentoUnidadeResponse(
            success=True,
            unidade=unidade,
            total=len(items),
            data=items,
        )

    except Exception as e:
        print(f"Erro no endpoint /orcamentos/unidade: {e}")
        return OrcamentoUnidadeResponse(success=False, error=str(e))


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
        release_connection(conn)
        
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
        release_connection(conn)
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
        release_connection(conn)
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
        release_connection(conn)
        return data
    except Exception as e:
        print(f"Erro no endpoint avançado: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tecnico/laudos/comparativo", response_model=LaudosComparativoResponse, tags=["Técnico"])
def get_laudos_comparativo(
    start_date: Optional[date] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Data final (YYYY-MM-DD)"),
    ano: Optional[int] = Query(None, description="Ano (compatibilidade legada com frontend)"),
    mes: Optional[int] = Query(None, description="Mês (compatibilidade legada com frontend)"),
):
    """
    Retorna laudos liberados dia a dia no período informado,
    comparando com o mesmo período do ano anterior.
    Aceita start_date/end_date ou ano/mes (range completo do mês).
    """
    import calendar
    from .services.tecnico import get_laudos_comparativo_data, build_laudos_comparativo

    hoje = datetime.now()

    # Compatibilidade retroativa: ano/mes → primeiro e último dia do mês
    if ano and mes and not start_date and not end_date:
        last_day = calendar.monthrange(ano, mes)[1]
        start_date = date(ano, mes, 1)
        end_date = date(ano, mes, last_day)

    if not start_date:
        start_date = hoje.replace(day=1).date()
    if not end_date:
        end_date = hoje.date()

    start_str = str(start_date)
    end_str   = str(end_date)

    cache_key = f"laudos_comparativo_{current_db_id.get()}_{start_str}_{end_str}"
    if cache_key in analytics_cache:
        return analytics_cache[cache_key]

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        df_atual, df_anterior = get_laudos_comparativo_data(cursor, start_str, end_str)
        release_connection(conn)

        payload = build_laudos_comparativo(df_atual, df_anterior)
        response = LaudosComparativoResponse(
            success=True,
            data=LaudosComparativoData(**payload),
        )
        analytics_cache[cache_key] = response
        return response

    except Exception as e:
        return LaudosComparativoResponse(success=False, error=str(e))


@app.get("/pacientes/busca", response_model=PacienteListResponse, tags=["Pacientes"])
def buscar_pacientes(
    nome: str = Query(..., min_length=2, description="Nome (parcial) do paciente"),
    page: int = Query(1, ge=1, description="Página"),
    limit: int = Query(20, ge=1, le=100, description="Resultados por página"),
):
    """
    Busca pacientes por nome (LIKE %nome%).
    Retorna lista paginada com última visita, total de visitas e dias sem visita.
    """
    try:
        db_id = current_db_id.get()
        cache_key = f"paciente_busca_{db_id}_{nome}_{page}_{limit}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        from .services.patient_profile import search_pacientes
        result = search_pacientes(nome, page, limit)
        items = [PacienteListItem(**i) for i in result["items"]]
        
        response = PacienteListResponse(
            success=True,
            total=result["total"],
            page=result["page"],
            limit=result["limit"],
            data=items,
        )
        
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return PacienteListResponse(success=False, error=str(e))


@app.get("/pacientes/{pac_reg}/perfil", response_model=PacientePerfilResponse, tags=["Pacientes"])
def get_perfil_paciente(pac_reg: int):
    """
    Retorna o perfil completo de um paciente com insights estratégicos e caching.
    """
    db_id = current_db_id.get()
    cache_key = f"perfil_{pac_reg}_{db_id}"
    
    if cache_key in analytics_cache:
        return PacientePerfilResponse(success=True, data=analytics_cache[cache_key])

    try:
        from .services.patient_profile import get_paciente_perfil
        perfil = get_paciente_perfil(pac_reg)
        
        data = PacientePerfilData(
            identidade=PacienteIdentidade(**perfil["identidade"]),
            classificacao=PacienteClassificacao(**perfil["classificacao"]),
            financeiro=PacienteResumoFinanceiro(**perfil["financeiro"]),
            historico_visitas=[PacienteVisita(**v) for v in perfil["historico_visitas"]],
            exames_mais_realizados=[PacienteExame(**e) for e in perfil["exames_mais_realizados"]],
            orcamentos=[PacienteOrcamento(**o) for o in perfil["orcamentos"]],
        )
        
        analytics_cache[cache_key] = data
        return PacientePerfilResponse(success=True, data=data)
    except Exception as e:
        return PacientePerfilResponse(success=False, error=str(e))


@app.get("/pacientes/periodo", response_model=PacientePeriodoResponse, tags=["Pacientes"])
def get_pacientes_periodo(
    start_date: Optional[date] = Query(None, description="Data inicial (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Data final (YYYY-MM-DD)"),
    full_scan: bool = Query(False, description="Se True, ignora as datas e busca todos os pacientes"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Retorna listagem estratégica de pacientes (LTV, Categoria) filtrada por período ou varredura completa.
    """
    try:
        db_id = current_db_id.get()
        cache_key = f"paciente_periodo_{db_id}_{start_date}_{end_date}_{full_scan}_{page}_{limit}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        from .services.patient_profile import get_pacientes_estrategico
        result = get_pacientes_estrategico(
            str(start_date) if start_date else None, 
            str(end_date) if end_date else None, 
            page, 
            limit, 
            full_scan
        )
        
        items = [PacientePeriodoItem(**i) for i in result["items"]]
        
        response = PacientePeriodoResponse(
            success=True,
            total=result["total"],
            page=result["page"],
            limit=result["limit"],
            data=items,
        )
        
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return PacientePeriodoResponse(success=False, error=str(e))


# --- Modular Comparison / BI v2 ---

@app.get("/comparativo/metadados", response_model=DiscoveryResponse, tags=["Comparativo"])
def get_metadata():
    try:
        data = get_comparison_metadata()
        return DiscoveryResponse(success=True, data=data)
    except Exception as e:
        return DiscoveryResponse(success=False, data=[], error=str(e))

@app.get("/comparativo/laudos_v2", response_model=ModularComparisonResponse, tags=["Comparativo"])
def get_laudos_comparison(
    start_date: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Data final (YYYY-MM-DD)"),
    years_back: int = Query(1, ge=0, le=5, description="Anos para trás"),
    granularity: str = Query("diario", pattern="^(diario|mensal|anual)$", description="Granularidade: diario, mensal, anual")
):
    try:
        db_id = current_db_id.get()
        cache_key = f"laudos_v2_{db_id}_{start_date}_{end_date}_{years_back}_{granularity}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        data = get_laudos_comparison_v2(cursor, str(start_date), str(end_date), years_back, granularity)
        release_connection(conn)
        
        response = ModularComparisonResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return ModularComparisonResponse(success=False, error=str(e))

@app.get("/comparativo/orcamentos", response_model=ModularComparisonResponse, tags=["Comparativo"])
def get_budgets_comparison(
    start_date: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Data final (YYYY-MM-DD)"),
    years_back: int = Query(1, ge=0, le=5, description="Anos para trás"),
    granularity: str = Query("diario", pattern="^(diario|mensal|anual)$", description="Granularidade: diario, mensal, anual")
):
    try:
        db_id = current_db_id.get()
        cache_key = f"orcamentos_comparativo_{db_id}_{start_date}_{end_date}_{years_back}_{granularity}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        data = get_orcamentos_comparison(cursor, str(start_date), str(end_date), years_back, granularity)
        release_connection(conn)
        
        response = ModularComparisonResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return ModularComparisonResponse(success=False, error=str(e))

@app.get("/comparativo/financeiro", response_model=ModularComparisonResponse, tags=["Comparativo"])
def get_financial_comparison(
    start_date: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Data final (YYYY-MM-DD)"),
    years_back: int = Query(1, ge=0, le=5, description="Anos para trás"),
    granularity: str = Query("diario", pattern="^(diario|mensal|anual)$", description="Granularidade: diario, mensal, anual")
):
    try:
        db_id = current_db_id.get()
        cache_key = f"financeiro_comparativo_{db_id}_{start_date}_{end_date}_{years_back}_{granularity}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        data = get_financeiro_comparison(cursor, str(start_date), str(end_date), years_back, granularity)
        release_connection(conn)
        
        response = ModularComparisonResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return ModularComparisonResponse(success=False, error=str(e))


@app.get("/comparativo/unidade", response_model=UnitComparativeResponse, tags=["Comparativo"])
def get_unit_dashboard(
    unidade: str = Query(..., description="Código da unidade (str_cod)"),
    start_date: date = Query(..., description="Data inicial"),
    end_date: date = Query(..., description="Data final"),
    years_back: int = Query(1, ge=0, le=5)
):
    try:
        db_id = current_db_id.get()
        cache_key = f"unit_dashboard_{db_id}_{unidade}_{start_date}_{end_date}_{years_back}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        data = get_unit_comparative_dashboard(cursor, unidade, str(start_date), str(end_date), years_back)
        release_connection(conn)
        
        response = UnitComparativeResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return UnitComparativeResponse(success=False, error=str(e))

@app.get("/comparativo/ranking", response_model=RankingComparisonResponse, tags=["Comparativo"])
def get_ranking_comp(
    entity_type: str = Query(..., pattern="^(medicos|recepcionistas)$"),
    start_date: date = Query(..., description="Data inicial"),
    end_date: date = Query(..., description="Data final"),
    years_back: int = Query(1, ge=0, le=5),
    unidade: Optional[str] = Query(None, description="Opcional: Filtrar por unidade")
):
    try:
        db_id = current_db_id.get()
        cache_key = f"ranking_comparativo_{db_id}_{entity_type}_{start_date}_{end_date}_{years_back}_{unidade}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        data = get_ranking_comparison(cursor, entity_type, str(start_date), str(end_date), years_back, unidade)
        release_connection(conn)
        
        response = RankingComparisonResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return RankingComparisonResponse(success=False, error=str(e))

@app.get("/comparativo/projecao", response_model=ProjectionResponse, tags=["Comparativo"])
def get_projections_route(
    entity: str = Query("faturamento", description="Entidade para projeção")
):
    try:
        db_id = current_db_id.get()
        cache_key = f"projecao_{db_id}_{entity}"
        if cache_key in analytics_cache:
            return analytics_cache[cache_key]

        conn = get_db_connection()
        cursor = conn.cursor(as_dict=True)
        data = get_performance_projections(cursor, entity)
        release_connection(conn)
        
        response = ProjectionResponse(success=True, data=data)
        analytics_cache[cache_key] = response
        return response
    except Exception as e:
        return ProjectionResponse(success=False, error=str(e))

@app.get("/exames/{exame_cod}/detalhes", response_model=ExamDetailResponse, tags=["Exames"])
def get_detalhes_exame(
    exame_cod: str,
    start_date: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Data final (YYYY-MM-DD)"),
    tpcod: str = Query('LB', description="Tipo do exame (LB, etc)")
):
    """
    Retorna o aprofundamento de um exame específico com insights estratégicos.
    """
    db_id = current_db_id.get()
    cache_key = f"exam_detail_{exame_cod}_{start_date}_{end_date}_{tpcod}_{db_id}"
    
    if cache_key in analytics_cache:
        return ExamDetailResponse(success=True, data=analytics_cache[cache_key])

    try:
        from .services.exam_detail import get_exam_details
        details = get_exam_details(exame_cod, str(start_date), str(end_date), tpcod)
        
        data = ExamDetailData(
            resumo=ExamDetailSummary(**details["resumo"]),
            ranking_medicos=[ExamInsightItem(**r) for r in details["ranking_medicos"]],
            ranking_unidades=[ExamInsightItem(**r) for r in details["ranking_unidades"]],
            ranking_convenios=[ExamInsightItem(**r) for r in details["ranking_convenios"]],
            ultimos_pacientes=[ExamPatientItem(**r) for r in details["ultimos_pacientes"]]
        )
        
        analytics_cache[cache_key] = data
        return ExamDetailResponse(success=True, data=data)
    except Exception as e:
        print(f"Erro no endpoint /exames/detalhes: {e}")
        return ExamDetailResponse(success=False, error=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
