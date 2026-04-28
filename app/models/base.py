from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class HealthResponse(BaseModel):
    status: str
    database: str
    version: Optional[str] = None

class UnitRevenueItem(BaseModel):
    unidade: str
    faturamento: float
    faturamento_convenio: float
    atendimentos: int

class UnitRevenueResponse(BaseModel):
    success: bool
    data: Optional[list[UnitRevenueItem]] = None
    error: Optional[str] = None

class UnitItem(BaseModel):
    cod: str
    nome: str

class UnitsResponse(BaseModel):
    success: bool
    data: Optional[list[UnitItem]] = None
    error: Optional[str] = None

class SLAItem(BaseModel):
    unidade: str
    percentual_no_prazo: float
    total_exames: int
    no_prazo: int
    atrasados: int
    prazo_medio_dias: float

class SLAResponse(BaseModel):
    success: bool
    data: Optional[list[SLAItem]] = None
    error: Optional[str] = None

class ClientsMetrics(BaseModel):
    total_clientes: int
    novos_clientes: int
    genero: dict[str, int]
    faixa_etaria: dict[str, int]
    faixa_etaria_por_genero: dict[str, dict[str, int]]
    cidades: list[dict[str, Any]]
    estado_civil: dict[str, int]

class ClientsResponse(BaseModel):
    success: bool
    data: Optional[ClientsMetrics] = None
    error: Optional[str] = None

class UnitFinancialItem(BaseModel):
    unidade: str
    faturado: float
    custo: float
    liquido: float
    margem: float
    faturado_convenio: float

class FinancialMetrics(BaseModel):
    faturado_total: float
    faturado_convenio: float
    total_geral: float
    custo_total: float
    recebido_total: float
    glosa_total: float
    percentual_glosa: float
    ticket_medio_global: float
    faturamento_por_convenio: Optional[list[dict]] = None
    faturamento_por_unidade: Optional[list[UnitFinancialItem]] = None

class FinancialResponse(BaseModel):
    success: bool
    data: Optional[FinancialMetrics] = None
    error: Optional[str] = None

class DoctorRankingItem(BaseModel):
    nome: str
    crm: Optional[int] = None
    uf: Optional[str] = None
    qtd_pedidos: int
    valor_total: float
    valor_particular: float = 0.0
    valor_convenio: float = 0.0
    ticket_medio: float

class CommercialResponse(BaseModel):
    success: bool
    data: list[DoctorRankingItem] = []
    error: Optional[str] = None

class DetailedFaturamento(BaseModel):
    bruto: float
    desconto: float
    liquido: float
    indice_desconto: float

class DetailedPagamentos(BaseModel):
    PIX: float = 0.0
    CARTAO: float = 0.0
    ESPECIE: float = 0.0
    OUTROS: float = 0.0

class DetailedPacientes(BaseModel):
    novos: int
    recorrentes: int
    total: int

class DetailedMeta(BaseModel):
    projetada: float
    realizado_percent: float

class DetailedMetrics(BaseModel):
    faturamento: DetailedFaturamento
    pagamentos: DetailedPagamentos
    pacientes: DetailedPacientes
    meta: Optional[DetailedMeta] = None

class DetailedResponse(BaseModel):
    success: bool
    data: Optional[DetailedMetrics] = None
    error: Optional[str] = None

# SLA Operacional Models
class SLAFaixaAtraso(BaseModel):
    menos_1h: int = 0
    entre_1_2h: int = 0
    entre_3_5h: int = 0
    entre_6_10h: int = 0
    entre_11_24h: int = 0
    mais_24h: int = 0

class SLAOperacionalItem(BaseModel):
    unidade: str
    unidade_recepcao: Optional[str] = None
    bancada: Optional[str] = None
    aparelho: Optional[str] = None
    liberacao_auto: Optional[str] = None
    quantidade: int
    no_prazo: int
    atrasado: int
    percentual_no_prazo: float
    faixas_atraso: SLAFaixaAtraso

class SLAAmostra(BaseModel):
    unidade: str
    total_exames: int
    novas_amostras: int
    percentual_retrabalho: float

class SLAResumoUnidade(BaseModel):
    unidade: str
    quantidade: int
    no_prazo: int
    atrasado: int
    percentual_no_prazo: float
    faixas_atraso: SLAFaixaAtraso

class SLAMetrics(BaseModel):
    geral: list[SLAOperacionalItem] = []
    por_unidade: list[SLAOperacionalItem] = []
    por_bancada: list[SLAOperacionalItem] = []
    resumo_por_unidade: list[SLAResumoUnidade] = []
    amostras: list[SLAAmostra] = []

class SLAOperacionalResponse(BaseModel):
    success: bool
    data: Optional[SLAMetrics] = None
    error: Optional[str] = None

# Budget Analysis Models
class BudgetSynthetic(BaseModel):
    quantidade_total: int
    valor_total: float
    quantidade_convertidos: int
    valor_convertidos: float
    quantidade_abertos: int
    valor_abertos: float
    taxa_conversao: float

class BudgetUnitItem(BaseModel):
    unidade: str
    quantidade_total: int
    valor_total: float
    quantidade_convertidos: int
    valor_convertidos: float
    quantidade_abertos: int
    valor_abertos: float
    taxa_conversao: float

class BudgetUserItem(BaseModel):
    unidade: str
    usuario: str
    quantidade_total: int
    valor_total: float
    quantidade_convertidos: int
    valor_convertidos: float
    quantidade_abertos: int
    valor_abertos: float
    taxa_conversao: float

class BudgetMetrics(BaseModel):
    sintetico_geral: BudgetSynthetic
    por_unidade: list[BudgetUnitItem] = []
    por_usuario: list[BudgetUserItem] = []

class BudgetResponse(BaseModel):
    success: bool
    data: Optional[BudgetMetrics] = None
    error: Optional[str] = None

class OrcamentoItem(BaseModel):
    orcamento_num: int
    data_cadastro: str
    status: str
    convertido: bool
    osm_num: Optional[int] = None
    usuario: Optional[str] = None
    unidade: str
    pac_reg: int
    pac_nome: str
    pac_categoria: Optional[str] = None
    pac_fone: Optional[str] = None
    pac_nasc: Optional[str] = None
    pac_sexo: Optional[str] = None
    valor_total: float
    total_visitas: Optional[int] = 0
    observacao: Optional[str] = None

class OrcamentosResponse(BaseModel):
    success: bool
    total: int = 0
    data: Optional[List[OrcamentoItem]] = None
    error: Optional[str] = None

class OrcamentoUnidadeItem(BaseModel):
    orcamento_num: int
    data_cadastro: str
    status: str
    convertido: bool          # True quando ORP_OSM_NUM IS NOT NULL
    osm_num: Optional[int] = None  # Número da OS gerada (se convertido)
    usuario: Optional[str] = None
    unidade: str
    pac_reg: int
    pac_nome: str
    pac_fone: Optional[str] = None
    pac_nasc: Optional[str] = None
    pac_sexo: Optional[str] = None
    valor_total: float
    observacao: Optional[str] = None

class OrcamentoUnidadeResponse(BaseModel):
    success: bool
    unidade: Optional[str] = None
    total: int = 0
    data: Optional[List[OrcamentoUnidadeItem]] = None
    error: Optional[str] = None

# Patient Intelligence Models
class PatientDemographics(BaseModel):
    total_pacientes: int
    sexo_distribuicao: Dict[str, float]
    faixa_etaria_distribuicao: Dict[str, float]
    top_cidades: Dict[str, int]

class PatientSocioEconomic(BaseModel):
    ticket_medio_geral: float
    top_pacientes_vip: List[Dict[str, Any]] # List of {nome, valor_total}
    fidelidade_recorrencia: Dict[str, int] # {unicos, retornaram, fieis_3plus}

class PatientPersona(BaseModel):
    descricao: str

class ProcedureByAgeGroup(BaseModel):
    faixa_etaria: str
    top_exames: List[str]

class GeoMarketingConfig(BaseModel):
    cidade: str
    ticket_medio: float

class ChurnRiskPatient(BaseModel):
    paciente: str
    dias_sem_visita: int
    valor_historico: float

class PatientAdvancedAnalytics(BaseModel):
    top_procedimentos_por_idade: List[ProcedureByAgeGroup]
    recencia_media_dias: float
    geomarketing: List[GeoMarketingConfig]
    risco_churn: List[ChurnRiskPatient]

class PatientIntelligenceResponse(BaseModel):
    success: bool
    data: Optional[dict] = None # Using dict to combine all models for simplicity or use nested
    demographics: Optional[PatientDemographics] = None
    socioeconomic: Optional[PatientSocioEconomic] = None
    persona: Optional[PatientPersona] = None
# Strategic Indicators Models (PA CAPA)
class GrowthMetrics(BaseModel):
    valor_atual: float
    valor_anterior: float
    crescimento_percent: float
    meta: float = 0.15

class ConversionMetrics(BaseModel):
    convertidos: int
    total: int
    taxa: float
    meta: float = 0.75

class FinancialFlow(BaseModel):
    faturado: float
    recebido: float
    diferenca: float

class StrategicIndicatorItem(BaseModel):
    particular_convenio: Dict[str, Any] # {particular: float, convenio: float, percent: float}
    crescimento: GrowthMetrics
    operacional: Dict[str, Any] # {pacientes: int, exames: int, ticket_medio: float, exames_por_paciente: float}
    novos_pacientes: Dict[str, int] # {total: int, particular: int, convenio: int}
    conversao_orcamento: ConversionMetrics
    pacientes_perdidos: int
    descontos: Dict[str, Any] # {total_desconto: float, indice_percent: float}
    fluxo_financeiro: FinancialFlow
    faturamento_por_colaborador: float
    ranking_medicos: List[DoctorRankingItem]
    ranking_recepcionistas: List[Dict[str, Any]]
    cortesias: int
    erros_fechamento: int

class StrategicIndicatorsResponse(BaseModel):
    success: bool
    data: Optional[StrategicIndicatorItem] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Perfil de Paciente
# ---------------------------------------------------------------------------

class PacienteListItem(BaseModel):
    pac_reg: int
    nome: str
    nascimento: Optional[str] = None
    sexo: Optional[str] = None
    fone: Optional[str] = None
    ultima_visita: Optional[str] = None
    total_visitas: int = 0
    dias_sem_visita: int = 0
    observacoes: Optional[str] = None

class PacienteListResponse(BaseModel):
    success: bool
    total: int = 0
    page: int = 1
    limit: int = 20
    data: Optional[List[PacienteListItem]] = None
    error: Optional[str] = None

class PacienteIdentidade(BaseModel):
    pac_reg: int
    nome: str
    nascimento: Optional[str] = None
    idade: Optional[int] = None
    sexo: Optional[str] = None
    fone: Optional[str] = None
    data_cadastro: Optional[str] = None
    tempo_como_paciente_dias: Optional[int] = None
    observacoes: Optional[str] = None
    medico_principal: Optional[str] = None
    unidade_principal: Optional[str] = None

class PacienteClassificacao(BaseModel):
    categoria: str  # Novo | Recorrente | Fiel | VIP
    total_visitas: int
    primeira_visita: Optional[str] = None
    ultima_visita: Optional[str] = None
    dias_sem_visita: int
    frequencia_visitas_dias: Optional[int] = None

class PacienteResumoFinanceiro(BaseModel):
    total_gasto: float
    ticket_medio: float
    convenio_principal: str
    valor_particular: float
    valor_convenio: float
    percent_particular: float
    taxa_conversao_orcamento: float = 0.0

class PacienteVisita(BaseModel):
    osm_num: int
    data: str
    unidade: str
    valor: float
    qtd_exames: int

class PacienteExame(BaseModel):
    exame: str
    frequencia: int
    valor_total: float

class PacienteOrcamento(BaseModel):
    orcamento_num: int
    data: str
    status: str
    convertido: bool
    valor_total: float
    dias_em_aberto: Optional[int] = None
    observacao: Optional[str] = None

class PacientePerfilData(BaseModel):
    identidade: PacienteIdentidade
    classificacao: PacienteClassificacao
    financeiro: PacienteResumoFinanceiro
    historico_visitas: List[PacienteVisita]
    exames_mais_realizados: List[PacienteExame]
    orcamentos: List[PacienteOrcamento]

class PacientePerfilResponse(BaseModel):
    success: bool
    data: Optional[PacientePerfilData] = None
    error: Optional[str] = None


class PacientePeriodoItem(PacienteListItem):
    total_gasto_historico: float
    ticket_medio_historico: float
    categoria: str
    unidade_frequente: Optional[str] = None


class PacientePeriodoResponse(BaseModel):
    success: bool
    total: int = 0
    page: int = 1
    limit: int = 20
    data: Optional[List[PacientePeriodoItem]] = None
    error: Optional[str] = None


# Técnico — Laudos comparativo (mês atual x mesmo mês ano anterior)
class LaudosDiaMetrics(BaseModel):
    quantidade: int
    valor: float
    no_prazo: int
    atrasado: int

class LaudosDiaItem(BaseModel):
    data: str  # YYYY-MM-DD do período atual
    atual: LaudosDiaMetrics
    anterior: LaudosDiaMetrics

class LaudosComparativoData(BaseModel):
    dias: List[LaudosDiaItem]
    totais_atual: LaudosDiaMetrics
    totais_anterior: LaudosDiaMetrics

class LaudosComparativoResponse(BaseModel):
    success: bool
    data: Optional[LaudosComparativoData] = None
    error: Optional[str] = None


# --- Modular Comparison / BI v2 ---

class ComparisonValue(BaseModel):
    period_label: str  # e.g. "Atual", "Anterior 1", "Anterior 2" or "2024"
    value: float

class ComparisonMetric(BaseModel):
    field: str
    values: List[ComparisonValue]

class ComparisonPoint(BaseModel):
    virtual_date: str  # Alinhado (e.g. "01-01")
    metrics: List[ComparisonMetric]

class ModularComparisonData(BaseModel):
    points: List[ComparisonPoint]
    totals: List[ComparisonMetric]

class ModularComparisonResponse(BaseModel):
    success: bool
    data: Optional[ModularComparisonData] = None
    error: Optional[str] = None

class DiscoveryEntity(BaseModel):
    name: str
    fields: List[str]

class DiscoveryResponse(BaseModel):
    success: bool
    data: List[DiscoveryEntity]
    error: Optional[str] = None


# --- Enhancements v3 ---

class UnitComparisonMetrics(BaseModel):
    # Consolidated metrics for a unit in a specific period
    period_label: str
    laudos_count: int
    laudos_value: float
    orcamentos_count: int
    orcamentos_conversion: float
    faturamento_total: float
    ticket_medio: float

class UnitComparativeDashboard(BaseModel):
    unidade_nome: str
    unidade_cod: str
    comparativos: List[UnitComparisonMetrics]
    diarios: Optional[ModularComparisonData] = None # Optional daily breakdown

class UnitComparativeResponse(BaseModel):
    success: bool
    data: Optional[UnitComparativeDashboard] = None
    error: Optional[str] = None

class RankingAgent(BaseModel):
    nome: str
    period_label: str
    rank: int
    rank_delta: int = 0 # Difference from previous period
    valor: float
    volume: int

class RankingComparisonData(BaseModel):
    entity_type: str # "medicos" or "recepcionistas"
    agents: List[RankingAgent]

class RankingComparisonResponse(BaseModel):
    success: bool
    data: Optional[RankingComparisonData] = None
    error: Optional[str] = None

class ProjectionPoint(BaseModel):
    label: str # "Realizado", "Projetado (Conservador)", "Projetado (Otimista)"
    valor: float

class ProjectionResult(BaseModel):
    entity: str
    last_update: str
    current_value: float
    projections: List[ProjectionPoint]
    confidence_score: float

class ProjectionResponse(BaseModel):
    success: bool
    data: Optional[ProjectionResult] = None
    error: Optional[str] = None

# --- Exam Deep Dive / Detail ---

class ExamDetailSummary(BaseModel):
    cod: str
    nome: str
    qtd_total: int
    faturado_bruto: float
    faturado_liquido: float
    ticket_medio: float
    prazo_medio_dias: float

class ExamInsightItem(BaseModel):
    nome: str
    qtd: int
    valor: float

class ExamPatientItem(BaseModel):
    data: str
    paciente: str
    osm: int
    convenio: str
    valor: float

class ExamDetailData(BaseModel):
    resumo: ExamDetailSummary
    ranking_medicos: List[ExamInsightItem]
    ranking_unidades: List[ExamInsightItem]
    ranking_convenios: List[ExamInsightItem]
    ultimos_pacientes: List[ExamPatientItem]

class ExamDetailResponse(BaseModel):
    success: bool
    data: Optional[ExamDetailData] = None
    error: Optional[str] = None

class MonthlyExecutionItem(BaseModel):
    month_year: Optional[str] = None
    date: Optional[str] = None
    revenue: float
    patients: int
    ticket_avg: float

class MonthlyExecutionResponse(BaseModel):
    success: bool
    data: Optional[List[MonthlyExecutionItem]] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Almoxarifado Models
# ---------------------------------------------------------------------------

class AlmoxarifadoKPIs(BaseModel):
    total_itens: int = 0
    total_ativos: int = 0
    com_saldo: int = 0
    sem_saldo: int = 0
    curva_a: int = 0
    controlados: int = 0
    pereceveis: int = 0
    valor_total_estoque: float = 0.0
    abaixo_ressuprimento: int = 0
    lotes_vencidos: int = 0
    lotes_vencendo_30d: int = 0

class AlmoxarifadoKPIsResponse(BaseModel):
    success: bool
    data: Optional[AlmoxarifadoKPIs] = None
    sub_almoxarifados: List[str] = []
    error: Optional[str] = None

class StockItem(BaseModel):
    cod: int
    descricao: str
    descricao_resumida: str
    saldo_atual: float
    estoque_maximo: float
    ponto_ressuprimento: float
    ponto_seguranca: float
    preco_medio: float
    preco_ult_entrada: float
    sub_almox: str
    unidade_medida: str
    curva_abc: str
    perecivel: str
    controlado: str
    criticidade: str
    ultima_entrada: Optional[str] = None
    ultima_saida: Optional[str] = None
    consumo_medio: float
    status_estoque: str  # ok | atencao | alerta | critico
    valor_total: float

class StockCatalogResponse(BaseModel):
    success: bool
    total: int = 0
    page: int = 1
    limit: int = 50
    data: Optional[List[StockItem]] = None
    error: Optional[str] = None

class LotItem(BaseModel):
    mat_cod: int
    material: str
    sub_almox: str
    lote_num: str
    data_entrada: Optional[str] = None
    data_validade: Optional[str] = None
    quantidade: float
    saldo_lote: float
    procedencia: str
    nfe_num: Optional[int] = None
    status: str
    unidade: str

class LotHistoryResponse(BaseModel):
    success: bool
    total: int = 0
    page: int = 1
    limit: int = 50
    data: Optional[List[LotItem]] = None
    error: Optional[str] = None

class ExpiryAlertItem(BaseModel):
    mat_cod: int
    material: str
    sub_almox: str
    lote_num: str
    data_validade: Optional[str] = None
    saldo: float
    unidade: str
    nivel_alerta: str  # vencido | critico | alerta | atencao
    dias_para_vencer: int

class ExpiryAlertResumo(BaseModel):
    vencidos: int = 0
    criticos: int = 0
    alertas: int = 0
    atencao: int = 0

class ExpiryAlertsResponse(BaseModel):
    success: bool
    total: int = 0
    resumo: Optional[ExpiryAlertResumo] = None
    data: Optional[List[ExpiryAlertItem]] = None
    error: Optional[str] = None

# --- Solicitações de Almoxarifado ---

class SolicitacaoItem(BaseModel):
    cod: int
    descricao: str
    qtde: float
    unidade: str
    pendente: float
    prioridade: Optional[str] = None

class SolicitacaoHeader(BaseModel):
    serie: int
    num: int
    data: str
    tipo: str
    setor: str
    solicitante: str
    status: str
    observacao: Optional[str] = None
    itens: List[SolicitacaoItem] = []

class SolicitacoesPendentesResponse(BaseModel):
    success: bool
    total: int = 0
    data: Optional[List[SolicitacaoHeader]] = None
    error: Optional[str] = None

# --- Módulo Financeiro ---

class ParcelaItem(BaseModel):
    num_parcela: int = 0
    vencimento: str = ""
    valor: float = 0.0
    status: str = "A"
    data_pagamento: Optional[str] = None

class CompromissoHeader(BaseModel):
    serie: int
    num: int
    credor: str = "(Sem Credor)"  # nullable no banco
    data_registro: str = ""
    empresa: int = 0
    valor_total: float = 0.0    # pode ser NULL se ipg_valor for NULL
    total_parcelas: int = 1
    status_consolidado: str = "Aberto"
    observacao: Optional[str] = None
    parcelas: List[ParcelaItem] = []

class CompromissosResponse(BaseModel):
    success: bool
    total: int = 0
    data: Optional[List[CompromissoHeader]] = None
    error: Optional[str] = None

class FluxoCaixaItem(BaseModel):
    categoria: str
    tipo: str  # RECEITA | DESPESA
    valor: float
    percentual: float = 0.0

class FluxoCaixaResumo(BaseModel):
    total_receitas: float
    total_despesas: float
    saldo_operacional: float
    itens: List[FluxoCaixaItem] = []

class FluxoCaixaResponse(BaseModel):
    success: bool
    data: Optional[FluxoCaixaResumo] = None
    error: Optional[str] = None

class FluxoDetalhadoItem(BaseModel):
    categoria: str
    valor: float
    percentual: float = 0.0

class FluxoDetalhadoSecao(BaseModel):
    total: float
    percentual_receita: float = 0.0
    itens: List[FluxoDetalhadoItem] = []

class FluxoCaixaDetalhadoResumo(BaseModel):
    periodo_de: str
    periodo_ate: str
    receitas: FluxoDetalhadoSecao
    despesas: FluxoDetalhadoSecao
    resultado_operacional: float
    resultado_percentual: float
    # Campos extras para replicar o relatório legado
    nao_operacional: Optional[FluxoDetalhadoSecao] = None
    superavit_deficit: Optional[float] = None
    saldo_inicial: Optional[float] = None
    saldo_final: Optional[float] = None

class FluxoCaixaDetalhadoResponse(BaseModel):
    success: bool
    data: Optional[FluxoCaixaDetalhadoResumo] = None
    error: Optional[str] = None
