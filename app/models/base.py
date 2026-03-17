from pydantic import BaseModel
from typing import List, Optional, Dict, Any

class HealthResponse(BaseModel):
    status: str
    database: str
    version: Optional[str] = None

class UnitRevenueItem(BaseModel):
    unidade: str
    faturamento: float
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


# Técnico — Laudos comparativo (mês atual x mesmo mês ano anterior)
class LaudosDiaMetrics(BaseModel):
    quantidade: int
    valor: float
    no_prazo: int
    atrasado: int

class LaudosDiaItem(BaseModel):
    dia: int
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
