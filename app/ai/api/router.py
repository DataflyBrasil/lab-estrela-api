from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from app.ai.orchestrator.client import ai_service
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    history: Optional[List[Dict[str, Any]]] = None

class ChatResponse(BaseModel):
    response: str
    session_id: str
    conversation_id: Optional[str] = None
    usage: Optional[Dict[str, int]] = None

@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(request: ChatRequest):
    """
    Chat with the AI Analytics Engine.
    Supports conversation history via session_id.
    """

    try:
        result = await ai_service.chat(
            message=request.message,
            session_id=request.session_id,
            user_id=request.user_id,
            history=request.history
        )
        return ChatResponse(**result)
    except Exception as e:
        logger.error(f"Error processing chat request: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Análise de Orçamentos em Aberto ──────────────────────────────────────────

class GrupoPaciente(BaseModel):
    """Dados de um paciente agrupado — calculados pelo frontend com score de conversão."""
    nome: str                          # Primeiro nome + inicial do sobrenome ("Maria S.")
    pac_categoria: Optional[str] = None  # VIP | Fiel | Novo | None
    total_orcamentos: int              # Quantos orçamentos em aberto
    valor_total_acumulado: float       # Soma dos valores em aberto
    max_dias: int                      # Orçamento mais antigo (dias)
    min_dias: int                      # Orçamento mais recente (dias)
    tem_telefone: bool
    conversion_score: int              # 0-100, calculado pelo algoritmo do frontend
    conversion_reason: str             # Razão legível ("paciente VIP · alto valor acumulado")
    unidades: List[str] = []

class OrcamentosAnalyzeRequest(BaseModel):
    total_em_aberto: float
    qtd_em_aberto: int
    ticket_medio: float
    media_dias_em_aberto: float
    grupos_pacientes: List[GrupoPaciente]  # Top 20 por conversion_score

class AcaoPriorizada(BaseModel):
    nome: str
    valor: float
    razao: str
    script: str

class OrcamentosAnalyzeResponse(BaseModel):
    resumo: str
    acoes_hoje: List[AcaoPriorizada]
    acoes_semana: List[AcaoPriorizada]
    nao_priorizar: List[str]
    insight_geral: str
    session_id: str

@router.post("/orcamentos/analyze", response_model=OrcamentosAnalyzeResponse)
async def analyze_open_budgets(request: OrcamentosAnalyzeRequest):
    """
    Analisa orçamentos em aberto por paciente e retorna um plano de ação priorizado.
    Age como um analista que conhece o negócio — quem ligar, quando e com qual script.
    """
    try:
        # Formata a lista de pacientes de forma estruturada para o prompt
        pacientes_txt = "\n".join([
            f"  {i+1}. {p.nome} | Score {p.conversion_score}/100 | {p.pac_categoria or 'Novo'} | "
            f"R$ {p.valor_total_acumulado:,.2f} ({p.total_orcamentos} orç.) | "
            f"{'⬤ ' + str(p.min_dias) + '-' + str(p.max_dias) + 'd em aberto'} | "
            f"{'✅ telefone' if p.tem_telefone else '❌ sem telefone'} | "
            f"Motivo do score: {p.conversion_reason}"
            for i, p in enumerate(request.grupos_pacientes[:20])
        ])

        prompt = f"""Você é um analista de vendas especializado em clínicas de saúde. Sua missão é transformar dados de orçamentos em aberto em um plano de ação concreto para a equipe comercial — como faria um piloto experiente antes do voo.

CONTEXTO GERAL:
- {request.qtd_em_aberto} orçamentos em aberto · R$ {request.total_em_aberto:,.2f} em potencial
- Ticket médio: R$ {request.ticket_medio:,.2f} · Média de {request.media_dias_em_aberto:.1f} dias sem conversão

PACIENTES RANKEADOS POR POTENCIAL DE CONVERSÃO (score calculado pelo sistema):
{pacientes_txt}

O score considera: valor dos orçamentos, histórico do paciente (VIP/Fiel/Novo), quantidade de orçamentos em aberto (engajamento) e dias em aberto (frescor do interesse). Quanto mais recente e mais valioso, maior o score.

SUA ANÁLISE (aja como analista, não como assistente genérico):
- "Hoje" = score alto + poucos dias em aberto = janela quente, não pode esperar
- "Esta semana" = score médio ou bom, mas menos urgente
- "Não priorizar" = muito antigo (>30d), sem telefone, ou score muito baixo
- Scripts devem variar por perfil:
  · VIP: tom exclusivo, agilidade, reconhecimento ("como nosso paciente especial...")
  · Fiel: reconhecimento e continuidade ("já que você nos conhece bem...")
  · Novo: acolhimento, segurança, facilidade ("aqui é simples e rápido...")

Responda EXATAMENTE neste JSON (sem markdown, sem texto fora do JSON):
{{
  "resumo": "2 frases: situação atual + maior oportunidade imediata com número concreto",
  "acoes_hoje": [
    {{
      "nome": "nome do paciente",
      "valor": 0.00,
      "razao": "por que este paciente agora — seja específico com os dados",
      "script": "script personalizado ao perfil, 2-3 linhas naturais de fala"
    }}
  ],
  "acoes_semana": [
    {{
      "nome": "nome do paciente",
      "valor": 0.00,
      "razao": "por que vale o esforço, mas sem urgência imediata",
      "script": "script personalizado ao perfil, 2-3 linhas"
    }}
  ],
  "nao_priorizar": [
    "Nome — motivo direto em meia frase"
  ],
  "insight_geral": "1 observação estratégica que a equipe não enxerga nos dados brutos"
}}"""

        result = await ai_service.chat(
            message=prompt,
            session_id=None,
            user_id="system_orcamentos_analysis",
            history=[]
        )

        import json, re
        raw = result.get("response", "")
        json_match = re.search(r'\{.*\}', raw, re.DOTALL)
        if json_match:
            parsed = json.loads(json_match.group())
        else:
            parsed = {
                "resumo": raw[:300] if raw else "Não foi possível gerar análise.",
                "acoes_hoje": [],
                "acoes_semana": [],
                "nao_priorizar": [],
                "insight_geral": "Analise os dados manualmente para identificar oportunidades.",
            }

        def parse_acoes(lista: list) -> List[AcaoPriorizada]:
            result_list = []
            for item in (lista or []):
                if isinstance(item, dict):
                    result_list.append(AcaoPriorizada(
                        nome=item.get("nome", "—"),
                        valor=float(item.get("valor", 0)),
                        razao=item.get("razao", ""),
                        script=item.get("script", ""),
                    ))
            return result_list

        return OrcamentosAnalyzeResponse(
            resumo=parsed.get("resumo", ""),
            acoes_hoje=parse_acoes(parsed.get("acoes_hoje", [])),
            acoes_semana=parse_acoes(parsed.get("acoes_semana", [])),
            nao_priorizar=[str(x) for x in (parsed.get("nao_priorizar") or [])],
            insight_geral=parsed.get("insight_geral", ""),
            session_id=result.get("session_id", ""),
        )
    except Exception as e:
        logger.error(f"Error analyzing open budgets: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
