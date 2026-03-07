"""
Ferramenta analítica para comparar métricas entre dois períodos.
Identifica variações, top contributors e maiores mudanças.
"""

from typing import Dict, Optional, List
from datetime import datetime
from app.ai.tools.query_metrics import query_metrics_tool
import logging

logger = logging.getLogger(__name__)

def compare_periods(
    metric_name: str,
    period1_start: str,
    period1_end: str,
    period2_start: str,
    period2_end: str,
    dimension: Optional[str] = None
) -> Dict:
    """
    Compara uma métrica entre dois períodos e identifica variações.
    
    Args:
        metric_name: Nome da métrica ("revenue", "exams", "conversion_rate", etc)
        period1_start: Data início período 1 (formato: YYYY-MM-DD)
        period1_end: Data fim período 1
        period2_start: Data início período 2
        period2_end: Data fim período 2
        dimension: Dimensão para breakdown ("unit", "doctor", "product", etc)
    
    Returns:
        {
            "period1": {"value": float, "label": str, "breakdown": {...}},
            "period2": {"value": float, "label": str, "breakdown": {...}},
            "variation": {
                "absolute": float,
                "percentage": float,
                "direction": "increase|decrease|stable"
            },
            "top_contributors": [
                {"item": str, "contribution": float, "percentage": float}
            ],
            "biggest_changes": [
                {"item": str, "change": float, "change_pct": float}
            ]
        }
    
    Example:
        compare_periods(
            "revenue",
            "2025-01-01", "2025-01-31",
            "2026-01-01", "2026-01-31",
            "unit"
        )
    """
    
    try:
        # 0. Normalizar ordem dos períodos (period1 = mais antigo, period2 = mais recente)
        #    A IA pode inverter a ordem. Garantimos consistência aqui.
        if period1_start > period2_start:
            period1_start, period2_start = period2_start, period1_start
            period1_end, period2_end = period2_end, period1_end
        
        # 1. Buscar dados dos dois períodos
        period1_data = _fetch_metric_data(metric_name, period1_start, period1_end, dimension)
        period2_data = _fetch_metric_data(metric_name, period2_start, period2_end, dimension)
        
        if not period1_data or not period2_data:
            return {
                "error": "Não foi possível buscar dados para comparação",
                "period1": period1_data,
                "period2": period2_data
            }
        
        # 2. Calcular variação total
        p1_value = period1_data.get("value", 0)
        p2_value = period2_data.get("value", 0)
        
        absolute_variation = p2_value - p1_value
        percentage_variation = (absolute_variation / p1_value * 100) if p1_value != 0 else 0
        
        # Determinar direção
        if abs(percentage_variation) < 1.0:
            direction = "stable"
        elif percentage_variation > 0:
            direction = "increase"
        else:
            direction = "decrease"
        
        # 3. Identificar top contributors (para a variação)
        top_contributors = []
        biggest_changes = []
        
        if dimension and "breakdown" in period2_data:
            p1_breakdown = period1_data.get("breakdown", {})
            p2_breakdown = period2_data.get("breakdown", {})
            
            # Calcular contribuição de cada item para a variação total
            contributions = {}
            changes = {}
            
            # Itens que existem no período 2
            for item, p2_val in p2_breakdown.items():
                p1_val = p1_breakdown.get(item, 0)
                contribution = p2_val - p1_val
                contributions[item] = contribution
                
                if p1_val > 0:
                    change_pct = (contribution / p1_val) * 100
                    changes[item] = {
                        "change": contribution,
                        "change_pct": change_pct,
                        "p1_value": p1_val,
                        "p2_value": p2_val
                    }
            
            # Top contributors (maiores contribuições absolutas para a variação)
            sorted_contributions = sorted(
                contributions.items(),
                key=lambda x: abs(x[1]),
                reverse=True
            )[:5]
            
            for item, contribution in sorted_contributions:
                pct_of_total_change = (contribution / absolute_variation * 100) if absolute_variation != 0 else 0
                top_contributors.append({
                    "item": item,
                    "contribution": round(contribution, 2),
                    "percentage_of_total_change": round(pct_of_total_change, 1)
                })
            
            # Biggest changes (maiores mudanças percentuais)
            sorted_changes = sorted(
                changes.items(),
                key=lambda x: abs(x[1]["change_pct"]),
                reverse=True
            )[:5]
            
            for item, change_data in sorted_changes:
                biggest_changes.append({
                    "item": item,
                    "change": round(change_data["change"], 2),
                    "change_pct": round(change_data["change_pct"], 1),
                    "from": round(change_data["p1_value"], 2),
                    "to": round(change_data["p2_value"], 2)
                })
        
        # 4. Montar resposta
        return {
            "comparison": {
                "metric": metric_name,
                "dimension": dimension,
                "period1": {
                    "label": f"{period1_start} a {period1_end}",
                    "value": round(p1_value, 2),
                    "breakdown": period1_data.get("breakdown", {})
                },
                "period2": {
                    "label": f"{period2_start} a {period2_end}",
                    "value": round(p2_value, 2),
                    "breakdown": period2_data.get("breakdown", {})
                }
            },
            "variation": {
                "absolute": round(absolute_variation, 2),
                "percentage": round(percentage_variation, 1),
                "direction": direction
            },
            "top_contributors": top_contributors,
            "biggest_changes": biggest_changes,
            "summary": _generate_summary(
                metric_name,
                p1_value,
                p2_value,
                absolute_variation,
                percentage_variation,
                direction,
                top_contributors
            )
        }
        
    except Exception as e:
        logger.error(f"Erro em compare_periods: {e}", exc_info=True)
        return {"error": str(e)}


def _fetch_metric_data(metric_name: str, start_date: str, end_date: str, dimension: Optional[str]) -> Dict:
    """
    Busca dados da métrica para o período especificado.
    Usa query_metrics_tool que aplica regras de negócio via semantic layer.
    """
    try:
        # Mapear nomes amigáveis para nomes internos da semantic layer
        METRIC_MAP = {
            "revenue": "total_revenue",
            "faturamento": "total_revenue",
            "total_revenue": "total_revenue",
            "exams": "exam_count",
            "exames": "exam_count",
            "exam_count": "exam_count",
            "patients": "patient_count",
            "pacientes": "patient_count",
            "patient_count": "patient_count",
            "ticket": "ticket_average",
            "ticket_medio": "ticket_average",
            "ticket_average": "ticket_average",
        }
        
        # Mapear nomes de dimensões
        DIMENSION_MAP = {
            "unit": "unit",
            "unidade": "unit",
            "doctor": "doctor",
            "medico": "doctor",
            "product": "exam_name",
            "exam_name": "exam_name",
            "exame": "exam_name",
            "insurance": "insurance",
            "convenio": "insurance",
            "month": "month",
            "date": "date",
        }
        
        internal_metric = METRIC_MAP.get(metric_name.lower(), metric_name.lower())
        internal_dimension = DIMENSION_MAP.get(dimension, dimension) if dimension else None
        
        # Buscar dados via semantic layer (aplica business rules automaticamente)
        results = query_metrics_tool.execute(
            metric=internal_metric,
            dimension=internal_dimension,
            start_date=start_date,
            end_date=end_date
        )
        
        # Verificar erros
        if results and isinstance(results, list) and len(results) > 0 and "error" in results[0]:
            logger.error(f"Erro query_metrics: {results[0]['error']}")
            return {"value": 0, "breakdown": {}}
        
        # Processar resultados
        if not results or not isinstance(results, list):
            return {"value": 0, "breakdown": {}}
        
        if internal_dimension:
            # Com dimensão: criar breakdown
            breakdown = {}
            for row in results:
                dim_value = str(row.get(internal_dimension, "Desconhecido")).strip()
                metric_value = float(row.get(internal_metric, 0) or 0)
                breakdown[dim_value] = metric_value
            
            total = sum(breakdown.values())
            return {
                "value": total,
                "breakdown": breakdown
            }
        else:
            # Sem dimensão: retornar valor total
            if len(results) == 1:
                total = float(results[0].get(internal_metric, 0) or 0)
            else:
                total = sum(float(row.get(internal_metric, 0) or 0) for row in results)
            
            return {
                "value": total,
                "breakdown": {}
            }
    
    except Exception as e:
        logger.error(f"Erro ao buscar métrica {metric_name}: {e}", exc_info=True)
        return {"value": 0, "breakdown": {}}


def _generate_summary(
    metric_name: str,
    p1_value: float,
    p2_value: float,
    absolute_var: float,
    percentage_var: float,
    direction: str,
    top_contributors: List[Dict]
) -> str:
    """
    Gera resumo textual da comparação.
    """
    metric_label = metric_name.title()
    
    if direction == "stable":
        summary = f"{metric_label} permaneceu estável entre os períodos (variação de {percentage_var:.1f}%)."
    elif direction == "increase":
        summary = f"{metric_label} **cresceu** {abs(absolute_var):,.2f} (+{percentage_var:.1f}%), saindo de {p1_value:,.2f} para {p2_value:,.2f}."
    else:
        summary = f"{metric_label} **caiu** {abs(absolute_var):,.2f} ({percentage_var:.1f}%), saindo de {p1_value:,.2f} para {p2_value:,.2f}."
    
    # Adicionar top contributors se houver
    if top_contributors and len(top_contributors) > 0:
        top_item = top_contributors[0]
        contribution_text = "crescimento" if direction == "increase" else "queda" if direction == "decrease" else "variação"
        summary += f" O principal contributor para essa {contribution_text} foi **{top_item['item']}** ({top_item['contribution']:,.2f}, {top_item['percentage_of_total_change']:.1f}% do total)."
    
    return summary


# ============================================================
# GEMINI FUNCTION DECLARATION
# ============================================================

COMPARE_PERIODS_DECLARATION = {
    "name": "compare_periods",
    "description": """
Compara uma métrica entre dois períodos de tempo e identifica variações, top contributors e maiores mudanças.

Use esta ferramenta quando o usuário perguntar:
- "Compare [métrica] de [período1] com [período2]"
- "Como foi [métrica] em [mês/ano] comparado com [outro mês/ano]?"
- "Qual a diferença de [métrica] entre [período1] e [período2]?"

A ferramenta retorna:
- Valores de ambos os períodos
- Variação absoluta e percentual
- Top contributors (itens que mais contribuíram para a variação)
- Maiores mudanças percentuais por item

Métricas suportadas: revenue, exams, conversion_rate, etc.
Dimensões suportadas: unit, doctor, product, payment_method
""".strip(),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "metric_name": {
                "type": "STRING",
                "description": "Nome da métrica a comparar (ex: 'revenue', 'exams', 'conversion_rate')"
            },
            "period1_start": {
                "type": "STRING",
                "description": "Data de início do período BASE/ANTERIOR (mais antigo) no formato YYYY-MM-DD. Ex: '2025-01-01'"
            },
            "period1_end": {
                "type": "STRING",
                "description": "Data de fim do período BASE/ANTERIOR no formato YYYY-MM-DD. Ex: '2025-01-31'"
            },
            "period2_start": {
                "type": "STRING",
                "description": "Data de início do período ATUAL/RECENTE (mais novo) no formato YYYY-MM-DD. Ex: '2026-01-01'"
            },
            "period2_end": {
                "type": "STRING",
                "description": "Data de fim do período ATUAL/RECENTE no formato YYYY-MM-DD. Ex: '2026-01-31'"
            },
            "dimension": {
                "type": "STRING",
                "description": "Dimensão para quebrar a análise (opcional). Valores: 'unit', 'doctor', 'product', 'insurance', etc."
            }
        },
        "required": ["metric_name", "period1_start", "period1_end", "period2_start", "period2_end"]
    }
}
