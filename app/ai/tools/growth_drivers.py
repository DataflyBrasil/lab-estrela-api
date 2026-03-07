"""
Ferramenta analítica para identificar principais fatores que causaram crescimento ou queda.
Analisa múltiplas dimensões e decomp

õe variações.
"""

from typing import Dict, List, Optional
from app.ai.tools.compare_periods import compare_periods, _fetch_metric_data
import logging

logger = logging.getLogger(__name__)

def identify_growth_drivers(
    metric_name: str,
    current_period_start: str,
    current_period_end: str,
    comparison_period_start: str,
    comparison_period_end: str,
    dimensions: Optional[List[str]] = None
) -> Dict:
    """
    Identifica os principais fatores que causaram crescimento ou queda de uma métrica.
    
    Analisa múltiplas dimensões e identifica:
    - Volume vs. Price effects (quando aplicável)
    - Top contributors absolutos
    - Mix effects (mudança na distribuição)
    - Novos vs. perdidos (itens que apareceram/desapareceram)
    
    Args:
        metric_name: Métrica a analisar ("revenue", "exams", etc)
        current_period_start: Início do período atual
        current_period_end: Fim do período atual
        comparison_period_start: Início do período de comparação
        comparison_period_end: Fim do período de comparação
        dimensions: Lista de dimensões para analisar (default: ["unit", "doctor"])
    
    Returns:
        {
            "primary_drivers": [
                {
                    "factor": str,
                    "impact_value": float,
                    "impact_percentage": float,
                    "description": str
                }
            ],
            "by_dimension": {
                "unit": {...},
                "doctor": {...}
            },
            "new_items": [...],
            "lost_items": [...],
            "summary": str
        }
    
    Example:
        identify_growth_drivers(
            "revenue",
            "2026-01-01", "2026-01-31",
            "2025-01-01", "2025-01-31",
            ["unit", "doctor"]
        )
    """
    
    try:
        if dimensions is None:
            dimensions = ["unit"]  # Default dimension
        
        # Normalizar: garantir que comparison é o mais antigo e current o mais recente
        if current_period_start < comparison_period_start:
            current_period_start, comparison_period_start = comparison_period_start, current_period_start
            current_period_end, comparison_period_end = comparison_period_end, current_period_end
        
        # 1. Buscar dados de ambos os períodos
        current_data = _fetch_metric_data(
            metric_name,
            current_period_start,
            current_period_end,
            dimension=None  # Busca total primeiro
        )
        comparison_data = _fetch_metric_data(
            metric_name,
            comparison_period_start,
            comparison_period_end,
            dimension=None
        )
        
        current_value = current_data.get("value", 0)
        comparison_value = comparison_data.get("value", 0)
        total_change = current_value - comparison_value
        
        if comparison_value == 0:
            return {
                "error": "Período de comparação tem valor zero, não é possível calcular drivers"
            }
        
        # 2. Analisar cada dimensão
        analysis_by_dimension = {}
        all_contributors = []
        
        for dimension in dimensions:
            dim_analysis = _analyze_dimension(
                metric_name,
                current_period_start,
                current_period_end,
                comparison_period_start,
                comparison_period_end,
                dimension,
                total_change
            )
            analysis_by_dimension[dimension] = dim_analysis
            
            # Coletar contributors desta dimensão
            for contrib in dim_analysis.get("top_contributors", []):
                all_contributors.append({
                    "factor": f"{dimension.title()}: {contrib['item']}",
                    "impact_value": contrib["contribution"],
                    "impact_percentage": contrib["percentage_of_total_change"],
                    "description": f"Contribuiu com {contrib['contribution']:,.2f}"
                })
        
        # 3. Identificar primary drivers (top 5 absolutos)
        primary_drivers = sorted(
            all_contributors,
            key=lambda x: abs(x["impact_value"]),
            reverse=True
        )[:5]
        
        # 4. Análise de volume vs price (se métrica for revenue)
        volume_price_analysis = None
        if metric_name.lower() in ["revenue", "faturamento"]:
            volume_price_analysis = _analyze_volume_vs_price(
                current_period_start,
                current_period_end,
                comparison_period_start,
                comparison_period_end
            )
        
        # 5. Gerar resumo
        summary = _generate_drivers_summary(
            metric_name,
            comparison_value,
            current_value,
            total_change,
            primary_drivers,
            volume_price_analysis
        )
        
        return {
            "overview": {
                "metric": metric_name,
                "comparison_period": f"{comparison_period_start} a {comparison_period_end}",
                "current_period": f"{current_period_start} a {current_period_end}",
                "comparison_value": round(comparison_value, 2),
                "current_value": round(current_value, 2),
                "total_change": round(total_change, 2),
                "total_change_pct": round((total_change / comparison_value * 100), 1)
            },
            "primary_drivers": primary_drivers,
            "by_dimension": analysis_by_dimension,
            "volume_price_decomposition": volume_price_analysis,
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Erro em identify_growth_drivers: {e}", exc_info=True)
        return {"error": str(e)}


def _analyze_dimension(
    metric_name: str,
    current_start: str,
    current_end: str,
    comparison_start: str,
    comparison_end: str,
    dimension: str,
    total_change: float
) -> Dict:
    """
    Analisa uma dimensão específica.
    """
    try:
        # Buscar dados quebrados por dimensão
        current_data = _fetch_metric_data(metric_name, current_start, current_end, dimension)
        comparison_data = _fetch_metric_data(metric_name, comparison_start, comparison_end, dimension)
        
        current_breakdown = current_data.get("breakdown", {})
        comparison_breakdown = comparison_data.get("breakdown", {})
        
        # Calcular contribuições
        contributors = []
        new_items = []
        lost_items = []
        
        # Todos os itens únicos
        all_items = set(current_breakdown.keys()) | set(comparison_breakdown.keys())
        
        for item in all_items:
            curr_val = current_breakdown.get(item, 0)
            comp_val = comparison_breakdown.get(item, 0)
            contribution = curr_val - comp_val
            
            # Classificar item
            if comp_val == 0 and curr_val > 0:
                new_items.append({"item": item, "value": curr_val})
            elif curr_val == 0 and comp_val > 0:
                lost_items.append({"item": item, "value": comp_val})
            
            # Calcular percentual do total change
            pct_of_total = (contribution / total_change * 100) if total_change != 0 else 0
            
            contributors.append({
                "item": item,
                "contribution": round(contribution, 2),
                "percentage_of_total_change": round(pct_of_total, 1),
                "from_value": round(comp_val, 2),
                "to_value": round(curr_val, 2)
            })
        
        # Ordenar por contribuição absoluta
        top_contributors = sorted(
            contributors,
            key=lambda x: abs(x["contribution"]),
            reverse=True
        )[:10]
        
        return {
            "dimension": dimension,
            "top_contributors": top_contributors,
            "new_items": sorted(new_items, key=lambda x: x["value"], reverse=True)[:5],
            "lost_items": sorted(lost_items, key=lambda x: x["value"], reverse=True)[:5]
        }
        
    except Exception as e:
        logger.error(f"Erro ao analisar dimensão {dimension}: {e}")
        return {"error": str(e)}


def _analyze_volume_vs_price(
    current_start: str,
    current_end: str,
    comparison_start: str,
    comparison_end: str
) -> Dict:
    """
    Decompõe variação de revenue em efeito volume vs efeito preço.
    Usa _fetch_metric_data (semantic layer) para garantir regras de negócio.
    
    Formula:
    ΔRevenue = ΔVolume * Price_base + Volume_current * ΔPrice + ΔVolume * ΔPrice
    """
    try:
        # Buscar revenue e volume (exams) para ambos períodos via semantic layer
        comp_revenue_data = _fetch_metric_data("total_revenue", comparison_start, comparison_end, None)
        curr_revenue_data = _fetch_metric_data("total_revenue", current_start, current_end, None)
        comp_exams_data = _fetch_metric_data("exam_count", comparison_start, comparison_end, None)
        curr_exams_data = _fetch_metric_data("exam_count", current_start, current_end, None)
        
        comp_revenue = comp_revenue_data.get("value", 0)
        curr_revenue = curr_revenue_data.get("value", 0)
        comp_exams = comp_exams_data.get("value", 0)
        curr_exams = curr_exams_data.get("value", 0)
        
        if comp_exams == 0 or comp_revenue == 0:
            return None
        
        # Calcular ticket médio (preço médio por exame)
        comp_avg_price = comp_revenue / comp_exams
        curr_avg_price = curr_revenue / curr_exams if curr_exams > 0 else 0
        
        # Variações
        delta_volume = curr_exams - comp_exams
        delta_price = curr_avg_price - comp_avg_price
        delta_revenue = curr_revenue - comp_revenue
        
        # Decomposição
        volume_effect = delta_volume * comp_avg_price
        price_effect = comp_exams * delta_price
        mix_effect = delta_volume * delta_price
        
        return {
            "comparison_period": {
                "revenue": round(comp_revenue, 2),
                "volume": int(comp_exams),
                "avg_price": round(comp_avg_price, 2)
            },
            "current_period": {
                "revenue": round(curr_revenue, 2),
                "volume": int(curr_exams),
                "avg_price": round(curr_avg_price, 2)
            },
            "decomposition": {
                "total_change": round(delta_revenue, 2),
                "volume_effect": round(volume_effect, 2),
                "price_effect": round(price_effect, 2),
                "mix_effect": round(mix_effect, 2),
                "volume_effect_pct": round(volume_effect / delta_revenue * 100, 1) if delta_revenue != 0 else 0,
                "price_effect_pct": round(price_effect / delta_revenue * 100, 1) if delta_revenue != 0 else 0
            },
            "interpretation": _interpret_volume_price(volume_effect, price_effect, delta_revenue)
        }
        
    except Exception as e:
        logger.error(f"Erro em volume vs price analysis: {e}", exc_info=True)
        return None


def _interpret_volume_price(volume_effect: float, price_effect: float, total_change: float) -> str:
    """
    Interpreta a decomposição volume vs price.
    """
    if total_change == 0:
        return "Não houve variação no faturamento."
    
    vol_pct = abs(volume_effect / total_change * 100)
    price_pct = abs(price_effect / total_change * 100)
    
    if vol_pct > 70:
        driver = "volume de exames"
    elif price_pct > 70:
        driver = "ticket médio (preço)"
    else:
        driver = "combinação de volume e preço"
    
    direction = "crescimento" if total_change > 0 else "queda"
    
    return f"O {direction} foi principalmente impulsionado por {driver}."


def _generate_drivers_summary(
    metric_name: str,
    comparison_value: float,
    current_value: float,
    total_change: float,
    primary_drivers: List[Dict],
    volume_price: Optional[Dict]
) -> str:
    """
    Gera resumo textual dos drivers.
    """
    change_pct = (total_change / comparison_value * 100) if comparison_value != 0 else 0
    direction = "crescimento" if total_change > 0 else "queda"
    
    summary = f"Analisei o {direction} de {abs(total_change):,.2f} ({abs(change_pct):.1f}%) em {metric_name}. "
    
    # Volume vs Price (se disponível)
    if volume_price and "decomposition" in volume_price:
        decomp = volume_price["decomposition"]
        vol_pct = decomp.get("volume_effect_pct", 0)
        prc_pct = decomp.get("price_effect_pct", 0)
        
        if abs(vol_pct) > abs(prc_pct):
            summary += f"O fator dominante foi **variação de volume** ({vol_pct:.1f}% do impacto). "
        else:
            summary += f"O fator dominante foi **variação de preço** ({prc_pct:.1f}% do impacto). "
    
    # Top drivers
    if primary_drivers:
        summary += "\n\n**Principais Drivers:**\n"
        for i, driver in enumerate(primary_drivers[:3], 1):
            summary += f"{i}. **{driver['factor']}**: {driver['impact_value']:,.2f} ({driver['impact_percentage']:.1f}% do total)\n"
    
    return summary.strip()


# ============================================================
# GEMINI FUNCTION DECLARATION
# ============================================================

IDENTIFY_GROWTH_DRIVERS_DECLARATION = {
    "name": "identify_growth_drivers",
    "description": """
Identifica os principais fatores que causaram crescimento ou queda de uma métrica.

Use esta ferramenta quando o usuário perguntar:
- "Por que [métrica] cresceu/caiu?"
- "Quais fatores influenciaram [métrica]?"
- "O que causou a mudança em [métrica]?"
- "Explique o crescimento/queda de [métrica]"

A ferramenta analisa múltiplas dimensões e retorna:
- Principais drivers com impacto quantificado
- Decomposição volume vs. preço (para revenue)
- Análise por unidade, médico, produto, etc.
- Itens novos e perdidos
- Resumo executivo com insights

Muito útil para análises de "root cause" e entendimento de variações.
""".strip(),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "metric_name": {
                "type": "STRING",
                "description": "Métrica a analisar (ex: 'revenue', 'exams', 'conversion_rate')"
            },
            "current_period_start": {
                "type": "STRING",
                "description": "Data de início do período atual (YYYY-MM-DD)"
            },
            "current_period_end": {
                "type": "STRING",
                "description": "Data de fim do período atual (YYYY-MM-DD)"
            },
            "comparison_period_start": {
                "type": "STRING",
                "description": "Data de início do período de comparação (YYYY-MM-DD)"
            },
            "comparison_period_end": {
                "type": "STRING",
                "description": "Data de fim do período de comparação (YYYY-MM-DD)"
            },
            "dimensions": {
                "type": "ARRAY",
                "items": {"type": "STRING"},
                "description": "Lista de dimensões para analisar (opcional). Ex: ['unit', 'doctor']"
            }
        },
        "required": ["metric_name", "current_period_start", "current_period_end", "comparison_period_start", "comparison_period_end"]
    }
}
