"""
Ferramenta para análise multidimensional de métricas.
Quebra dados por múltiplas dimensões simultaneamente.
"""

from typing import Dict, List, Optional
from app.ai.tools.compare_periods import _fetch_metric_data
import logging

logger = logging.getLogger(__name__)

def breakdown_analysis(
    metric_name: str,
    period_start: str,
    period_end: str,
    dimensions: List[str],
    top_n: int = 10
) -> Dict:
    """
    Quebra uma métrica em múltiplas dimensões simultaneamente.
    
    Args:
        metric_name: Métrica a analisar
        period_start: Data de início (YYYY-MM-DD)
        period_end: Data de fim (YYYY-MM-DD)
        dimensions: Lista de dimensões para analisar (ex: ["unit", "doctor", "product"])
        top_n: Número de top itens a retornar por dimensão (default: 10)
    
    Returns:
        {
            "total_value": float,
            "by_dimension": {
                "unit": {
                    "distribution": [{item, value, percentage}],
                    "top_items": [...],
                    "concentration": "high|medium|low"
                },
                ...
            },
            "summary": str
        }
    
    Example:
        breakdown_analysis(
            "revenue",
            "2026-01-01", "2026-01-31",
            ["unit", "doctor"],
            top_n=5
        )
    """
    
    try:
        # 1. Buscar valor total
        total_data = _fetch_metric_data(metric_name, period_start, period_end, dimension=None)
        total_value = total_data.get("value", 0)
        
        if total_value == 0:
            return {"error": "Valor total é zero, não é possível fazer breakdown"}
        
        # 2. Analisar cada dimensão
        analysis_by_dimension = {}
        
        for dimension in dimensions:
            dim_data = _fetch_metric_data(metric_name, period_start, period_end, dimension)
            breakdown = dim_data.get("breakdown", {})
            
            if not breakdown:
                analysis_by_dimension[dimension] = {
                    "error": f"Sem dados para dimensão {dimension}"
                }
                continue
            
            # Calcular distribuição e concentração
            dim_analysis = _analyze_dimension_breakdown(breakdown, total_value, top_n)
            analysis_by_dimension[dimension] = dim_analysis
        
        # 3. Gerar resumo
        summary = _generate_breakdown_summary(
            metric_name,
            total_value,
            period_start,
            period_end,
            analysis_by_dimension
        )
        
        return {
            "metric": metric_name,
            "period": f"{period_start} a {period_end}",
            "total_value": round(total_value, 2),
            "by_dimension": analysis_by_dimension,
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Erro em breakdown_analysis: {e}", exc_info=True)
        return {"error": str(e)}


def _analyze_dimension_breakdown(breakdown: Dict, total_value: float, top_n: int) -> Dict:
    """
    Analisa breakdown de uma dimensão.
    """
    # Criar lista ordenada de itens
    items = []
    for item_name, value in breakdown.items():
        percentage = (value / total_value * 100) if total_value > 0 else 0
        items.append({
            "item": item_name,
            "value": round(value, 2),
            "percentage": round(percentage, 1)
        })
    
    # Ordenar por valor decrescente
    items_sorted = sorted(items, key=lambda x: x["value"], reverse=True)
    
    # Top N
    top_items = items_sorted[:top_n]
    
    # Calcular concentração (% que os top 20% representam)
    concentration = _calculate_concentration(items_sorted)
    
    # Distribuição completa
    distribution = items_sorted
    
    return {
        "total_items": len(items),
        "top_items": top_items,
        "distribution": distribution,
        "concentration": {
            "level": concentration["level"],
            "top_20_pct_share": concentration["top_20_pct"],
            "interpretation": concentration["interpretation"]
        }
    }


def _calculate_concentration(items: List[Dict]) -> Dict:
    """
    Calcula nível de concentração usando regra de Pareto (80/20).
    """
    if not items:
        return {"level": "unknown", "top_20_pct": 0, "interpretation": "Sem dados"}
    
    total_items = len(items)
    top_20_pct_count = max(1, int(total_items * 0.2))
    
    # Somar valor dos top 20%
    top_20_pct_value = sum(item["value"] for item in items[:top_20_pct_count])
    total_value = sum(item["value"] for item in items)
    
    if total_value == 0:
        return {"level": "unknown", "top_20_pct": 0, "interpretation": "Sem dados"}
    
    top_20_pct_share = (top_20_pct_value / total_value * 100)
    
    # Classificar concentração
    if top_20_pct_share >= 80:
        level = "high"
        interpretation = "Alta concentração - Princípio de Pareto aplicável (80/20)"
    elif top_20_pct_share >= 60:
        level = "medium"
        interpretation = "Concentração moderada - Distribuição relativamente balanceada"
    else:
        level = "low"
        interpretation = "Baixa concentração - Distribuição bem distribuída"
    
    return {
        "level": level,
        "top_20_pct": round(top_20_pct_share, 1),
        "interpretation": interpretation
    }


def _generate_breakdown_summary(
    metric_name: str,
    total_value: float,
    period_start: str,
    period_end: str,
    analysis: Dict
) -> str:
    """
    Gera resumo textual do breakdown.
    """
    summary = f"Análise de {metric_name} no período de {period_start} a {period_end} (total: {total_value:,.2f}):\n\n"
    
    for dimension, data in analysis.items():
        if "error" in data:
            continue
        
        summary += f"**{dimension.title()}:**\n"
        
        # Top 3 items
        top_items = data.get("top_items", [])[:3]
        for i, item in enumerate(top_items, 1):
            summary += f"{i}. {item['item']}: {item['value']:,.2f} ({item['percentage']:.1f}%)\n"
        
        # Concentração
        concentration = data.get("concentration", {})
        summary += f"   • {concentration.get('interpretation', '')}\n"
        summary += f"   • Top 20% representa {concentration.get('top_20_pct', 0):.1f}% do total\n\n"
    
    return summary.strip()


# ============================================================
# GEMINI FUNCTION DECLARATION
# ============================================================

BREAKDOWN_ANALYSIS_DECLARATION = {
    "name": "breakdown_analysis",
    "description": """
Quebra uma métrica em múltiplas dimensões simultaneamente e analisa distribuição.

Use esta ferramenta quando o usuário perguntar:
- "Detalhe [métrica] por [dimensão]"
- "Quebre [métrica] por unidade/médico/produto"
- "Qual a distribuição de [métrica]?"
- "Mostre os top [dimensão] em [métrica]"
- "Análise multidimensional de [métrica]"

A ferramenta retorna:
- Valor total da métrica
- Breakdown por cada dimensão solicitada
- Top N items de cada dimensão (ranking)
- Análise de concentração (Pareto 80/20)
- Distribuição completa

Muito útil para entender como uma métrica está distribuída entre diferentes categorias.

Exemplos de dimensões: unit, doctor, product, payment_method, insurance, trip_reason
""".strip(),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "metric_name": {
                "type": "STRING",
                "description": "Métrica a analisar (ex: 'revenue', 'exams', 'budgets')"
            },
            "period_start": {
                "type": "STRING",
                "description": "Data de início do período (YYYY-MM-DD)"
            },
            "period_end": {
                "type": "STRING",
                "description": "Data de fim do período (YYYY-MM-DD)"
            },
            "dimensions": {
                "type": "ARRAY",
                "items": {"type": "STRING"},
                "description": "Lista de dimensões para analisar. Ex: ['unit', 'doctor', 'product']"
            },
            "top_n": {
                "type": "INTEGER",
                "description": "Número de top itens a retornar por dimensão (padrão: 10)"
            }
        },
        "required": ["metric_name", "period_start", "period_end", "dimensions"]
    }
}
