"""
Ferramentas estatísticas para análise de dados.
Calcula estatísticas descritivas, tendências, e identifica padrões.
"""

from typing import Dict, List, Optional
import statistics
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def calculate_statistics(
    metric_name: str,
    start_date: str,
    end_date: str,
    groupby: str = "month"
) -> Dict:
    """
    Calcula estatísticas descritivas de uma métrica ao longo do tempo.
    
    Args:
        metric_name: Métrica a analisar
        start_date: Data de início (YYYY-MM-DD)
        end_date: Data de fim (YYYY-MM-DD)
        groupby: Agrupamento temporal ("day", "week", "month")
    
    Returns:
        {
            "timeseries": [...],  # Valores ao longo do tempo
            "statistics": {
                "mean": float,
                "median": float,
                "std_dev": float,
                "min": float,
               "max": float,
                "range": float
            },
            "trend": {
                "direction": "increasing|decreasing|stable",
                "avg_growth_rate": float,
                "volatility": "low|medium|high"
            },
            "outliers": [...],
            "summary": str
        }
    
    Example:
        calculate_statistics("revenue", "2025-01-01", "2025-12-31", "month")
    """
    
    try:
        from app.ai.tools.compare_periods import _fetch_metric_data
        
        # 1. Gerar períodos baseado em groupby
        periods = _generate_periods(start_date, end_date, groupby)
        
        # 2. Buscar dados para cada período via semantic layer (com regras de negócio)
        timeseries = []
        values = []
        
        for period in periods:
            data = _fetch_metric_data(metric_name, period["start"], period["end"], None)
            value = data.get("value", 0)
            
            timeseries.append({
                "period": period["label"],
                "start_date": period["start"],
                "end_date": period["end"],
                "value": round(value, 2)
            })
            values.append(value)
        
        if not values or all(v == 0 for v in values):
            return {"error": "Sem dados suficientes para análise estatística"}
        
        # 3. Calcular estatísticas descritivas
        stats = {
            "count": len(values),
            "mean": round(statistics.mean(values), 2),
            "median": round(statistics.median(values), 2),
            "std_dev": round(statistics.stdev(values), 2) if len(values) > 1 else 0,
            "min": round(min(values), 2),
            "max": round(max(values), 2),
            "range": round(max(values) - min(values), 2)
        }
        
        # 4. Análise de tendência
        trend_analysis = _analyze_trend(values)
        
        # 5. Identificar outliers
        outliers = _identify_outliers(timeseries, stats["mean"], stats["std_dev"])
        
        # 6. Gerar resumo
        summary = _generate_stats_summary(metric_name, stats, trend_analysis, len(outliers))
        
        return {
            "metric": metric_name,
            "period": f"{start_date} a {end_date}",
            "groupby": groupby,
            "timeseries": timeseries,
            "statistics": stats,
            "trend": trend_analysis,
            "outliers": outliers,
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Erro em calculate_statistics: {e}", exc_info=True)
        return {"error": str(e)}


def _generate_periods(start_date: str, end_date: str, groupby: str) -> List[Dict]:
    """
    Gera lista de períodos para análise temporal.
    """
    from dateutil.relativedelta import relativedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    periods = []
    current = start
    
    while current <= end:
        if groupby == "day":
            period_end = current
            delta = timedelta(days=1)
            label = current.strftime("%Y-%m-%d")
        elif groupby == "week":
            period_end = min(current + timedelta(days=6), end)
            delta = timedelta(days=7)
            label = f"Semana {current.strftime('%Y-%W')}"
        else:  # month
            period_end = min(current + relativedelta(months=1) - timedelta(days=1), end)
            delta = relativedelta(months=1)
            label = current.strftime("%Y-%m")
        
        periods.append({
            "label": label,
            "start": current.strftime("%Y-%m-%d"),
            "end": period_end.strftime("%Y-%m-%d")
        })
        
        current += delta
    
    return periods


def _analyze_trend(values: List[float]) -> Dict:
    """
    Analisa tendência dos valores.
    """
    if len(values) < 2:
        return {"direction": "stable", "avg_growth_rate": 0, "volatility": "low"}
    
    # Calcular taxa de crescimento média
    growth_rates = []
    for i in range(1, len(values)):
        if values[i-1] != 0:
            growth_rate = ((values[i] - values[i-1]) / values[i-1]) * 100
            growth_rates.append(growth_rate)
    
    avg_growth_rate = statistics.mean(growth_rates) if growth_rates else 0
    
    # Determinar direção
    if abs(avg_growth_rate) < 2:
        direction = "stable"
    elif avg_growth_rate > 0:
        direction = "increasing"
    else:
        direction = "decreasing"
    
    # Volatilidade (baseada no desvio padrão das taxas de crescimento)
    if growth_rates:
        volatility_value = statistics.stdev(growth_rates) if len(growth_rates) > 1 else 0
        if volatility_value < 10:
            volatility = "low"
        elif volatility_value < 25:
            volatility = "medium"
        else:
            volatility = "high"
    else:
        volatility = "low"
    
    return {
        "direction": direction,
        "avg_growth_rate": round(avg_growth_rate, 2),
        "volatility": volatility,
        "volatility_value": round(volatility_value, 2) if growth_rates else 0
    }


def _identify_outliers(timeseries: List[Dict], mean: float, std_dev: float) -> List[Dict]:
    """
    Identifica outliers usando o método de 2 desvios padrão.
    """
    if std_dev == 0:
        return []
    
    outliers = []
    threshold = 2 * std_dev
    
    for point in timeseries:
        value = point["value"]
        deviation = abs(value - mean)
        
        if deviation > threshold:
            outliers.append({
                "period": point["period"],
                "value": value,
                "deviation_from_mean": round(deviation, 2),
                "direction": "above" if value > mean else "below"
            })
    
    return outliers


def _generate_stats_summary(metric_name: str, stats: Dict, trend: Dict, outlier_count: int) -> str:
    """
    Gera resumo textual das estatísticas.
    """
    direction_text = {
        "increasing": "crescente",
        "decreasing": "decrescente",
        "stable": "estável"
    }
    
    volatility_text = {
        "low": "baixa volatilidade",
        "medium": "volatilidade moderada",
        "high": "alta volatilidade"
    }
    
    summary = f"{metric_name.title()} apresenta tendência **{direction_text[trend['direction']]}** "
    summary += f"com taxa média de crescimento de {trend['avg_growth_rate']:.1f}% por período. "
    summary += f"A métrica tem {volatility_text[trend['volatility']]}.\n\n"
    
    summary += f"**Estatísticas:** Média de {stats['mean']:,.2f}, "
    summary += f"variando entre {stats['min']:,.2f} (mínimo) e {stats['max']:,.2f} (máximo). "
    
    if outlier_count > 0:
        summary += f"\n\n⚠️ **{outlier_count} período(s) identificado(s) como outlier(s)** "
        summary += "(desvio significativo da média)."
    
    return summary.strip()


# ============================================================
# GEMINI FUNCTION DECLARATION
# ============================================================

CALCULATE_STATISTICS_DECLARATION = {
    "name": "calculate_statistics",
    "description": """
Calcula estatísticas descritivas de uma métrica ao longo do tempo.

Use esta ferramenta quando o usuário perguntar:
- "Qual a tendência de [métrica]?"
- "Como variou [métrica] nos últimos meses?"
- "Mostre estatísticas de [métrica]"
- "[Métrica] está crescendo ou caindo?"
- "Identifique outliers em [métrica]"

A ferramenta retorna:
- Série temporal com valores por período
- Estatísticas descritivas (média, mediana, desvio padrão, etc)
- Análise de tendência (crescente/decrescente/estável)
- Taxa média de crescimento
- Volatilidade
- Outliers identificados

Útil para entender padrões históricos e comportamento da métrica.
""".strip(),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "metric_name": {
                "type": "STRING",
                "description": "Métrica a analisar (ex: 'revenue', 'exams')"
            },
            "start_date": {
                "type": "STRING",
                "description": "Data de início da análise (YYYY-MM-DD)"
            },
            "end_date": {
                "type": "STRING",
                "description": "Data de fim da análise (YYYY-MM-DD)"
            },
            "groupby": {
                "type": "STRING",
                "description": "Agrupamento temporal"
            }
        },
        "required": ["metric_name", "start_date", "end_date"]
    }
}
