from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection
from app.services.budget import get_budget_data, process_budget_metrics
import logging

logger = logging.getLogger(__name__)

class QueryBudgetsTool:
    """
    Tool para consultar métricas de orçamentos.
    Reutiliza a lógica do endpoint /comercial/orcamentos.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna métricas de orçamentos (quantidade, valor, taxa de conversão).
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com métricas de orçamentos
        """
        try:
            # Valores default
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).date().strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().date().strftime('%Y-%m-%d')
            
            logger.info(f"[QueryBudgetsTool] Fetching budgets: {start_date} to {end_date}")
            print(f"📋 Consultando orçamentos ({start_date} a {end_date})...")
            
            # Reutilizar lógica do endpoint
            conn = get_db_connection()
            cursor = conn.cursor()
            
            df_budgets = get_budget_data(cursor, start_date, end_date)
            conn.close()
            
            if df_budgets.empty:
                return {
                    "message": "Nenhum orçamento encontrado para o período",
                    "period": {"start": start_date, "end": end_date},
                    "metrics": None
                }
            
            metrics = process_budget_metrics(df_budgets)
            
            # Converter Pydantic models para dict
            formatted_result = {
                "period": {"start": start_date, "end": end_date},
                "summary": {
                    "total_budgets": metrics.sintetico_geral.quantidade_total,
                    "total_value": float(metrics.sintetico_geral.valor_total),
                    "converted_budgets": metrics.sintetico_geral.quantidade_convertidos,
                    "converted_value": float(metrics.sintetico_geral.valor_convertidos),
                    "open_budgets": metrics.sintetico_geral.quantidade_abertos,
                    "open_value": float(metrics.sintetico_geral.valor_abertos),
                    "conversion_rate": float(metrics.sintetico_geral.taxa_conversao)
                },
                "by_unit": [
                    {
                        "unit": item.unidade,
                        "total_budgets": item.quantidade_total,
                        "total_value": float(item.valor_total),
                        "converted_budgets": item.quantidade_convertidos,
                        "conversion_rate": float(item.taxa_conversao)
                    } for item in metrics.por_unidade
                ],
                "by_user": [
                    {
                        "unit": item.unidade,
                        "user": item.usuario,
                        "total_budgets": item.quantidade_total,
                        "total_value": float(item.valor_total),
                        "converted_budgets": item.quantidade_convertidos,
                        "conversion_rate": float(item.taxa_conversao)
                    } for item in metrics.por_usuario
                ]
            }
            
            print(f"✅ {metrics.sintetico_geral.quantidade_total} orçamentos encontrados. ")
            print(f"   Taxa de conversão: {metrics.sintetico_geral.taxa_conversao:.1f}%")
            
            return formatted_result
            
        except Exception as e:
            logger.error(f"Error in QueryBudgetsTool: {e}", exc_info=True)
            return {"error": str(e)}

query_budgets_tool = QueryBudgetsTool()
