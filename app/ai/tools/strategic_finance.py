from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection
from app.services.analytics import get_financial_analytics_data, process_financial_analytics_python
import logging

logger = logging.getLogger(__name__)

class QueryStrategicFinanceTool:
    """
    Tool para consultar métricas financeiras estratégicas.
    Reutiliza a lógica do endpoint /financeiro/estrategico.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna métricas financeiras estratégicas (faturamento, margem, caixa, etc).
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com métricas financeiras estratégicas
        """
        try:
            # Valores default
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).date()
            else:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            
            if not end_date:
                end_date = datetime.now().date()
            else:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            logger.info(f"[QueryStrategicFinanceTool] Fetching strategic finance: {start_date} to {end_date}")
            print(f"💰 Consultando financeiro estratégico ({start_date} a {end_date})...")
            
            # Reutilizar lógica do endpoint
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            
            df_faturamento, df_caixa, total_atendimentos, valor_mte_final, valor_ipc_final, df_units_convenio = get_financial_analytics_data(cursor, start_date, end_date)
            conn.close()
            
            analytics_result = process_financial_analytics_python(df_faturamento, df_caixa, total_atendimentos, valor_mte_final, valor_ipc_final, df_units_convenio)
            
            # Converter Pydantic model para dict se necessário
            if hasattr(analytics_result, 'dict'):
                result = analytics_result.dict()
            else:
                result = analytics_result
            
            print(f"✅ Dados financeiros processados. Faturamento bruto: R$ {result.get('faturamento_bruto', 0):,.2f}")
            
            # Formatar para LLM
            return {
                "period": {"start": str(start_date), "end": str(end_date)},
                "metrics": result
            }
            
        except Exception as e:
            logger.error(f"Error in QueryStrategicFinanceTool: {e}", exc_info=True)
            return {"error": str(e)}

query_strategic_finance_tool = QueryStrategicFinanceTool()
