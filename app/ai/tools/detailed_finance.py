from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection
from app.services.analytics import get_detailed_finance_data, process_detailed_finance_python
import logging

logger = logging.getLogger(__name__)

class QueryDetailedFinanceTool:
    """
    Tool para consultar detalhamento financeiro.
    Reutiliza a lógica do endpoint /financeiro/detalhado.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna detalhamento financeiro (produtos, formas de pagamento, pacientes).
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com detalhamento financeiro
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
            
            logger.info(f"[QueryDetailedFinanceTool] Fetching detailed finance: {start_date} to {end_date}")
            print(f"📊 Consultando financeiro detalhado ({start_date} a {end_date})...")
            
            # Reutilizar lógica do endpoint
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            
            mte_totals, df_payments, df_patients, valor_convenio_faturado = get_detailed_finance_data(cursor, start_date, end_date)
            conn.close()
            
            analytics_result = process_detailed_finance_python(mte_totals, df_payments, df_patients, valor_convenio_faturado)
            
            # Converter Pydantic model para dict se necessário
            if hasattr(analytics_result, 'dict'):
                result = analytics_result.dict()
            else:
                result = analytics_result
            
            print(f"✅ Detalhamento financeiro processado.")
            
            # Formatar para LLM
            return {
                "period": {"start": str(start_date), "end": str(end_date)},
                "details": result
            }
            
        except Exception as e:
            logger.error(f"Error in QueryDetailedFinanceTool: {e}", exc_info=True)
            return {"error": str(e)}

query_detailed_finance_tool = QueryDetailedFinanceTool()
