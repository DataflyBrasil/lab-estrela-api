from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.analytics import get_clients_analytics_data, process_clients_analytics_python
import logging

logger = logging.getLogger(__name__)

class QueryClientAnalyticsTool:
    """
    Tool para consultar análise de clientes/pacientes.
    Reutiliza a lógica do endpoint /clients.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna análise de clientes/pacientes (novos, recorrentes, demografias).
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com análise de clientes
        """
        conn = None
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).date()
            else:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

            if not end_date:
                end_date = datetime.now().date()
            else:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            logger.info(f"[QueryClientAnalyticsTool] Fetching client analytics: {start_date} to {end_date}")
            print(f"👥 Consultando análise de clientes ({start_date} a {end_date})...")
            
            # Reutilizar lógica do endpoint
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            df = get_clients_analytics_data(cursor, start_date, end_date)
            release_connection(conn)
            conn = None
            
            if df.empty:
                return {
                    "message": "Nenhum dado de clientes encontrado para o período",
                    "period": {"start": str(start_date), "end": str(end_date)},
                    "metrics": None
                }
            
            analytics_result = process_clients_analytics_python(df, start_date, end_date)
            
            # Converter Pydantic model para dict se necessário
            if hasattr(analytics_result, 'dict'):
                result = analytics_result.dict()
            else:
                result = analytics_result
            
            print(f"✅ Análise de clientes processada.")
            
            # Formatar para LLM
            return {
                "period": {"start": str(start_date), "end": str(end_date)},
                "metrics": result
            }
            
        except Exception as e:
            logger.error(f"Error in QueryClientAnalyticsTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)

query_client_analytics_tool = QueryClientAnalyticsTool()
