from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection
from app.services.analytics import get_unit_revenue_data, aggregate_unit_revenue_python
import logging

logger = logging.getLogger(__name__)

class QueryUnitRevenueTool:
    """
    Tool para consultar faturamento e atendimentos por unidade.
    Reutiliza a lógica do endpoint /unidades/faturamento.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna faturamento e atendimentos por unidade.
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 14 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com dados de faturamento por unidade
        """
        try:
            # Valores default
            if not start_date:
                start_date = (datetime.now() - timedelta(days=14)).date()
            else:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            
            if not end_date:
                end_date = datetime.now().date()
            else:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            logger.info(f"[QueryUnitRevenueTool] Fetching unit revenue: {start_date} to {end_date}")
            print(f"🏥 Consultando faturamento por unidade ({start_date} a {end_date})...")
            
            # Reutilizar lógica do endpoint
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            
            df_caixa, df_fatura, df_atendimentos = get_unit_revenue_data(
                cursor, start_date, end_date
            )
            conn.close()
            
            if df_atendimentos.empty:
                return {
                    "message": "Nenhum dado encontrado para o período",
                    "period": {"start": str(start_date), "end": str(end_date)},
                    "units": []
                }
            
            result = aggregate_unit_revenue_python(df_caixa, df_fatura, df_atendimentos)
            
            # Formatar para LLM
            formatted_result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "total_units": len(result),
                "total_revenue": sum(float(r['faturamento']) for r in result),
                "total_exams": sum(int(r['atendimentos']) for r in result),
                "units": [
                    {
                        "name": str(r['unidade']).strip(),
                        "revenue": float(r['faturamento']),
                        "exams": int(r['atendimentos'])
                    } for r in result
                ]
            }
            
            print(f"✅ {len(result)} unidades encontradas. Total: R$ {formatted_result['total_revenue']:,.2f}")
            
            return formatted_result
            
        except Exception as e:
            logger.error(f"Error in QueryUnitRevenueTool: {e}", exc_info=True)
            return {"error": str(e)}

query_unit_revenue_tool = QueryUnitRevenueTool()
