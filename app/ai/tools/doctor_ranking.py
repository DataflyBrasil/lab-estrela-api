from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.analytics import get_commercial_analytics_data, process_commercial_analytics_python
import logging

logger = logging.getLogger(__name__)

class QueryDoctorRankingTool:
    """
    Tool para consultar ranking de médicos por produção.
    Reutiliza a lógica do endpoint /comercial/medicos.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna ranking de médicos solicitantes ordenado por produção.
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com ranking de médicos
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

            logger.info(f"[QueryDoctorRankingTool] Fetching doctor ranking: {start_date} to {end_date}")
            print(f"🩺 Consultando ranking de médicos ({start_date} a {end_date})...")

            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            df_medicos = get_commercial_analytics_data(cursor, start_date, end_date)
            release_connection(conn)
            conn = None
            
            if df_medicos.empty:
                return {
                    "message": "Nenhum dado de médicos encontrado para o período",
                    "period": {"start": str(start_date), "end": str(end_date)},
                    "doctors": []
                }
            
            # process_commercial_analytics_python retorna uma LISTA diretamente
            result = process_commercial_analytics_python(df_medicos)
            
            # Formatar para LLM (converter campos do formato da API)
            formatted_doctors = []
            for doctor in result:
                formatted_doctors.append({
                    "name": doctor['nome'],
                    "crm": doctor.get('crm'),
                    "uf": doctor.get('uf'),
                    "exams": doctor['qtd_pedidos'],
                    "revenue": doctor['valor_total'],
                    "average_ticket": doctor['ticket_medio']
                })
            
            formatted_result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "total_doctors": len(formatted_doctors),
                "total_revenue": sum(d['revenue'] for d in formatted_doctors),
                "total_exams": sum(d['exams'] for d in formatted_doctors),
                "top_10": formatted_doctors[:10],  # Top 10 médicos
                "all_doctors": formatted_doctors  # Todos os médicos
            }
            
            print(f"✅ {len(formatted_doctors)} médicos encontrados. Total: R$ {formatted_result['total_revenue']:,.2f}")
            
            return formatted_result
            
        except Exception as e:
            logger.error(f"Error in QueryDoctorRankingTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)

query_doctor_ranking_tool = QueryDoctorRankingTool()
