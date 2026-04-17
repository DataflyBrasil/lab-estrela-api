from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.analytics import get_exam_sla_data, calculate_exam_sla_python
import logging

logger = logging.getLogger(__name__)

class QueryExamSLATool:
    """
    Tool para consultar SLA de exames (particular e convênio).
    Reutiliza a lógica dos endpoints /exames/prazo/particular e /exames/prazo/convenio.
    """
    
    def execute(self, 
                sla_type: str = 'all',
                start_date: Optional[str] = None, 
                end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna SLA de exames (percentual no prazo, atrasados, etc).
        
        Args:
            sla_type: Tipo de SLA ('particular', 'convenio', ou 'all'). Default: 'all'
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com dados de SLA por unidade
        """
        conn = None
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
            
            # Validar tipo
            if sla_type not in ['particular', 'convenio', 'all']:
                sla_type = 'all'
            
            logger.info(f"[QueryExamSLATool] Fetching exam SLA ({sla_type}): {start_date} to {end_date}")
            print(f"⏱️  Consultando SLA de exames ({sla_type}) ({start_date} a {end_date})...")
            
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)

            results = {}

            if sla_type == 'all':
                types_to_fetch = ['particular', 'convenio']
            else:
                types_to_fetch = [sla_type]

            for filter_type in types_to_fetch:
                df = get_exam_sla_data(cursor, start_date, end_date, filter_type)

                if df.empty:
                    results[filter_type] = {
                        "message": f"Nenhum dado de SLA {filter_type} encontrado para o período",
                        "units": []
                    }
                else:
                    analytics_result = calculate_exam_sla_python(df)
                    results[filter_type] = {
                        "total_units": len(analytics_result),
                        "units": [
                            {
                                "name": r['unidade'],
                                "percentage_on_time": float(r['percentual_no_prazo']),
                                "total_exams": int(r['total_exames']),
                                "on_time": int(r['no_prazo']),
                                "delayed": int(r['atrasados']),
                                "average_days": float(r['prazo_medio_dias'])
                            } for r in analytics_result
                        ]
                    }

            release_connection(conn)
            conn = None
            
            # Calcular totais gerais
            total_exams = 0
            total_on_time = 0
            
            for filter_type in types_to_fetch:
                if 'units' in results[filter_type]:
                    for unit in results[filter_type]['units']:
                        total_exams += unit['total_exams']
                        total_on_time += unit['on_time']
            
            overall_percentage = (total_on_time / total_exams * 100) if total_exams > 0 else 0
            
            formatted_result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "sla_type": sla_type,
                "overall_metrics": {
                    "total_exams": total_exams,
                    "on_time": total_on_time,
                    "delayed": total_exams - total_on_time,
                    "percentage_on_time": round(overall_percentage, 2)
                },
                "by_type": results
            }
            
            print(f"✅ SLA geral: {overall_percentage:.1f}% no prazo ({total_on_time}/{total_exams} exames)")
            
            return formatted_result
            
        except Exception as e:
            logger.error(f"Error in QueryExamSLATool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)

query_exam_sla_tool = QueryExamSLATool()
