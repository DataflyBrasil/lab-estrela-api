from typing import Optional, Dict, Any
from app.database import get_db_connection, release_connection
from app.services.metas import get_monthly_execution, get_daily_execution
import logging

logger = logging.getLogger(__name__)


class QueryMetasExecucaoTool:
    """
    Tool para consultar execução real mensal e diária.
    Reutiliza exatamente a lógica dos endpoints GET /metas/execucao e GET /metas/execucao/diaria.
    """

    def execute(
        self,
        granularity: str = 'mensal',
        unidade: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna a execução real do laboratório no ano atual (faturamento, pacientes, ticket médio).

        Args:
            granularity: 'mensal' para acumulado por mês no ano (default) ou 'diario' para dias do mês atual
            unidade: Nome da unidade para filtrar (opcional). Se omitido, retorna consolidado de todas as unidades

        Returns:
            Dicionário com execução real (faturamento, pacientes, ticket_avg) por período
        """
        conn = None
        try:
            if granularity not in ('mensal', 'diario'):
                granularity = 'mensal'

            logger.info(f"[QueryMetasExecucaoTool] Fetching execution ({granularity}), unidade={unidade}")
            print(f"📈 Consultando execução real ({granularity}, unidade={unidade or 'todas'})...")

            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)

            if granularity == 'diario':
                # Idêntico ao endpoint GET /metas/execucao/diaria em main.py
                data = get_daily_execution(cursor, unidade)
                label = "dias do mês atual"
            else:
                # Idêntico ao endpoint GET /metas/execucao em main.py
                data = get_monthly_execution(cursor, unidade)
                label = "meses do ano atual"

            release_connection(conn)
            conn = None

            total_revenue = sum(float(d.get('revenue', 0)) for d in data)
            total_patients = sum(int(d.get('patients', 0)) for d in data)

            print(f"✅ {len(data)} {label}. Faturamento total: R$ {total_revenue:,.2f}, Pacientes: {total_patients}")

            return {
                "granularity": granularity,
                "unidade": unidade,
                "total_revenue": round(total_revenue, 2),
                "total_patients": total_patients,
                "data": data
            }

        except Exception as e:
            logger.error(f"Error in QueryMetasExecucaoTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


query_metas_execucao_tool = QueryMetasExecucaoTool()
