from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.sla import get_sla_data, process_sla_operational
import logging

logger = logging.getLogger(__name__)


class QueryOperationalSLATool:
    """
    Tool para consultar SLA operacional por bancada e aparelho.
    Reutiliza exatamente a lógica do endpoint GET /operacional/sla.
    """

    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna SLA operacional (tempo de liberação de resultados) por unidade, bancada e aparelho.

        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje

        Returns:
            Dicionário com SLA geral, por unidade, por bancada, resumo por unidade e amostras
        """
        conn = None
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).date().strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().date().strftime('%Y-%m-%d')

            logger.info(f"[QueryOperationalSLATool] Fetching operational SLA: {start_date} to {end_date}")
            print(f"🔬 Consultando SLA operacional ({start_date} a {end_date})...")

            # Idêntico ao endpoint GET /operacional/sla em main.py
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            df_sla, df_amostras = get_sla_data(cursor, start_date, end_date)
            release_connection(conn)
            conn = None

            result = process_sla_operational(df_sla, df_amostras)

            # Calcular percentual geral para o log
            total = sum(u.get('quantidade', 0) for u in result.get('resumo_por_unidade', []))
            no_prazo = sum(u.get('no_prazo', 0) for u in result.get('resumo_por_unidade', []))
            pct = round(no_prazo / total * 100, 1) if total > 0 else 0
            print(f"✅ SLA operacional: {pct}% no prazo ({no_prazo}/{total} exames)")

            return {
                "period": {"start": start_date, "end": end_date},
                "sla": result
            }

        except Exception as e:
            logger.error(f"Error in QueryOperationalSLATool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


query_operational_sla_tool = QueryOperationalSLATool()
