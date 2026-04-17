from typing import Optional, Dict, Any
from datetime import datetime
from app.database import get_db_connection, release_connection
from app.services.tecnico import get_laudos_comparativo_data, build_laudos_comparativo
import logging

logger = logging.getLogger(__name__)


class QueryLaudosComparativoTool:
    """
    Tool para consultar laudos liberados comparando o período atual com o mesmo período do ano anterior.
    Reutiliza exatamente a lógica do endpoint GET /tecnico/laudos/comparativo.
    """

    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna laudos liberados dia a dia no período, comparando com o mesmo período do ano anterior.

        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: primeiro dia do mês atual
            end_date: Data final (YYYY-MM-DD). Default: hoje

        Returns:
            Dicionário com totais do período atual e anterior, e comparativo dia a dia
        """
        conn = None
        try:
            hoje = datetime.now()
            if not start_date:
                start_date = hoje.replace(day=1).strftime('%Y-%m-%d')
            if not end_date:
                end_date = hoje.strftime('%Y-%m-%d')

            logger.info(f"[QueryLaudosComparativoTool] Fetching laudos comparativo: {start_date} to {end_date}")
            print(f"📋 Consultando comparativo de laudos ({start_date} a {end_date})...")

            # Idêntico ao endpoint GET /tecnico/laudos/comparativo em main.py
            conn = get_db_connection()
            cursor = conn.cursor()
            df_atual, df_anterior = get_laudos_comparativo_data(cursor, start_date, end_date)
            release_connection(conn)
            conn = None

            payload = build_laudos_comparativo(df_atual, df_anterior)

            totais_atual = payload.get('totais_atual', {})
            totais_anterior = payload.get('totais_anterior', {})
            qtd_atual = totais_atual.get('quantidade', 0)
            qtd_anterior = totais_anterior.get('quantidade', 0)
            var_pct = round((qtd_atual - qtd_anterior) / qtd_anterior * 100, 1) if qtd_anterior else 0
            print(f"✅ Laudos: {qtd_atual} atual vs {qtd_anterior} anterior ({var_pct:+.1f}%)")

            return {
                "period": {"start": start_date, "end": end_date},
                "comparativo": payload
            }

        except Exception as e:
            logger.error(f"Error in QueryLaudosComparativoTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


query_laudos_comparativo_tool = QueryLaudosComparativoTool()
