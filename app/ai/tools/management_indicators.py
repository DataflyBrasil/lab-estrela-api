from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.strategic import get_strategic_indicators, get_units
import logging

logger = logging.getLogger(__name__)


class QueryManagementIndicatorsTool:
    """
    Tool para consultar indicadores estratégicos de gestão.
    Reutiliza exatamente a lógica do endpoint GET /management/indicators.
    """

    def execute(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        unidade: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna indicadores estratégicos: mix particular/convênio, crescimento vs. período anterior,
        indicadores operacionais, novos pacientes, conversão de orçamentos, descontos,
        fluxo financeiro e rankings de médicos e recepcionistas.

        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: primeiro dia do mês atual
            end_date: Data final (YYYY-MM-DD). Default: hoje
            unidade: Nome da unidade para filtrar (opcional). Se omitido, retorna consolidado

        Returns:
            Dicionário com todos os indicadores estratégicos
        """
        conn = None
        try:
            hoje = datetime.now()
            if not start_date:
                start_date = hoje.replace(day=1).strftime('%Y-%m-%d')
            if not end_date:
                end_date = hoje.strftime('%Y-%m-%d')

            logger.info(f"[QueryManagementIndicatorsTool] Fetching indicators: {start_date} to {end_date}, unidade={unidade}")
            print(f"📊 Consultando indicadores de gestão ({start_date} a {end_date}, unidade={unidade or 'todas'})...")

            # Idêntico ao endpoint GET /management/indicators em main.py
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            indicators = get_strategic_indicators(cursor, start_date, end_date, unidade)
            release_connection(conn)
            conn = None

            # Log resumido
            operacional = indicators.get('operacional', {})
            print(f"✅ Pacientes: {operacional.get('pacientes', 0)}, "
                  f"Exames: {operacional.get('exames', 0)}, "
                  f"Ticket médio: R$ {operacional.get('ticket_medio', 0):,.2f}")

            return {
                "period": {"start": start_date, "end": end_date},
                "unidade": unidade,
                "indicators": indicators
            }

        except Exception as e:
            logger.error(f"Error in QueryManagementIndicatorsTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


class ListUnitsTool:
    """
    Tool para listar as unidades disponíveis no banco ativo.
    Reutiliza exatamente a lógica do endpoint GET /unidades.
    """

    def execute(self) -> Dict[str, Any]:
        """
        Retorna a lista de todas as unidades ativas no banco de dados selecionado.

        Returns:
            Dicionário com lista de unidades (código e nome)
        """
        conn = None
        try:
            logger.info("[ListUnitsTool] Fetching units list")
            print("🏥 Listando unidades disponíveis...")

            # Idêntico ao endpoint GET /unidades em main.py
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            units = get_units(cursor)
            release_connection(conn)
            conn = None

            # get_units retorna lista de tuplas (cod, nome)
            if units and isinstance(units[0], (list, tuple)):
                formatted = [{"cod": u[0], "nome": u[1]} for u in units]
            else:
                formatted = [{"cod": u.get('str_cod', ''), "nome": u.get('str_nome', '')} for u in units]

            print(f"✅ {len(formatted)} unidades encontradas")

            return {
                "total": len(formatted),
                "units": formatted
            }

        except Exception as e:
            logger.error(f"Error in ListUnitsTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


query_management_indicators_tool = QueryManagementIndicatorsTool()
list_units_tool = ListUnitsTool()
