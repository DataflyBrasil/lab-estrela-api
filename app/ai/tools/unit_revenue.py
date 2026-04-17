from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.analytics import get_unit_revenue_data, aggregate_unit_revenue_python
import logging

logger = logging.getLogger(__name__)

class QueryUnitRevenueTool:
    """
    Tool para consultar faturamento e atendimentos por unidade.
    Reutiliza exatamente a mesma lógica do endpoint GET /unidades/faturamento.
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
        conn = None
        try:
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

            # Idêntico ao endpoint GET /unidades/faturamento em main.py
            conn = get_db_connection()
            cursor = conn.cursor(as_dict=True)
            df_faturamento, df_atendimentos = get_unit_revenue_data(cursor, start_date, end_date)
            release_connection(conn)
            conn = None

            if df_atendimentos.empty:
                return {
                    "message": "Nenhum dado encontrado para o período",
                    "period": {"start": str(start_date), "end": str(end_date)},
                    "units": []
                }

            result = aggregate_unit_revenue_python(df_faturamento, df_atendimentos)

            units = []
            for r in result:
                caixa = float(r['faturamento'])
                convenio = float(r.get('faturamento_convenio', 0))
                units.append({
                    "name": str(r['unidade']).strip(),
                    "revenue_total": round(caixa + convenio, 2),   # Caixa + Convênio (mesmo critério do "Total Geral" do dashboard)
                    "revenue_caixa": round(caixa, 2),
                    "revenue_convenio": round(convenio, 2),
                    "exams": int(r['atendimentos'])
                })

            total_revenue = sum(u['revenue_total'] for u in units)
            total_caixa = sum(u['revenue_caixa'] for u in units)
            total_convenio = sum(u['revenue_convenio'] for u in units)

            formatted_result = {
                "period": {"start": str(start_date), "end": str(end_date)},
                "total_units": len(units),
                "total_revenue": round(total_revenue, 2),       # Caixa + Convênio
                "total_caixa": round(total_caixa, 2),
                "total_convenio": round(total_convenio, 2),
                "total_exams": sum(u['exams'] for u in units),
                "units": units
            }

            print(f"✅ {len(units)} unidades encontradas. Total Geral (Caixa + Convênio): R$ {total_revenue:,.2f}")

            return formatted_result

        except Exception as e:
            logger.error(f"Error in QueryUnitRevenueTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)

query_unit_revenue_tool = QueryUnitRevenueTool()
