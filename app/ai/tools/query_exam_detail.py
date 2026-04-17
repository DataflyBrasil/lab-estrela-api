from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.services.exam_detail import get_exam_details
import logging

logger = logging.getLogger(__name__)


class QueryExamDetailTool:
    """
    Tool para consultar o detalhamento de um exame específico.
    Reutiliza exatamente a lógica do endpoint GET /exames/{exame_cod}/detalhes.
    get_exam_details gerencia sua própria conexão internamente (5 queries paralelas).
    """

    def execute(
        self,
        exame_cod: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        tpcod: str = 'LB'
    ) -> Dict[str, Any]:
        """
        Retorna análise aprofundada de um exame específico: resumo, ranking de médicos,
        unidades, convênios e últimos pacientes atendidos.

        Args:
            exame_cod: Código do exame (ex: 'HEM', 'TSH', 'GLICO')
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje
            tpcod: Tipo do exame (ex: 'LB' para laboratório). Default: 'LB'

        Returns:
            Dicionário com resumo, rankings e últimos pacientes do exame
        """
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            logger.info(f"[QueryExamDetailTool] Fetching exam detail: {exame_cod} ({start_date} to {end_date})")
            print(f"🧪 Consultando detalhes do exame '{exame_cod}' ({start_date} a {end_date})...")

            # Idêntico ao endpoint GET /exames/{exame_cod}/detalhes em main.py
            # get_exam_details gerencia conexão internamente com 5 queries paralelas
            details = get_exam_details(exame_cod, start_date, end_date, tpcod)

            resumo = details.get('resumo', {})
            print(f"✅ Exame '{resumo.get('nome', exame_cod)}': {resumo.get('qtd_total', 0)} realizações, "
                  f"R$ {resumo.get('faturado_bruto', 0):,.2f} faturado")

            return {
                "period": {"start": start_date, "end": end_date},
                "exame_cod": exame_cod,
                "tpcod": tpcod,
                "details": details
            }

        except Exception as e:
            logger.error(f"Error in QueryExamDetailTool: {e}", exc_info=True)
            return {"error": str(e)}


query_exam_detail_tool = QueryExamDetailTool()
