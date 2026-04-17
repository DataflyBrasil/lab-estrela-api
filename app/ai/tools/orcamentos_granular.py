from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from app.database import get_db_connection, release_connection
from app.services.budget import get_orcamentos_pacientes, get_orcamentos_por_unidade
import logging

logger = logging.getLogger(__name__)


class QueryOrcamentosPacientesTool:
    """
    Tool para listar orçamentos do período com dados de cada paciente.
    Reutiliza exatamente a lógica do endpoint GET /orcamentos/pacientes.
    """

    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna a lista de orçamentos do período com dados de cada paciente
        (nome, categoria, status, se converteu em OS, valor, unidade, usuário).

        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje

        Returns:
            Dicionário com total e lista de orçamentos por paciente
        """
        conn = None
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            logger.info(f"[QueryOrcamentosPacientesTool] Fetching orcamentos pacientes: {start_date} to {end_date}")
            print(f"📋 Consultando orçamentos por paciente ({start_date} a {end_date})...")

            # Idêntico ao endpoint GET /orcamentos/pacientes em main.py
            conn = get_db_connection()
            cursor = conn.cursor()
            items = get_orcamentos_pacientes(cursor, start_date, end_date)
            release_connection(conn)
            conn = None

            # Serializar para dicionário (os itens podem ser objetos Pydantic ou dicts)
            def to_dict(item):
                if hasattr(item, 'dict'):
                    return item.dict()
                if hasattr(item, '__dict__'):
                    return item.__dict__
                return dict(item)

            serialized = [to_dict(i) for i in items]

            # Estatísticas rápidas para contexto
            total = len(serialized)
            convertidos = sum(1 for i in serialized if i.get('convertido'))
            taxa = round(convertidos / total * 100, 1) if total > 0 else 0
            valor_total = sum(float(i.get('valor_total', 0)) for i in serialized)

            print(f"✅ {total} orçamentos. Convertidos: {convertidos} ({taxa}%). Total: R$ {valor_total:,.2f}")

            return {
                "period": {"start": start_date, "end": end_date},
                "total": total,
                "convertidos": convertidos,
                "taxa_conversao": taxa,
                "valor_total": round(valor_total, 2),
                "orcamentos": serialized[:50]  # Limitar para não sobrecarregar o contexto da IA
            }

        except Exception as e:
            logger.error(f"Error in QueryOrcamentosPacientesTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


class QueryOrcamentosUnidadeTool:
    """
    Tool para listar orçamentos de uma unidade específica com flag de conversão.
    Reutiliza exatamente a lógica do endpoint GET /orcamentos/unidade.
    """

    def execute(
        self,
        unidade: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retorna os orçamentos emitidos para uma unidade específica, com dados completos
        do paciente e indicação se o orçamento foi convertido em OS.

        Args:
            unidade: Nome exato da unidade (ex: 'SERRINHA', 'PAULO AFONSO')
            start_date: Data inicial (YYYY-MM-DD). Default: últimos 30 dias
            end_date: Data final (YYYY-MM-DD). Default: hoje

        Returns:
            Dicionário com lista de orçamentos da unidade
        """
        conn = None
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')

            logger.info(f"[QueryOrcamentosUnidadeTool] Fetching orcamentos unidade '{unidade}': {start_date} to {end_date}")
            print(f"📋 Consultando orçamentos da unidade '{unidade}' ({start_date} a {end_date})...")

            # Idêntico ao endpoint GET /orcamentos/unidade em main.py
            conn = get_db_connection()
            cursor = conn.cursor()
            items = get_orcamentos_por_unidade(cursor, unidade, start_date, end_date)
            release_connection(conn)
            conn = None

            def to_dict(item):
                if hasattr(item, 'dict'):
                    return item.dict()
                if hasattr(item, '__dict__'):
                    return item.__dict__
                return dict(item)

            serialized = [to_dict(i) for i in items]

            total = len(serialized)
            convertidos = sum(1 for i in serialized if i.get('convertido'))
            taxa = round(convertidos / total * 100, 1) if total > 0 else 0
            valor_total = sum(float(i.get('valor_total', 0)) for i in serialized)

            print(f"✅ {total} orçamentos em '{unidade}'. Convertidos: {convertidos} ({taxa}%). Total: R$ {valor_total:,.2f}")

            return {
                "period": {"start": start_date, "end": end_date},
                "unidade": unidade,
                "total": total,
                "convertidos": convertidos,
                "taxa_conversao": taxa,
                "valor_total": round(valor_total, 2),
                "orcamentos": serialized[:50]
            }

        except Exception as e:
            logger.error(f"Error in QueryOrcamentosUnidadeTool: {e}", exc_info=True)
            return {"error": str(e)}
        finally:
            if conn is not None:
                release_connection(conn)


query_orcamentos_pacientes_tool = QueryOrcamentosPacientesTool()
query_orcamentos_unidade_tool = QueryOrcamentosUnidadeTool()
