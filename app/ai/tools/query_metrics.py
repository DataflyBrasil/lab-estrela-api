import pandas as pd
from typing import Optional, Dict, List, Any
from app.ai.semantic_layer.sql_generator import SqlGenerator
from app.database import get_db_connection, release_connection, current_db_id, DB_CONFIGS
import logging

logger = logging.getLogger(__name__)

class QueryMetricsTool:
    def __init__(self):
        self.sql_generator = SqlGenerator()

    def execute(self, metric: str, dimension: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, filters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Generates and executes a SQL query to fetch the requested metric.

        Args:
            metric: The name of the metric to query (e.g., 'total_revenue').
            dimension: Optional dimension to group by (e.g., 'month', 'unit').
            filters: Optional dictionary of filters (not fully implemented in SQL generator yet).

        Returns:
            A list of dictionaries containing the query results.
        """
        conn = None
        try:
            print(f"\n🧠  Thinking: Analyzing metric '{metric}' by dimension '{dimension}'...")
            logger.info(f"Generating query for metric: {metric}, dimension: {dimension}")

            sql = self.sql_generator.generate_query(metric, dimension, start_date, end_date, filters)

            print(f"📝  Generated SQL:\n    {sql}")
            logger.info(f"Generated SQL: {sql}")

            # Log detalhado do banco que será usado
            db_id = current_db_id.get()
            db_config = DB_CONFIGS.get(db_id, DB_CONFIGS["1"])
            print(f"🔌  [query_metrics] Banco selecionado: ID={db_id} | host={db_config['server']} | db={db_config['database']}")
            logger.info(f"[query_metrics] Conectando ao banco ID={db_id} host={db_config['server']}")

            conn = get_db_connection()
            print(f"✅  [query_metrics] Conexão obtida com sucesso (host={db_config['server']})")
            cursor = conn.cursor(as_dict=True)
            print("🚀  Executing SQL on Database...")
            cursor.execute(sql)
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)

            count = len(df)
            print(f"📊  Query Result: {count} rows fetched.")
            if count > 0:
                print(f"    Sample: {df.iloc[0].to_dict()}")

            # Safety: Limit rows returned to LLM to prevent context overflow
            if count > 100:
                print(f"⚠️  Result too large. Truncating to top 100 rows for AI context.")
                return df.head(100).to_dict(orient='records')

            return df.to_dict(orient='records')

        except Exception as e:
            db_id = current_db_id.get()
            db_config = DB_CONFIGS.get(db_id, DB_CONFIGS["1"])
            print(f"❌  [query_metrics] ERRO ao conectar no banco ID={db_id} host={db_config['server']}: {e}")
            logger.error(f"Error evaluating metrics (DB ID={db_id} host={db_config['server']}): {e}")
            return [{"error": str(e)}]
        finally:
            if conn is not None:
                release_connection(conn)

query_metrics_tool = QueryMetricsTool()
