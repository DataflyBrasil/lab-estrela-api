import pandas as pd
from typing import Optional, Dict, List, Any
from app.ai.semantic_layer.sql_generator import SqlGenerator
from app.ai.database.connection import get_ai_db_engine
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
        try:
            print(f"\n🧠  Thinking: Analyzing metric '{metric}' by dimension '{dimension}'...")
            logger.info(f"Generating query for metric: {metric}, dimension: {dimension}")
            
            sql = self.sql_generator.generate_query(metric, dimension, start_date, end_date, filters)
            
            print(f"📝  Generated SQL:\n    {sql}")
            logger.info(f"Generated SQL: {sql}")
            
            engine = get_ai_db_engine()
            # Use engine connection
            with engine.connect() as conn:
                print("🚀  Executing SQL on Database...")
                df = pd.read_sql(sql, conn)
                
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
            logger.error(f"Error evaluating metrics: {e}")
            return [{"error": str(e)}]

query_metrics_tool = QueryMetricsTool()
