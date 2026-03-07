from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from app.database import get_db_connection
from app.services.patient import get_patient_data, process_patient_intelligence
import logging

logger = logging.getLogger(__name__)

class QueryPatientIntelligenceTool:
    """
    Tool para consultar inteligência de pacientes (personas, LTV, etc).
    Reutiliza a lógica do endpoint /inteligencia/pacientes.
    """
    
    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Retorna inteligência de pacientes incluindo demografia, socioeconômico e analytics avançado.
        
        Args:
            start_date: Data inicial (YYYY-MM-DD). Default: último ano
            end_date: Data final (YYYY-MM-DD). Default: hoje
            
        Returns:
            Dicionário com inteligência de pacientes
        """
        try:
            # Valores default (último ano para ter volume relevante)
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).date().strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().date().strftime('%Y-%m-%d')
            
            logger.info(f"[QueryPatientIntelligenceTool] Fetching patient intelligence: {start_date} to {end_date}")
            print(f"🔬 Consultando inteligência de pacientes ({start_date} a {end_date})...")
            
            # Reutilizar lógica do endpoint
            conn = get_db_connection()
            cursor = conn.cursor()
            
            df = get_patient_data(cursor, start_date, end_date)
            conn.close()
            
            intelligence = process_patient_intelligence(df)
            
            # Converter Pydantic model para dict se necessário
            if hasattr(intelligence, 'dict'):
                result = intelligence.dict()
            else:
                result = intelligence
            
            print(f"✅ Inteligência de pacientes processada.")
            
            # Formatar para LLM
            return {
                "period": {"start": start_date, "end": end_date},
                "intelligence": result
            }
            
        except Exception as e:
            logger.error(f"Error in QueryPatientIntelligenceTool: {e}", exc_info=True)
            return {"error": str(e)}

query_patient_intelligence_tool = QueryPatientIntelligenceTool()
