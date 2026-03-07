
import pandas as pd
from database import get_db_connection
from analytics_utils import get_financial_analytics_data, process_financial_analytics_python
import sys

def verify_fix_gross():
    start_date = "2025-12-01"
    end_date = "2025-12-31"
    
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    print(f"--- Verifying GROSS Fix for {start_date} to {end_date} ---")
    
    try:
        df_faturamento, df_caixa, total_atendimentos, valor_mte_final = get_financial_analytics_data(cursor, start_date, end_date)
        
        mns_val = df_faturamento['valor'].sum() if not df_faturamento.empty else 0
        print(f"MNS Faturamento Base: R$ {mns_val:.2f}")
        print(f"MTE Final (Gross): R$ {valor_mte_final:.2f}")
        print(f"Estimated Total: R$ {mns_val + valor_mte_final:.2f}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    conn.close()

if __name__ == "__main__":
    verify_fix_gross()
