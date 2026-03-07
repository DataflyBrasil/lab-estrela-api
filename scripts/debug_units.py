
import pandas as pd
from database import get_db_connection
import sys

def debug_units_and_mns():
    start_date = "2025-12-01"
    end_date = "2025-12-31"
    
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to DB")
        return

    try:
        cursor = conn.cursor(as_dict=True)
        
        print("--- 1. List of Units (STR) ---")
        cursor.execute("SELECT str_cod, str_nome FROM str")
        units = cursor.fetchall()
        for u in units:
            print(f"ID: {u['str_cod']}, Name: {u['str_nome']}")
            
        print("\n--- 2. MNS Table Columns ---")
        # SQL Server specific way to get columns or just fetch 1 row
        cursor.execute("SELECT TOP 1 * FROM MNS")
        row = cursor.fetchone()
        if row:
            print("Columns found in MNS:")
            print(", ".join(row.keys()))
            
            # Check for unit column candidates
            unit_cols = [k for k in row.keys() if 'str' in k.lower() or 'unid' in k.lower()]
            print(f"Potential Unit Columns in MNS: {unit_cols}")
            
            if unit_cols:
                unit_col = unit_cols[0]
                print(f"\n--- 3. MNS Breakdown by Unit ({unit_col}) ---")
                query = f"""
                SELECT 
                    {unit_col},
                    SUM(mns_vlr) as total_valor
                FROM MNS
                WHERE MNS_DT BETWEEN '{start_date} 00:00:00' AND '{end_date} 23:59:59'
                GROUP BY {unit_col}
                ORDER BY total_valor DESC
                """
                cursor.execute(query)
                res = cursor.fetchall()
                print(pd.DataFrame(res).to_string())
        else:
            print("MNS table is empty or unreadable.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    debug_units_and_mns()
