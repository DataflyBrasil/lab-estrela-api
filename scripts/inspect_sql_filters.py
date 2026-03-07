
from database import get_db_connection
import pandas as pd

def inspect_sql_filters():
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    # 1. Inspect CNV.cnv_caixa_fatura
    print("--- CNV: cnv_caixa_fatura Distribution ---")
    cursor.execute("""
        SELECT cnv_caixa_fatura, COUNT(*) as qtd 
        FROM CNV 
        GROUP BY cnv_caixa_fatura
    """)
    print(pd.DataFrame(cursor.fetchall()).to_string())

    # 2. Inspect STR.str_str_cod
    print("\n--- STR: str_str_cod Distribution ---")
    cursor.execute("""
        SELECT str_str_cod, COUNT(*) as qtd 
        FROM STR 
        GROUP BY str_str_cod
    """)
    print(pd.DataFrame(cursor.fetchall()).to_string())
    
    # 3. Check '01 ' value mapping
    print("\n--- STR where str_str_cod = '01 ' ---")
    cursor.execute("SELECT TOP 5 str_cod, str_nome FROM STR WHERE str_str_cod = '01 '")
    print(pd.DataFrame(cursor.fetchall()).to_string())

    conn.close()

if __name__ == "__main__":
    inspect_sql_filters()
