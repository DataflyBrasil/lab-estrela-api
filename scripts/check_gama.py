from database import get_db_connection
import pandas as pd

conn = get_db_connection()
cursor = conn.cursor(as_dict=True)

print("--- Check GAMA Status ---")
cursor.execute("SELECT cnv_cod, cnv_nome, cnv_caixa_fatura FROM CNV WHERE cnv_nome LIKE '%GAMA%'")
print(pd.DataFrame(cursor.fetchall()).to_string())

conn.close()
