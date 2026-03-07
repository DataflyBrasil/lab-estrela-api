import os
os.environ['TDSVER'] = '7.0'
import pymssql
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return pymssql.connect(
        server=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        database=os.getenv('DB_NAME'),
        port=int(os.getenv('DB_PORT', 1433)),
        timeout=30
    )

def deep_explore():
    conn = get_conn()
    cursor = conn.cursor(as_dict=True)
    
    # 1. Check Satisfaction/NPS
    for t in ['sat', 'esoc_sat', 'ect_esoc_sat']:
        try:
            cursor.execute(f"SELECT COUNT(*) as c FROM {t}")
            print(f"Table {t} count: {cursor.fetchone()['c']}")
        except:
            print(f"Table {t} not found")

    # 2. Check Orçamentos/PME (Pre-atendimento)
    try:
        cursor.execute("SELECT TOP 5 * FROM pme")
        rows = cursor.fetchall()
        print("\nPME sample (Pre-atendimento):", rows)
    except:
        print("PME error")

    # 3. Check Cortesias (smm_vlr = 0)
    try:
        cursor.execute("SELECT COUNT(*) as c FROM smm WHERE smm_vlr = 0")
        print(f"\nSMM records with vlr=0 (Cortesias?): {cursor.fetchone()['c']}")
    except:
        print("SMM vlr check error")

    # 4. Check Usuarios/Colaboradores
    try:
        cursor.execute("SELECT TOP 5 usr_login, usr_nome FROM usr")
        print("\nUSR sample:", cursor.fetchall())
    except:
        print("USR error")

    conn.close()

if __name__ == "__main__":
    deep_explore()
