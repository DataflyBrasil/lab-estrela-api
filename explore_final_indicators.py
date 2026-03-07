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

def explore():
    conn = get_conn()
    cursor = conn.cursor(as_dict=True)
    
    # Check RCL (Reclamações - potential NPS source)
    try:
        cursor.execute("SELECT COUNT(*) as c FROM rcl")
        print(f"RCL count: {cursor.fetchone()['c']}")
    except:
        print("RCL table missing or error")
        
    # Check PAC columns for registration date
    try:
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'pac' AND COLUMN_NAME LIKE '%DTHR%'")
        cols = cursor.fetchall()
        print(f"PAC DTHR columns: {[c['COLUMN_NAME'] for c in cols]}")
        
        # Sample pac
        cursor.execute("SELECT TOP 3 * FROM pac")
        print("PAC sample:", cursor.fetchone())
    except Exception as e:
        print(f"PAC error: {e}")

    # Check PME (Pre-atendimento)
    try:
        cursor.execute("SELECT COUNT(*) as c FROM pme")
        print(f"PME count: {cursor.fetchone()['c']}")
        
        cursor.execute("SELECT TOP 1 * FROM pme")
        print("PME sample:", cursor.fetchone())
    except:
        print("PME table missing or error")

    conn.close()

if __name__ == "__main__":
    explore()
