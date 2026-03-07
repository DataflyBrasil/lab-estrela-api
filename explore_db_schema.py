import os
os.environ['TDSVER'] = '7.0'
import pymssql
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return pymssql.connect(
        server=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        database=os.getenv('DB_NAME'),
        port=int(os.getenv('DB_PORT', 1433)),
        timeout=30
    )

def explore_tables():
    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)
    
    tables_to_explore = ['sac', 'sat_sat', 'psv', 'usr', 'rdi', 'mot_desct', 'smm', 'osm', 'orc']
    
    for table in tables_to_explore:
        print(f"\n--- Table: {table} ---")
        try:
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
            cols = cursor.fetchall()
            for col in cols:
                print(f"  {col['COLUMN_NAME']} ({col['DATA_TYPE']})")
                
            # Sample data
            print(f"\n  Sample data from {table}:")
            cursor.execute(f"SELECT TOP 3 * FROM {table}")
            rows = cursor.fetchall()
            for row in rows:
                print(f"    {row}")
        except Exception as e:
            print(f"  Error: {e}")
            
    conn.close()

if __name__ == "__main__":
    explore_tables()
