import os
os.environ['TDSVER'] = '7.0'
import pymssql
from dotenv import load_dotenv

load_dotenv()

def get_conn(db_id):
    if db_id == "1":
        return pymssql.connect(
            server=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASS'),
            database=os.getenv('DB_NAME'),
            port=int(os.getenv('DB_PORT', 1433)),
            timeout=30
        )
    else:
        return pymssql.connect(
            server=os.getenv('DB_HOST_PAULO_AFONSO'),
            user=os.getenv('DB_USER_PAULO_AFONSO'),
            password=os.getenv('DB_PASS_PAULO_AFONSO'),
            database=os.getenv('DB_NAME_PAULO_AFONSO'),
            port=int(os.getenv('DB_PORT_PAULO_AFONSO', 1433)),
            timeout=30
        )

def check_counts():
    tables = ['pme', 'pre', 'sac', 'sat_sat', 'orc', 'met', 'cre', 'pag', 'STATUS_PGT_ORC', 'bxa', 'rdi', 'osm', 'smm', 'psv', 'usr', 'pac', 'cnv', 'str']
    
    for db_id in ["1", "2"]:
        db_name = "Sisal" if db_id == "1" else "Paulo Afonso"
        print(f"\n=== Database: {db_name} (ID: {db_id}) ===")
        try:
            conn = get_conn(db_id)
            cursor = conn.cursor()
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  {table}: {count}")
                except Exception as e:
                    print(f"  {table}: Error: {e}")
            conn.close()
        except Exception as e:
            print(f"  Connection Error: {e}")

if __name__ == "__main__":
    check_counts()
