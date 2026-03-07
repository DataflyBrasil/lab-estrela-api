
import os
import pymssql
from dotenv import load_dotenv

load_dotenv()

PROD_HOST = os.getenv('PROD_DB_HOST', 'labestrela.fmddns.com')
PROD_NAME = os.getenv('PROD_DB_NAME', 'smart')
PROD_USER = os.getenv('PROD_DB_USER', 'sa')
PROD_PASS = os.getenv('PROD_DB_PASS', 'sa')
PROD_PORT = int(os.getenv('PROD_DB_PORT', 1433))

os.environ['TDSVER'] = '7.0'

def test_keys(table_name):
    try:
        conn = pymssql.connect(
            server=PROD_HOST, user=PROD_USER, password=PROD_PASS,
            database=PROD_NAME, port=PROD_PORT, timeout=30, as_dict=True
        )
        cursor = conn.cursor(as_dict=True)
        cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
        row = cursor.fetchone()
        if row:
            print(f"--- KEYS FOR {table_name} ---")
            print(list(row.keys()))
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

test_keys('OSM')
test_keys('SMM')
