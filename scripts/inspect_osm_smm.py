
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

def get_schema(table_name):
    try:
        conn = pymssql.connect(
            server=PROD_HOST,
            user=PROD_USER,
            password=PROD_PASS,
            database=PROD_NAME,
            port=PROD_PORT,
            timeout=30,
            as_dict=True
        )
        cursor = conn.cursor(as_dict=True)
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, (table_name,))
        cols = cursor.fetchall()
        print(f"--- SCHEMA FOR {table_name} ---")
        for col in cols:
            print(f"{col['COLUMN_NAME']}: {col['DATA_TYPE']} ({col['CHARACTER_MAXIMUM_LENGTH']}) Nullable: {col['IS_NULLABLE']}")
        conn.close()
    except Exception as e:
        print(f"Error inspecting {table_name}: {e}")

get_schema('OSM')
get_schema('SMM')
