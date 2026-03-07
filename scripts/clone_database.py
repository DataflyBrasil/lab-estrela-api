
import os
import pymssql
import argparse
from dotenv import load_dotenv
import time

# Carrega variáveis de ambiente
load_dotenv()

# Configurações de Produção (Defaults para Serrinha)
PROD_HOST = os.getenv('PROD_DB_HOST', 'labestrela.fmddns.com')
PROD_NAME = os.getenv('PROD_DB_NAME', 'smart')
PROD_USER = os.getenv('PROD_DB_USER', 'sa')
PROD_PASS = os.getenv('PROD_DB_PASS', 'sa')
PROD_PORT = int(os.getenv('PROD_DB_PORT', 1433))

# Configurações Locais (Docker)
LOCAL_HOST = 'localhost'
LOCAL_NAME = 'smart'
LOCAL_USER = 'sa'
LOCAL_PASS = 'R#256123904'
LOCAL_PORT = 1433

# Fix para pymssql e versões antigas do SQL Server/TDS
os.environ['TDSVER'] = '7.0'

class DatabaseCloner:
    def __init__(self, limit):
        self.limit = limit
        self.prod_conn = None
        self.local_conn = None

    def connect_prod(self):
        print(f"Conectando ao banco de PRODUÇÃO ({PROD_HOST})...")
        self.prod_conn = self._get_connection(PROD_HOST, PROD_USER, PROD_PASS, PROD_NAME, PROD_PORT)
        return self.prod_conn is not None

    def connect_local(self, database=LOCAL_NAME):
        print(f"Conectando ao banco LOCAL ({database})...")
        self.local_conn = self._get_connection(LOCAL_HOST, LOCAL_USER, LOCAL_PASS, database, LOCAL_PORT, autocommit=True)
        return self.local_conn is not None

    def _get_connection(self, host, user, password, database, port, autocommit=False):
        try:
            conn = pymssql.connect(
                server=host,
                user=user,
                password=password,
                database=database,
                port=port,
                timeout=60,
                as_dict=True,
                autocommit=autocommit
            )
            return conn
        except Exception as e:
            print(f"❌ Erro ao conectar em {host}: {e}")
            return None

    def ensure_connections(self):
        """Verifica e restaura as conexões se necessário."""
        if not self.prod_conn or not self._is_alive(self.prod_conn):
            if not self.connect_prod():
                return False
        if not self.local_conn or not self._is_alive(self.local_conn):
            if not self.connect_local():
                return False
        return True

    def _is_alive(self, conn):
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            return True
        except:
            return False

    def create_local_db_if_not_exists(self):
        print("Verificando se o banco local existe...")
        if self.connect_local(database='master'):
            try:
                cursor = self.local_conn.cursor()
                cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{LOCAL_NAME}') BEGIN CREATE DATABASE {LOCAL_NAME}; END")
                print(f"Banco de dados '{LOCAL_NAME}' garantido no servidor local.")
                self.local_conn.close()
                self.local_conn = None
            except Exception as e:
                print(f"Erro ao criar banco local: {e}")
                exit(1)

    def discover_tables(self):
        if not self.ensure_connections(): return []
        query = """
        SELECT t.name AS table_name, s.name AS schema_name, p.rows AS row_count
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0, 1) AND p.rows > 0
        ORDER BY p.rows DESC
        """
        cursor = self.prod_conn.cursor(as_dict=True)
        cursor.execute(query)
        return cursor.fetchall()

    def get_table_schema(self, schema, table):
        query = """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        cursor = self.prod_conn.cursor(as_dict=True)
        cursor.execute(query, (schema, table))
        return cursor.fetchall()

    def generate_create_table_sql(self, schema, table, columns):
        cols_def = []
        for col in columns:
            col_name = col['COLUMN_NAME']
            data_type = col['DATA_TYPE']
            length = col['CHARACTER_MAXIMUM_LENGTH']
            precision = col['NUMERIC_PRECISION']
            scale = col['NUMERIC_SCALE']
            is_nullable = "NULL" if col['IS_NULLABLE'] == 'YES' else "NOT NULL"

            if data_type in ['varchar', 'char', 'nvarchar', 'nchar']:
                type_def = f"{data_type}({length if length != -1 else 'MAX'})"
            elif data_type in ['decimal', 'numeric']:
                type_def = f"{data_type}({precision}, {scale})"
            else:
                type_def = data_type
            cols_def.append(f"[{col_name}] {type_def} {is_nullable}")
        return f"CREATE TABLE [{schema}].[{table}] ({', '.join(cols_def)})"


    def clone_table(self, schema, table, row_count, since=None):
        if not self.ensure_connections():
            print(f"⚠️ Pulo {table}: Sem conexão.")
            return

        # Mapeamento de colunas de data para filtros rápidos
        date_cols = {
            'OSM': 'osm_dthr',
            'SMM': 'smm_dthr', # Algumas versões do smart tem, outras usam join com OSM
            'BXA': 'bxa_dthr',
            'MNS': 'mns_dt',
            'MTE': 'mte_dthr',
            'IPC': 'ipc_dthr',
            'PAC': 'pac_dreg'
        }
        
        filter_sql = ""
        if since and table.upper() in date_cols:
            col = date_cols[table.upper()]
            filter_sql = f"WHERE {col} >= '{since}'"

        limit_str = f"TOP {self.limit}" if self.limit > 0 else ""
        
        # Se tiver filtro, precisamos recalcular o count estimado para log
        current_rows_to_clone = row_count
        if filter_sql:
            print(f"Processando {schema}.{table} (Filtrado desde {since})...", end=" ", flush=True)
        else:
            rows_to_clone = min(self.limit, row_count) if self.limit > 0 else row_count
            print(f"Processando {schema}.{table} ({rows_to_clone} rows)...", end=" ", flush=True)

        try:
            columns = self.get_table_schema(schema, table)
            if not columns:
                print("Skipped (no schema)")
                return

            if not self.ensure_connections(): return

            create_sql = self.generate_create_table_sql(schema, table, columns)
            local_cursor = self.local_conn.cursor()
            local_cursor.execute(f"IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL DROP TABLE [{schema}].[{table}]")
            local_cursor.execute(create_sql)

            # Data fetch
            col_names = [f"[{c['COLUMN_NAME']}]" for c in columns]
            select_query = f"SELECT {limit_str} {', '.join(col_names)} FROM [{schema}].[{table}] WITH (NOLOCK) {filter_sql}"
            
            prod_cursor = self.prod_conn.cursor(as_dict=True)
            prod_cursor.execute(select_query)
            
            # Transfer in chunks
            chunk_size = 10000
            total_cloned = 0
            
            insert_placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO [{schema}].[{table}] VALUES ({insert_placeholders})"

            while True:
                rows = prod_cursor.fetchmany(chunk_size)
                if not rows:
                    break
                
                # Data to insert with case-insensitive lookup
                data_to_insert = []
                for row in rows:
                    tuple_row = []
                    # Create a lowercase map for this row's keys to be safe
                    row_lower = {k.lower(): v for k, v in row.items()}
                    for col in columns:
                        col_name = col['COLUMN_NAME']
                        # Try exact match first, then lowercase
                        val = row.get(col_name, row_lower.get(col_name.lower()))
                        tuple_row.append(val)
                    data_to_insert.append(tuple(tuple_row))
                
                try:
                    local_cursor = self.local_conn.cursor()
                    local_cursor.executemany(insert_sql, data_to_insert)
                    self.local_conn.commit()
                    total_cloned += len(data_to_insert)
                    if total_cloned % 50000 == 0 or len(data_to_insert) < chunk_size:
                        print(f"[{total_cloned}]", end=" ", flush=True)
                except Exception as e:
                    print(f"\n❌ Erro na inserção em {table}: {e}")
                    # Se der erro de coluna não encontrada no filtro, tenta sem filtro
                    if "Invalid column name" in str(e) and filter_sql:
                        print(f"⚠️ Coluna de data não encontrada. Tentando sem filtro...")
                        # Recursão simples para tentar sem filtro se a coluna mapeada falhar
                        return self.clone_table(schema, table, row_count, since=None)
                    break
            
            print(f"✅")
        except Exception as e:
            if "Invalid column name" in str(e) and filter_sql:
                print(f"⚠️ Coluna de data não encontrada. Tentando sem filtro...")
                return self.clone_table(schema, table, row_count, since=None)
            print(f"❌ Erro: {e}")
            if "Not connected" in str(e) or "connection" in str(e).lower():
                self.local_conn = None
                self.prod_conn = None

def main():
    parser = argparse.ArgumentParser(description='Clone SQL Server production DB to local Docker DB.')
    parser.add_argument('--limit', type=int, default=1000, help='Max rows per table (set to 0 for unlimited)')
    parser.add_argument('--tables', type=str, help='Comma separated list of tables to clone (e.g. ORP,PAC)')
    parser.add_argument('--since', type=str, help='Initial date for filtering (YYYY-MM-DD)')
    args = parser.parse_args()
    
    cloner = DatabaseCloner(args.limit)
    cloner.create_local_db_if_not_exists()

    if not cloner.connect_prod() or not cloner.connect_local():
        return

    all_tables = cloner.discover_tables()
    print(f"Encontradas {len(all_tables)} tabelas com dados.")

    if args.tables:
        target_tables = [t.strip().upper() for t in args.tables.split(',')]
        # Primeiro pega os que têm dados
        tables_to_clone = [t for t in all_tables if t['table_name'].upper() in target_tables]
        
        # Depois tenta encontrar os que não têm dados mas foram pedidos
        cloned_names = [t['table_name'].upper() for t in tables_to_clone]
        missing_names = [t for t in target_tables if t not in cloned_names]
        
        for missing in missing_names:
            tables_to_clone.append({'schema_name': 'dbo', 'table_name': missing, 'row_count': 0})
    else:
        tables_to_clone = all_tables

    if args.limit == 0 and not args.since:
        print("🚨 ATENÇÃO: Modo SEM LIMITE e SEM FILTRO ativado. Isso pode levar muito tempo.")
        
    print(f"Iniciando clonagem de {len(tables_to_clone)} tabelas...")
    for tbl in tables_to_clone:
        cloner.clone_table(tbl['schema_name'], tbl['table_name'], tbl['row_count'], since=args.since)

if __name__ == "__main__":
    main()
