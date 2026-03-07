
import os
import pymssql
import random
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

# Configurações Locais
LOCAL_HOST = 'localhost'
LOCAL_NAME = 'smart'
LOCAL_USER = 'sa'
LOCAL_PASS = 'R#256123904'
LOCAL_PORT = 1433

os.environ['TDSVER'] = '7.0'

fake = Faker(['pt_BR'])

def get_connection(database=LOCAL_NAME, autocommit=True):
    return pymssql.connect(
        server=LOCAL_HOST,
        user=LOCAL_USER,
        password=LOCAL_PASS,
        database=database,
        port=LOCAL_PORT,
        autocommit=autocommit
    )

def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2026, 1, 31)

def get_table_metadata(cursor, table_name):
    query = """
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = %s
    ORDER BY ORDINAL_POSITION
    """
    cursor.execute(query, (table_name,))
    return cursor.fetchall()

def get_default_value(data_type, col_name, is_nullable):
    if is_nullable == 'YES':
        return None
    
    dt = data_type.lower()
    if dt in ['int', 'smallint', 'tinyint', 'bigint', 'numeric', 'decimal', 'float', 'real']:
        return 0
    if dt in ['char', 'varchar', 'nvarchar', 'nchar', 'text']:
        return '1' # Um valor seguro curto
    if dt in ['datetime', 'date', 'smalldatetime', 'datetime2']:
        return datetime.now()
    if dt == 'bit':
        return 0
    return None

class SmartGenerator:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(as_dict=True)
        self.units = []
        self.cnvs = []
        self.pacs = []

    def populate_table(self, table_name, rows_list):
        if not rows_list: return
        cols_meta = get_table_metadata(self.cursor, table_name)
        col_names = [c['COLUMN_NAME'] for c in cols_meta]
        
        placeholders = ", ".join(["%s"] * len(col_names))
        sql = f"INSERT INTO [{table_name}] ({', '.join(f'[{n}]' for n in col_names)}) VALUES ({placeholders})"
        
        final_data = []
        for row_dict in rows_list:
            row_tuple = []
            # Map keys to lowercase for insensitive lookup
            row_map = {k.lower(): v for k, v in row_dict.items()}
            for col in cols_meta:
                cname = col['COLUMN_NAME']
                cname_lower = cname.lower()
                if cname_lower in row_map:
                    row_tuple.append(row_map[cname_lower])
                else:
                    row_tuple.append(get_default_value(col['DATA_TYPE'], cname, col['IS_NULLABLE']))
            final_data.append(tuple(row_tuple))
            
        try:
            cur = self.conn.cursor()
            cur.executemany(sql, final_data)
        except Exception as e:
            print(f"Erro ao inserir em {table_name}: {e}")
            # print(f"SQL: {sql}")
            raise e

    def seed_base(self):
        print("Semeando base...")
        # STR
        self.cursor.execute("TRUNCATE TABLE STR")
        str_rows = []
        for i in range(1, 21):
            cod = str(i).zfill(3)
            # str_resp é INT
            str_rows.append({'str_cod': cod, 'str_nome': f"UNIDADE {cod}", 'str_str_cod': '01', 'str_resp': 1})
            self.units.append(cod)
        self.populate_table('STR', str_rows)

        # CNV
        self.cursor.execute("TRUNCATE TABLE CNV")
        cnv_rows = []
        for i in range(1, 11):
            cod = str(i).zfill(3)
            # cnv_tipo é INT, CNV_IND_HON_UNICO deve ser 'S'
            cnv_rows.append({
                'cnv_cod': cod, 'cnv_nome': f"CONVENIO {cod}", 'cnv_caixa_fatura': 'C', 
                'cnv_stat': 'A', 'cnv_tipo': 1, 'CNV_IND_HON_UNICO': 'S'
            })
            self.cnvs.append(cod)
        self.populate_table('CNV', cnv_rows)

        # PAC
        self.cursor.execute("TRUNCATE TABLE PAC")
        pac_rows = []
        for i in range(1, 10001):
            pac_rows.append({
                'pac_reg': i, 'pac_nome': fake.name()[:50], 'pac_sexo': random.choice(['M', 'F']),
                'pac_nasc': fake.date_of_birth(), 'pac_cnv': random.choice(self.cnvs),
                'pac_pac_reg': i, 'pac_dreg': datetime.now()
            })
            self.pacs.append(i)
        self.populate_table('PAC', pac_rows)
        self.conn.commit()
        print("Base OK.")

    def generate_mass(self, target_osm=5000):
        print(f"Gerando {target_osm} OSMs...")
        batch_size = 5000
        total_smm = 0
        for b in range(0, target_osm, batch_size):
            osm_batch = []
            smm_batch = []
            for i in range(batch_size):
                osm_num = b + i + 1
                dt = random_date(START_DATE, END_DATE)
                cnv = random.choice(self.cnvs)
                pac = random.choice(self.pacs)
                unit = random.choice(self.units)
                # osm_proc e osm_mreq são INT
                osm_batch.append({
                    'osm_num': osm_num, 'osm_serie': 1, 'osm_pac': pac, 'osm_cnv': cnv, 
                    'osm_str': unit, 'osm_dthr': dt, 'osm_proc': 1, 'osm_mreq': 1
                })
                
                num_items = random.randint(3, 7)
                for s in range(1, num_items + 1):
                    vlr = float(random.randint(50, 500))
                    # smm_rep e smm_med são char/int? SMM_REP: char (10), SMM_MED: int
                    smm_batch.append({
                        'smm_osm': osm_num, 'smm_osm_serie': 1, 'smm_num': s, 'smm_tpcod': '1',
                        'smm_cod': 'EX'+str(random.randint(100, 999)), 'smm_str': unit,
                        'smm_rep': '1', 'smm_med': 1,
                        'smm_vlr': vlr, 'SMM_AJUSTE_VLR': vlr * -0.1, 'smm_dthr_lanc': dt, 'smm_sfat': 'F'
                    })
            
            self.populate_table('OSM', osm_batch)
            self.populate_table('SMM', smm_batch)
            total_smm += len(smm_batch)
            self.conn.commit()
            print(f"Progresso: {b + batch_size}/{target_osm} OSMs | {total_smm} SMMs", end="\r")
        print("\nPronto.")

if __name__ == "__main__":
    conn = get_connection(autocommit=False)
    gen = SmartGenerator(conn)
    try:
        gen.seed_base()
        gen.generate_mass()
    finally:
        conn.close()
