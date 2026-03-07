
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

def get_local_connection(autocommit=True):
    return pymssql.connect(
        server=LOCAL_HOST,
        user=LOCAL_USER,
        password=LOCAL_PASS,
        database=LOCAL_NAME,
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

# Listas para FKs
UNITS = []
CONVENIOS = []
PATIENTS = []

def seed_base_data(conn):
    cursor = conn.cursor()
    print("Semeando dados base (Unidades, Convênios, Pacientes)...")
    
    # 1. Unidades (STR)
    try:
        cursor.execute("TRUNCATE TABLE STR")
        unidades = []
        for i in range(1, 21):
            cod = str(i).zfill(3)
            nome = f"UNIDADE {cod} - {fake.city()}"
            # O erro anterior foi truncamento em str_str_cod. Vamos usar apenas '01' ou similar.
            # str_resp é obrigatório
            unidades.append((cod, nome, '01', 'RESPONSA')) 
            UNITS.append(cod)
        cursor.executemany("INSERT INTO STR (str_cod, str_nome, str_str_cod, str_resp) VALUES (%s, %s, %s, %s)", unidades)
    except Exception as e: print(f"Erro em STR: {e}")
    
    # 2. Convênios (CNV)
    try:
        cursor.execute("TRUNCATE TABLE CNV")
        cnvs = [('001', 'PARTICULAR', 'C', 'A', '1')] # cnv_tipo é obrigatório
        CONVENIOS.append('001')
        for i in range(2, 11):
            cod = str(i).zfill(3)
            nome = fake.company()
            tipo = random.choice(['C', 'F'])
            cnvs.append((cod, nome, tipo, 'A', '1'))
            CONVENIOS.append(cod)
        cursor.executemany("INSERT INTO CNV (cnv_cod, cnv_nome, cnv_caixa_fatura, cnv_stat, cnv_tipo) VALUES (%s, %s, %s, %s, %s)", cnvs)
    except Exception as e: print(f"Erro em CNV: {e}")
    
    # 3. Pacientes (PAC)
    print("Gerando 10.000 pacientes...")
    try:
        # PAC tbm tem campos obrigatórios possivelmente, mas parece que dreg e reg bastam
        cursor.execute("TRUNCATE TABLE PAC")
        batch_size = 2000
        for i in range(0, 10000, batch_size):
            pacs = []
            for j in range(i, i + batch_size):
                reg = j + 1
                pacs.append((
                    reg, fake.name()[:50], random.choice(['M', 'F']), 
                    fake.date_of_birth(minimum_age=0, maximum_age=90),
                    random.choice(CONVENIOS), reg, # pac_pac_reg para compatibilidade
                    datetime.now() - timedelta(days=random.randint(0, 1000))
                ))
                PATIENTS.append(reg)
            cursor.executemany("INSERT INTO PAC (pac_reg, pac_nome, pac_sexo, pac_nasc, pac_cnv, pac_pac_reg, pac_dreg) VALUES (%s, %s, %s, %s, %s, %s, %s)", pacs)
    except Exception as e: print(f"Erro em PAC: {e}")
    
    conn.commit()
    print("Dados base OK.")

def generate_massive_data(conn, target_osms=500000):
    cursor = conn.cursor()
    print(f"Iniciando geração de volume massivo...")
    
    osm_batch_size = 5000
    smm_batch_size = 10000
    
    current_osm = 0
    total_smm = 0
    
    # BXA generation (Pagamentos)
    print("Gerando BXAs (Pagamentos)...")
    try:
        cursor.execute("TRUNCATE TABLE BXA")
        bxas = []
        for i in range(10000):
            bxas.append((
                1, i+1, random_date(START_DATE, END_DATE), 'sa', datetime.now(), 
                random.choice(CONVENIOS), 1, float(random.randint(50, 5000)), 0, 'L'
            ))
        cursor.executemany("""
            INSERT INTO BXA (BXA_SERIE, BXA_NUM, BXA_DTHR, BXA_USR_LOGIN, BXA_DTHR_REG, BXA_CNV_COD, BXA_EMP_COD, BXA_VALOR_RECEB, BXA_VALOR_GLOSA, BXA_STATUS)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, bxas)
    except Exception as e: print(f"Erro em BXA: {e}")

    print(f"Alvo: {target_osms} Ordens (OSM)")

    for b in range(0, target_osms, osm_batch_size):
        osms = []
        smms = []
        for i in range(b, b + osm_batch_size):
            osm_num = current_osm + (i - b) + 1
            osm_serie = 1
            dt_atend = random_date(START_DATE, END_DATE)
            cnv = random.choice(CONVENIOS)
            pac = random.choice(PATIENTS)
            unit = random.choice(UNITS)
            
            # osm_proc e osm_mreq não podem ser NULL
            osms.append((osm_num, osm_serie, pac, cnv, unit, dt_atend, '1', 'AUTO', None)) 
            
            num_items = random.randint(2, 8)
            for s in range(1, num_items + 1):
                vlr = float(random.randint(100, 1000))
                custo = vlr * -0.1 # Exemplo de ajuste/custo
                # smm_num, smm_tpcod, smm_cod, smm_str, smm_rep, smm_med
                smms.append((osm_num, osm_serie, s, '1', 'EX'+str(random.randint(100, 999)), unit, '0', 0, vlr, custo, dt_atend, 'C')) # smm_sfat = 'C'

        # Inserir OSM
        cursor.executemany("""
            INSERT INTO OSM (osm_num, osm_serie, osm_pac, osm_cnv, osm_str, osm_dthr, osm_proc, osm_mreq, osm_status) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, osms)

        # Inserir SMM
        for k in range(0, len(smms), smm_batch_size):
            chunk = smms[k:k + smm_batch_size]
            cursor.executemany("""
                INSERT INTO SMM (smm_osm, smm_osm_serie, smm_num, smm_tpcod, smm_cod, smm_str, smm_rep, smm_med, smm_vlr, SMM_AJUSTE_VLR, smm_dthr_lanc, smm_sfat) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, chunk)
            total_smm += len(chunk)
        
        conn.commit()
        current_osm += len(osms)
        print(f"Progresso: {current_osm}/{target_osms} OSMs | {total_smm} SMMs geradas...", end="\r")

    print(f"\n🏁 Geração finalizada: {current_osm} OSMs, {total_smm} SMMs.")

if __name__ == "__main__":
    connection = get_local_connection(autocommit=False)
    try:
        # Primeiro garantir que a estrutura existe (usando as colunas que mapeamos)
        # Vamos usar o script de clonagem para criar a estrutura se não existir, filtrando limit 0
        
        seed_base_data(connection)
        generate_massive_data(connection)
    finally:
        connection.close()
