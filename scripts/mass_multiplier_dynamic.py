
import os
import pymssql
from dotenv import load_dotenv

load_dotenv()

# Configurações Locais
LOCAL_HOST = 'localhost'
LOCAL_NAME = 'smart'
LOCAL_USER = 'sa'
LOCAL_PASS = 'R#256123904'
LOCAL_PORT = 1433

def get_columns(cursor, table):
    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION")
    return [c['COLUMN_NAME'] for c in cursor.fetchall()]

def multiply():
    conn = pymssql.connect(
        server=LOCAL_HOST, user=LOCAL_USER, password=LOCAL_PASS,
        database=LOCAL_NAME, port=LOCAL_PORT, autocommit=True
    )
    cursor = conn.cursor(as_dict=True)
    
    osm_cols = get_columns(cursor, 'OSM')
    smm_cols = get_columns(cursor, 'SMM')
    
    print(f"Colunas OSM detectadas: {len(osm_cols)}")
    print(f"Colunas SMM detectadas: {len(smm_cols)}")
    
    target_smm = 10_000_000
    batch_size = 5_000
    
    while True:
        cursor.execute("SELECT COUNT(*) as cnt FROM SMM")
        current_count = cursor.fetchone()['cnt']
        print(f"Volume atual SMM: {current_count:,}")
        
        if current_count >= target_smm:
            print("Alvo atingido!")
            break
            
        # Build dynamic SQL for OSM
        osm_select = []
        for c in osm_cols:
            if c.lower() == 'osm_num':
                osm_select.append("osm_num + @MaxOSM")
            elif c.lower() in ['osm_dthr', 'osm_dthr_reg', 'osm_dthr_lanc', 'osm_dthr_alter']:
                osm_select.append(f"DATEADD(minute, (ABS(CHECKSUM(NEWID())) % 1440), {c})")
            else:
                osm_select.append(f"[{c}]")
        
        # Build dynamic SQL for SMM
        smm_select = []
        for c in smm_cols:
            if c.lower() == 'smm_osm':
                smm_select.append("smm_osm + @MaxOSM")
            elif c.lower() in ['smm_dthr_exec', 'smm_dthr_lanc', 'smm_dthr_alter']:
                smm_select.append(f"DATEADD(minute, (ABS(CHECKSUM(NEWID())) % 1440), {c})")
            else:
                smm_select.append(f"[{c}]")

        sql = f"""
        SET NOCOUNT ON;
        DECLARE @MaxOSM INT;
        SELECT @MaxOSM = MAX(osm_num) FROM OSM;
        
        -- Multiplicar OSM
        INSERT INTO OSM ({', '.join(f'[{c}]' for c in osm_cols)})
        SELECT TOP ({batch_size}) {', '.join(osm_select)}
        FROM OSM ORDER BY osm_num;

        -- Multiplicar SMM
        INSERT INTO SMM ({', '.join(f'[{c}]' for c in smm_cols)})
        SELECT TOP ({batch_size * 5}) {', '.join(smm_select)}
        FROM SMM 
        WHERE smm_osm > (@MaxOSM - {batch_size})
        ORDER BY smm_osm;
        """
        
        try:
            cursor.execute(sql)
        except Exception as e:
            print(f"\nErro durante batch: {e}. Tentando reconectar...")
            conn.close()
            import time
            time.sleep(15)
            try:
                conn = pymssql.connect(server=LOCAL_HOST, user=LOCAL_USER, password=LOCAL_PASS, database=LOCAL_NAME, port=LOCAL_PORT, autocommit=True)
                cursor = conn.cursor(as_dict=True)
            except: pass
            continue
        
        # Cooldown para não crashar o Docker
        import time
        time.sleep(0.1)

    conn.close()
    print("🏁 Explosão finalizada.")

if __name__ == "__main__":
    multiply()
