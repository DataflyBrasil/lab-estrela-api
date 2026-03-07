
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

def multiply():
    conn = pymssql.connect(
        server=LOCAL_HOST,
        user=LOCAL_USER,
        password=LOCAL_PASS,
        database=LOCAL_NAME,
        port=LOCAL_PORT,
        autocommit=True
    )
    cursor = conn.cursor()
    
    print("Iniciando explosão de dados (T-SQL)...")
    
    print("Iniciando explosão de dados (Batch-based T-SQL)...")
    
    # Alvo: 100 milhões de SMMs
    target_smm = 100_000_000
    batch_size = 200_000 # 200k por transação
    
    while True:
        cursor.execute("SELECT COUNT(*) FROM SMM")
        current_count = cursor.fetchone()[0]
        print(f"Volume atual SMM: {current_count:,}")
        
        if current_count >= target_smm:
            print("Alvo atingido!")
            break
            
        sql = f"""
        SET NOCOUNT ON;
        DECLARE @MaxOSM INT;
        SELECT @MaxOSM = MAX(osm_num) FROM OSM;
        
        -- Multiplicar OSM
        INSERT INTO OSM (osm_num, osm_serie, osm_pac, osm_cnv, osm_str, osm_dthr, osm_proc, osm_mreq, osm_status)
        SELECT TOP ({batch_size}) osm_num + @MaxOSM, osm_serie, osm_pac, osm_cnv, osm_str, 
               DATEADD(minute, (ABS(CHECKSUM(NEWID())) % 1440), osm_dthr),
               osm_proc, osm_mreq, osm_status
        FROM OSM ORDER BY osm_num;

        -- Multiplicar SMM
        INSERT INTO SMM (smm_osm, smm_osm_serie, smm_num, smm_tpcod, smm_cod, smm_str, smm_rep, smm_med, smm_vlr, SMM_AJUSTE_VLR, smm_dthr_lanc, smm_sfat)
        SELECT TOP ({batch_size * 5}) smm_osm + @MaxOSM, smm_osm_serie, smm_num, smm_tpcod, smm_cod, smm_str, smm_rep, smm_med, 
               smm_vlr, SMM_AJUSTE_VLR, 
               DATEADD(minute, (ABS(CHECKSUM(NEWID())) % 1440), smm_dthr_lanc),
               smm_sfat
        FROM SMM 
        WHERE smm_osm > (@MaxOSM - {batch_size}) -- Pega apenas os novos OSMs para manter consistência
        ORDER BY smm_osm;
        """
        try:
            cursor.execute(sql)
        except Exception as e:
            print(f"\nErro durante batch: {e}. Tentando reconectar...")
            conn.close()
            import time
            time.sleep(10)
            conn = pymssql.connect(server=LOCAL_HOST, user=LOCAL_USER, password=LOCAL_PASS, database=LOCAL_NAME, port=LOCAL_PORT, autocommit=True)
            cursor = conn.cursor()
            continue
        
        # Check count
        cursor.execute("SELECT COUNT(*) FROM SMM")
        count = cursor.fetchone()[0]
        print(f"Total SMM: {count:,}")
        
        if count > 110000000: # Limite de segurança (~10-12GB)
            print("Alvo de volume atingido.")
            break

    conn.close()
    print("🏁 Explosão finalizada.")

if __name__ == "__main__":
    multiply()
