import pymssql
import json
import os
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

# Carrega variáveis do .env se existir
load_dotenv()

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
DB_HOST = os.getenv('DB_HOST', 'labestrela.fmddns.com')
DB_NAME = os.getenv('DB_NAME', 'smart')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASS = os.getenv('DB_PASS', 'sa')
DB_PORT = int(os.getenv('DB_PORT', 1433))

# --- CONFIGURAÇÕES DO SSH (OPCIONAL) ---
USE_SSH = os.getenv('USE_SSH', 'false').lower() == 'true'
SSH_HOST = os.getenv('SSH_HOST', 'labestrela.fmddns.com')
SSH_PORT = int(os.getenv('SSH_PORT', 22))
SSH_USER = os.getenv('SSH_USER', 'sa')
SSH_PASSWORD = os.getenv('SSH_PASSWORD', 'sa')

def map_database():
    conn = None
    tunnel = None
    
    # Lista de versões do TDS para tentar (comum em bancos legados)
    tds_versions = ['7.0', '7.1', '7.2', '7.3', '7.4']
    
    for version in tds_versions:
        try:
            # Configura a versão do protocolo TDS para esta tentativa
            os.environ['TDSVER'] = version
            
            if USE_SSH:
                if not tunnel:
                    print(f"Iniciando túnel SSH para {SSH_HOST}...")
                    tunnel = SSHTunnelForwarder(
                        (SSH_HOST, SSH_PORT),
                        ssh_username=SSH_USER,
                        ssh_password=SSH_PASSWORD,
                        remote_bind_address=('127.0.0.1', DB_PORT)
                    )
                    tunnel.start()
                    print(f"Túnel estabelecido na porta local {tunnel.local_bind_port}!")
                
                server_ip = '127.0.0.1'
                server_port = tunnel.local_bind_port
            else:
                server_ip = DB_HOST
                server_port = DB_PORT

            print(f"Tentando conectar ao SQL Server em {server_ip}:{server_port} (TDS {version})...")
            conn = pymssql.connect(
                server=server_ip,
                port=server_port,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                timeout=10,
                login_timeout=10
            )
            print(f"✅ Sucesso! Conectado usando TDS {version}")
            break 
        except Exception as e:
            print(f"❌ Falha com TDS {version}: {str(e).strip()}")
            if conn: conn.close()
            conn = None
            continue

    if not conn:
        print("\n❌ ERRO - Protocolo TDS.")
        if tunnel: tunnel.stop()
        return

    try:
        cursor = conn.cursor(as_dict=True)
        print("Buscando lista de tabelas e colunas ...")
        
        query = """
        SELECT 
            t.name AS table_name,
            c.name AS column_name,
            tp.name AS data_type,
            c.max_length,
            c.is_nullable
        FROM sys.tables t
        JOIN sys.columns c ON t.object_id = c.object_id
        JOIN sys.types tp ON c.user_type_id = tp.user_type_id
        WHERE t.is_ms_shipped = 0
        ORDER BY t.name, c.column_id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        mapping = {}
        for row in rows:
            table = row['table_name']
            if table not in mapping:
                mapping[table] = []
            
            mapping[table].append({
                'column': row['column_name'],
                'type': row['data_type'],
                'max_length': row['max_length'],
                'nullable': row['is_nullable']
            })
        
        print(f"Mapeamento concluído, {len(mapping)} tabelas encontradas.")
        
        output_file = 'db_mapping.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=4, ensure_ascii=False)
        
        print(f"Sucesso! Resultado salvo em: {output_file}")

    except Exception as e:
        print(f"\nERRO DURANTE MAPEAMENTO: {e}")
    finally:
        if conn:
            conn.close()
        if tunnel:
            tunnel.stop()

if __name__ == "__main__":
    map_database()
