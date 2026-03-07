from http.server import BaseHTTPRequestHandler
import pymssql
import json
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Configurações do Banco
DB_HOST = os.getenv('DB_HOST', 'labestrela.fmddns.com')
DB_NAME = os.getenv('DB_NAME', 'smart')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASS = os.getenv('DB_PASS', 'sa')
DB_PORT = int(os.getenv('DB_PORT', 1433))
os.environ['TDSVER'] = '7.0'

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            conn = pymssql.connect(
                server=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                timeout=30
            )
            
            cursor = conn.cursor(as_dict=True)
            
            # Query de Análise Consolidada
            query = """
            SELECT TOP 5000
                ISNULL(COALESCE(c.cde_nome, NULLIF(LTRIM(RTRIM(p.pac_cid)), '')), 'NAO INFORMADA') as cidade,
                p.PAC_SEXO as genero,
                DATEDIFF(YEAR, p.PAC_NASC, GETDATE()) as idade,
                s.STR_NOME as unidade,
                o.OSM_DTHR as data,
                ISNULL(i.vlr, 0) as valor
            FROM osm o
            INNER JOIN pac p ON o.OSM_PAC = p.PAC_REG
            INNER JOIN str s ON o.OSM_STR = s.STR_COD
            LEFT JOIN cde c ON LTRIM(RTRIM(p.pac_cid)) = LTRIM(RTRIM(c.cde_cod))
            LEFT JOIN (
                SELECT IPC_OSM_SERIE, IPC_OSM_NUM, SUM(IPC_VALOR) as vlr
                FROM IPC
                GROUP BY IPC_OSM_SERIE, IPC_OSM_NUM
            ) i ON o.OSM_SERIE = i.IPC_OSM_SERIE AND o.OSM_NUM = i.IPC_OSM_NUM
            WHERE o.OSM_DTHR >= DATEADD(MONTH, -12, GETDATE())
            ORDER BY o.OSM_DTHR DESC
            """
            
            cursor.execute(query)
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                self.send_error_response(404, "Nenhum dado encontrado")
                return

            df = pd.DataFrame(rows)
            df['valor'] = df['valor'].apply(lambda x: float(x) if x is not None else 0.0)
            df['cidade'] = df['cidade'].str.strip().str.upper()

            # Agregações para o Dashboard
            city_perf = df.groupby('cidade').agg({
                'valor': ['count', 'sum', 'mean']
            }).reset_index()
            city_perf.columns = ['Cidade', 'Atendimentos', 'Faturamento', 'TicketMedio']
            
            gender_perf = df.groupby('genero').agg({
                'valor': ['count', 'sum', 'mean']
            }).reset_index()
            gender_perf.columns = ['Genero', 'Atendimentos', 'Faturamento', 'TicketMedio']

            result = {
                "success": True,
                "data": {
                    "performance_cidades": city_perf.to_dict(orient='records'),
                    "performance_genero": gender_perf.to_dict(orient='records'),
                    "total_analisado": len(df),
                    "ticket_medio_geral": float(df['valor'].mean())
                }
            }

            self.send_success_response(result)

        except Exception as e:
            self.send_error_response(500, str(e))

    def send_success_response(self, data):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))

    def send_error_response(self, code, message):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"success": False, "error": message}).encode('utf-8'))

def run_etl_logic():
    try:
        conn = pymssql.connect(
            server=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            timeout=30
        )
        
        cursor = conn.cursor(as_dict=True)
        
        query = """
        SELECT TOP 5000
            ISNULL(COALESCE(c.cde_nome, NULLIF(LTRIM(RTRIM(p.pac_cid)), '')), 'NAO INFORMADA') as cidade,
            p.PAC_SEXO as genero,
            DATEDIFF(YEAR, p.PAC_NASC, GETDATE()) as idade,
            s.STR_NOME as unidade,
            o.OSM_DTHR as data,
            ISNULL(i.vlr, 0) as valor
        FROM osm o
        INNER JOIN pac p ON o.OSM_PAC = p.PAC_REG
        INNER JOIN str s ON o.OSM_STR = s.STR_COD
        LEFT JOIN cde c ON LTRIM(RTRIM(p.pac_cid)) = LTRIM(RTRIM(c.cde_cod))
        LEFT JOIN (
            SELECT IPC_OSM_SERIE, IPC_OSM_NUM, SUM(IPC_VALOR) as vlr
            FROM IPC
            GROUP BY IPC_OSM_SERIE, IPC_OSM_NUM
        ) i ON o.OSM_SERIE = i.IPC_OSM_SERIE AND o.OSM_NUM = i.IPC_OSM_NUM
        WHERE o.OSM_DTHR >= DATEADD(MONTH, -12, GETDATE())
        ORDER BY o.OSM_DTHR DESC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return {"success": False, "error": "Nenhum dado encontrado"}

        df = pd.DataFrame(rows)
        df['valor'] = df['valor'].apply(lambda x: float(x) if x is not None else 0.0)
        df['cidade'] = df['cidade'].str.strip().str.upper()

        city_perf = df.groupby('cidade').agg({'valor': ['count', 'sum', 'mean']}).reset_index()
        city_perf.columns = ['Cidade', 'Atendimentos', 'Faturamento', 'TicketMedio']
        
        gender_perf = df.groupby('genero').agg({'valor': ['count', 'sum', 'mean']}).reset_index()
        gender_perf.columns = ['Genero', 'Atendimentos', 'Faturamento', 'TicketMedio']

        return {
            "success": True,
            "data": {
                "performance_cidades": city_perf.to_dict(orient='records'),
                "performance_genero": gender_perf.to_dict(orient='records'),
                "total_analisado": len(df),
                "ticket_medio_geral": float(df['valor'].mean())
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    result = run_etl_logic()
    print(json.dumps(result, ensure_ascii=False))
