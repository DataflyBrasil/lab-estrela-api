import pymssql
import json
import os
import pandas as pd
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# --- CONFIGURAÇÕES ---
DB_HOST = os.getenv('DB_HOST', 'labestrela.fmddns.com')
DB_NAME = os.getenv('DB_NAME', 'smart')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PASS = os.getenv('DB_PASS', 'sa')
DB_PORT = int(os.getenv('DB_PORT', 1433))
os.environ['TDSVER'] = '7.0'

def run_advanced_analysis():
    print("🚀 Iniciando Análise Avançada (Cidades + Financeiro)...")
    conn = None
    try:
        conn = pymssql.connect(
            server=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            timeout=60
        )
        
        cursor = conn.cursor(as_dict=True)
        
        # Query mais robusta cruzando todas as dimensões solicitadas
        query = """
        SELECT TOP 10000
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
        
        print("📊 Coletando dados (amostra de 3000 registros)...")
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("Nenhum registro encontrado.")
            return

        # Usando Pandas para análise rápida e potente
        df = pd.DataFrame(rows)
        
        # Converter Decimal para float para evitar erro de JSON
        df['valor'] = df['valor'].apply(lambda x: float(x) if x is not None else 0.0)
        
        # Limpeza de nomes de cidades (remover espaços extras e padronizar)
        df['cidade'] = df['cidade'].str.strip().str.upper()

        # 1. Performance por Cidade
        city_perf = df.groupby('cidade').agg({
            'valor': ['count', 'sum', 'mean']
        }).reset_index()
        city_perf.columns = ['Cidade', 'Atendimentos', 'Faturamento_Total', 'Ticket_Medio']
        city_perf = city_perf.sort_values(by='Faturamento_Total', ascending=False)

        # 2. Performance por Gênero
        gender_perf = df.groupby('genero').agg({
            'valor': ['count', 'sum', 'mean']
        }).reset_index()
        gender_perf.columns = ['Genero', 'Atendimentos', 'Faturamento', 'Ticket_Medio']

        print("\n" + "="*50)
        print("🏆 RANKING DE CIDADES POR FATURAMENTO (AMOSTRA)")
        print("="*50)
        for _, row in city_perf.head(15).iterrows():
            print(f"{row['Cidade']:.<30} R$ {row['Faturamento_Total']:>10.2f} (TM: R$ {row['Ticket_Medio']:.2f})")

        print("\n" + "="*50)
        print("👫 PERFIL DE GENDER E TICKET MÉDIO")
        print("="*50)
        for _, row in gender_perf.iterrows():
            print(f"{row['Genero']:.<10} {row['Atendimentos']} atendimentos | Ticket Médio: R$ {row['Ticket_Medio']:.2f}")

        # Salva o resultado final para visualização
        output_file = 'analise_financeira_cidades.json'
        final_data = {
            "performance_cidades": city_perf.to_dict(orient='records'),
            "performance_genero": gender_perf.to_dict(orient='records'),
            "total_analisado": len(df),
            "ticket_medio_geral": float(df['valor'].mean())
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=4, ensure_ascii=False)
            
        print(f"\n✅ Análise detalhada exportada para: {output_file}")

    except Exception as e:
        print(f"❌ Erro na análise: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    run_advanced_analysis()
