"""
Configuração de conexão com Supabase (PostgreSQL) para chat.
Separado do SQL Server que é usado para dados do cliente.
"""
import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Configurações do Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

# Cliente Supabase (singleton)
_supabase_client: Client = None

def get_supabase_client() -> Client:
    """
    Retorna uma instância do cliente Supabase.
    Usa service role key para bypass de RLS (Row Level Security).
    """
    global _supabase_client
    
    if _supabase_client is None:
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
        
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    
    return _supabase_client

def test_supabase_connection():
    """Testa a conexão com o Supabase"""
    try:
        client = get_supabase_client()
        
        # Tenta fazer uma query simples
        response = client.table('chat_conversations').select('count', count='exact').execute()
        
        return True, f"Conexão bem-sucedida! {response.count} conversas no banco."
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    success, result = test_supabase_connection()
    if success:
        print(f"✅ {result}")
    else:
        print(f"❌ Erro de conexão: {result}")
