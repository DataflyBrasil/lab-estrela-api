import sys
import os
sys.path.append(os.getcwd())

from app.database import test_connection, current_db_id

def debug_conn():
    print(f"Current DB ID: {current_db_id.get()}")
    success, result = test_connection()
    if success:
        print(f"✅ Connection successful: {result}")
    else:
        print(f"❌ Connection failed: {result}")

if __name__ == "__main__":
    debug_conn()
