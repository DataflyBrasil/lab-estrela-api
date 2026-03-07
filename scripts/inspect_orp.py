from database import get_db_connection

def inspect_orp():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 * FROM ORP")
        columns = [column[0] for column in cursor.description]
        print(columns)
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    inspect_orp()
