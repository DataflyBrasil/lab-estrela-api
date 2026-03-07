from database import get_db_connection

def inspect_pac():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 * FROM PAC")
        columns = [column[0] for column in cursor.description]
        print(f"PAC Columns: {columns}")
        
        # Also check OSM to confirm linkage
        cursor.execute("SELECT TOP 1 OSM_PAC FROM OSM")
        osm_cols = [column[0] for column in cursor.description]
        print(f"OSM Link Columns: {osm_cols}")
        
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    inspect_pac()
