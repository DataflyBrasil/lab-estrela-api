import time
import pymssql
from app.database import DB_CONFIGS

def test_speed():
    config = DB_CONFIGS["1"]
    conn = pymssql.connect(**config)
    cursor = conn.cursor(as_dict=True)
    
    # Test 1: Simple count
    start = time.time()
    cursor.execute("SELECT COUNT(*) as total FROM OSM WITH(NOLOCK)")
    row = cursor.fetchone()
    print(f"OSM Count: {row['total']} (Time: {time.time() - start:.2f}s)")
    
    # Test 2: Basic join
    start = time.time()
    cursor.execute("""
    SELECT TOP 10 o.osm_num, s.str_nome 
    FROM OSM o WITH(NOLOCK)
    JOIN STR s WITH(NOLOCK) ON o.osm_str = s.str_cod
    """)
    cursor.fetchall()
    print(f"Join Test: Done (Time: {time.time() - start:.2f}s)")
    
    # Test 3: The specific query part that might be slow
    start = time.time()
    cursor.execute("""
    SELECT TOP 100 *
    FROM RCL r WITH(NOLOCK)
    JOIN SMM s WITH(NOLOCK) ON r.RCL_SMM = s.SMM_NUM AND r.RCL_OSM = s.SMM_OSM AND r.RCL_OSM_SERIE = s.SMM_OSM_SERIE
    """)
    cursor.fetchall()
    print(f"RCL-SMM Join: Done (Time: {time.time() - start:.2f}s)")
    
    conn.close()

if __name__ == "__main__":
    test_speed()
