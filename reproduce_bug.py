import requests
import json

BASE_URL = "http://localhost:8000"

def debug_request():
    params = {
        "start_date": "2026-01-29",
        "end_date": "2026-02-28",
        "unidade": "UNIDADE SEDE ESTRELA SERRINHA"
    }
    # No x-database-id header, should default to 1 (Sisal)
    headers = {}
    
    print(f"Calling: {BASE_URL}/management/indicators with params: {params}")
    try:
        response = requests.get(f"{BASE_URL}/management/indicators", params=params, headers=headers, timeout=60)
        print(f"Status Code: {response.status_code}")
        print("Response Body:")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_request()
