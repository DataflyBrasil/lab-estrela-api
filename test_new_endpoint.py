import requests
import json

BASE_URL = "http://localhost:8000"

def test_period_endpoint():
    print("Testing /pacientes/periodo with period...")
    params = {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
        "page": 1,
        "limit": 5
    }
    response = requests.get(f"{BASE_URL}/pacientes/periodo", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Total: {data.get('total')}")
        print(f"Items: {len(data.get('data', []))}")
        if data.get('data'):
            first = data['data'][0]
            print(f"Sample Patient: {first['nome']} - Category: {first['categoria']} - LTV: {first['total_gasto_historico']}")
    else:
        print(f"Error: {response.text}")

def test_full_scan():
    print("\nTesting /pacientes/periodo with full_scan=True...")
    params = {
        "full_scan": True,
        "page": 1,
        "limit": 5
    }
    response = requests.get(f"{BASE_URL}/pacientes/periodo", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Total: {data.get('total')}")
        print(f"Items: {len(data.get('data', []))}")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    try:
        test_period_endpoint()
        test_full_scan()
    except Exception as e:
        print(f"Connection error: {e}")
