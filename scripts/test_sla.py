import requests
import json

BASE_URL = "http://localhost:8000"

def test_endpoint(endpoint):
    print(f"Testing {endpoint}...")
    try:
        response = requests.get(f"{BASE_URL}{endpoint}")
        if response.status_code == 200:
            data = response.json()
            if data['success']:
                print(f"SUCCESS: {len(data['data'])} units found.")
                if len(data['data']) > 0:
                    print(f"Sample: {data['data'][0]}")
            else:
                print(f"API ERROR: {data['error']}")
        else:
            print(f"HTTP ERROR: {response.status_code}")
    except Exception as e:
        print(f"CONNECTION ERROR: {e}")

if __name__ == "__main__":
    test_endpoint("/health")
    test_endpoint("/exames/prazo/particular")
    test_endpoint("/exames/prazo/convenio")
