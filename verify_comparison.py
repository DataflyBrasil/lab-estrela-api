import requests
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:8000"

def test_metadata():
    print("\n--- Testing Metadata ---")
    response = requests.get(f"{BASE_URL}/comparativo/metadados")
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))

def test_laudos_v2():
    print("\n--- Testing Laudos V2 (1 year back, diario) ---")
    today = datetime.now()
    start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "years_back": 1,
        "granularity": "diario"
    }
    response = requests.get(f"{BASE_URL}/comparativo/laudos_v2", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data['success']}")
        if data['success']:
            print(f"Points returned: {len(data['data']['points'])}")
            print(f"Totals returned: {len(data['data']['totals'])}")
    else:
        print(response.text)

def test_orcamentos():
    print("\n--- Testing Orcamentos (1 year back, diario) ---")
    today = datetime.now()
    start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "years_back": 1,
        "granularity": "diario"
    }
    response = requests.get(f"{BASE_URL}/comparativo/orcamentos", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data['success']}")
        if data['success']:
            print(f"Points returned: {len(data['data']['points'])}")

def test_financeiro():
    print("\n--- Testing Financeiro (1 year back, diario) ---")
    today = datetime.now()
    start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "years_back": 1,
        "granularity": "diario"
    }
    response = requests.get(f"{BASE_URL}/comparativo/financeiro", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data['success']}")
        if data['success']:
            # Check fields
            metrics = data['data']['totals']
            fields = [m['field'] for m in metrics]
            print(f"Fields returned: {fields}")
            if "faturamento" in fields and "faturamento_convenio" in fields:
                print("✅ Financial metrics found: faturamento, faturamento_convenio")

def test_unit_dashboard():
    print("\n--- Testing Unit Dashboard (1 year back) ---")
    today = datetime.now()
    start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    
    params = {
        "unidade": "010101", # Common unit code pattern
        "start_date": start_date,
        "end_date": end_date,
        "years_back": 1
    }
    response = requests.get(f"{BASE_URL}/comparativo/unidade", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data['success']}")
        if data['success']:
            print(f"Unit: {data['data']['unidade_nome']}")
            print(f"Periods: {[p['period_label'] for p in data['data']['comparativos']]}")

def test_ranking():
    print("\n--- Testing Ranking Comparison (Medicos) ---")
    today = datetime.now()
    start_date = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = today.strftime('%Y-%m-%d')
    
    params = {
        "entity_type": "medicos",
        "start_date": start_date,
        "end_date": end_date,
        "years_back": 1
    }
    response = requests.get(f"{BASE_URL}/comparativo/ranking", params=params)
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data['success']}")
        if data['success']:
            print(f"Agents returned: {len(data['data']['agents'])}")

def test_projections():
    print("\n--- Testing Projections ---")
    response = requests.get(f"{BASE_URL}/comparativo/projecao")
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Success: {data['success']}")
        if data['success']:
            print(f"Current Value: {data['data']['current_value']}")
            for p in data['data']['projections']:
                print(f"  {p['label']}: {p['valor']}")

if __name__ == "__main__":
    test_metadata()
    test_unit_dashboard()
    test_ranking()
    test_projections()
