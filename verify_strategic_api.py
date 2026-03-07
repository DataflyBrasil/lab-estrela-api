import requests
import json

BASE_URL = "http://localhost:8000"

def test_indicators(db_id, start_date, end_date, unidade=None):
    headers = {"x-database-id": db_id}
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    if unidade:
        params["unidade"] = unidade
    
    print(f"\n--- Testing DB ID: {db_id}, Unidade: {unidade} ---")
    try:
        response = requests.get(f"{BASE_URL}/management/indicators", headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data['success']:
                # Print a subset of indicators
                metrics = data['data']
                print(f"  Success! Total Patients: {metrics['operacional']['pacientes']}")
                print(f"  Revenue (Particular): {metrics['particular_convenio']['particular']:.2f}")
                print(f"  Revenue (Convenio): {metrics['particular_convenio']['convenio']:.2f}")
                print(f"  New Patients: {metrics['novos_pacientes']}")
                print(f"  Ticket Médio: {metrics['operacional']['ticket_medio']:.2f}")
                print(f"  Ranking Medicos (Top 1): {metrics['ranking_medicos'][0]['nome'] if metrics['ranking_medicos'] else 'N/A'}")
                print(f"  Ranking Recepcionistas (Top 1): {metrics['ranking_recepcionistas'][0]['usuario'] if metrics['ranking_recepcionistas'] else 'N/A'}")
            else:
                print(f"  API Error: {data['error']}")
        else:
            print(f"  HTTP Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"  Request Failed: {e}")

if __name__ == "__main__":
    # Test Sisal (Full)
    test_indicators("1", "2024-02-01", "2024-02-28")
    
    # Test PA (Full)
    test_indicators("2", "2024-02-01", "2024-02-28")
    
    # Test PA (Specific Unit)
    test_indicators("2", "2024-02-01", "2024-02-28", "UNIDADE FUTURA PA")
