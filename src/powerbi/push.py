import requests
from .auth import get_access_token

def refresh_dataset(workspace_id: str, dataset_id: str):
    token = get_access_token()

    url = (
        f"https://api.powerbi.com/v1.0/myorg/"
        f"groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers)

    if response.status_code == 202:
        print("🔄 Refresh do dataset iniciado com sucesso!")
    else:
        print("❌ Erro ao solicitar refresh do dataset")
        print("Status:", response.status_code)
        print("Resposta:", response.text)