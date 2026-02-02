import requests
from .auth import get_access_token

BASE_URL = "https://api.powerbi.com/v1.0/myorg"


def listar_datasets(workspace_id: str):
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }

    url = f"{BASE_URL}/groups/{workspace_id}/datasets"
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()["value"]