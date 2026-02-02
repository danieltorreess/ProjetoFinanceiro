import requests
from .auth import get_access_token

def list_workspaces():
    token = get_access_token()

    url = "https://api.powerbi.com/v1.0/myorg/groups"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print("❌ Erro ao listar workspaces")
        print(response.status_code)
        print(response.text)
        return

    data = response.json()

    print("✅ Workspaces encontrados:\n")
    for ws in data.get("value", []):
        print(f"• Nome: {ws['name']} | ID: {ws['id']}")

if __name__ == "__main__":
    list_workspaces()