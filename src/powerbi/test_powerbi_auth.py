from .auth import get_access_token

if __name__ == "__main__":
    token = get_access_token()
    print("✅ Autenticação com Azure/Power BI OK!")
    print(token[:50], "...")