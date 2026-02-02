from .dataset import listar_datasets

WORKSPACE_ID = "3c950437-3a73-4270-b175-d5b8c5edd24f"

datasets = listar_datasets(WORKSPACE_ID)

print("\n📊 Datasets encontrados:\n")

for ds in datasets:
    print(f"• Nome: {ds['name']} | ID: {ds['id']}")