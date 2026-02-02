import subprocess
import sys
import time
from datetime import datetime

# ==========================================
# 🚀 Pipeline de Carga Financeira - Daniel
# ==========================================
jobs = [
    "job_stg_load_saidas.py",
    "job_ods_load_saidas.py",
    "job_stg_load_entradas.py",
    "job_ods_load_entradas.py",
    "job_stg_load_investimento.py",
    "job_ods_load_investimento.py",
    "job_dim_tempo.py",
    "job_dim_tipo_conta.py",
    "job_dim_banco.py",
    "job_dim_plano_conta.py"
]

print("\n==============================================")
print("🚀 INICIANDO PIPELINE DE CARGA FINANCEIRA 🚀")
print(f"🕒 Início: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
print("==============================================\n")

start_pipeline = time.time()

# ==========================================
# Execução das Jobs em sequência
# ==========================================
for job in jobs:
    print(f"▶️ Executando: {job}")
    start_time = time.time()

    result = subprocess.run(
        [sys.executable, f"src/{job}"],
        capture_output=True,
        text=True
    )

    duration = round(time.time() - start_time, 2)

    if result.returncode == 0:
        print(f"✅ {job} concluído com sucesso ({duration}s)\n")
        print(result.stdout)
    else:
        print(f"❌ Erro ao executar {job} ({duration}s)")
        print("Saída de erro:")
        print(result.stderr)
        print("⛔ Execução interrompida. Corrija o erro antes de continuar.\n")
        sys.exit(1)

# ==========================================
# Resumo final
# ==========================================
total_duration = round(time.time() - start_pipeline, 2)
print("==============================================")
print("🎯 TODAS AS ETAPAS EXECUTADAS COM SUCESSO!")
print(f"🕒 Duração total: {total_duration} segundos")
print("🏁 Pipeline finalizado com sucesso.")
print("==============================================\n")

# ==========================================
# 🔄 Refresh do Power BI
# ==========================================
from powerbi.push import refresh_dataset

WORKSPACE_ID = "3c950437-3a73-4270-b175-d5b8c5edd24f"
DATASET_ID = "03fc1e7f-8c53-412f-87e5-f2857411d7ac"

print("🔄 Iniciando refresh do Power BI...")
refresh_dataset(WORKSPACE_ID, DATASET_ID)