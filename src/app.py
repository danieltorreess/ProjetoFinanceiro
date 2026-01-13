import subprocess
import sys
import time

# ==========================================
# Lista das jobs em ordem de execução
# ==========================================
jobs = [
    "job_stg_load_saidas.py",
    "job_ods_load_saidas.py",
    "job_stg_load_entradas.py",
    "job_ods_load_entradas.py",
    "job_dim_tempo.py",
    "job_dim_tipo_conta.py",
    "job_dim_banco.py",
    "job_dim_plano_conta.py"
]

print("\n🚀 Iniciando execução completa do pipeline de cargas...\n")

for job in jobs:
    print(f"▶️ Executando {job}...")
    start_time = time.time()
    
    # Executa o script como subprocesso
    result = subprocess.run([sys.executable, f"src/{job}"], capture_output=True, text=True)
    
    # Mostra o output da execução
    if result.returncode == 0:
        print(result.stdout)
        print(f"✅ {job} concluído com sucesso em {round(time.time() - start_time, 2)} segundos.\n")
    else:
        print(f"❌ Erro ao executar {job}:\n{result.stderr}")
        print("⛔ Execução interrompida para análise.\n")
        sys.exit(1)

print("🎯 Todas as cargas foram executadas com sucesso!\n")