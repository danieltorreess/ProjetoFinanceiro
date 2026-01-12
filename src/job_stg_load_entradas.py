import os
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

# ==============================
# 1. Carregar variáveis do .env
# ==============================
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")

# ==============================
# 2. Caminho do Excel
# ==============================
excel_path = os.getenv("EXCEL_PATH")
sheet_name = "EntradasAnalitico"

# ==============================
# 3. Criar engine SQLAlchemy
# ==============================
engine = sa.create_engine(
    f"mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}"
    f"?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
)

# ==============================
# 4. Ler Excel
# ==============================
df = pd.read_excel(excel_path, sheet_name=sheet_name)

print("Colunas originais do Excel:", df.columns.tolist())

# Renomear colunas (Excel → SQL)
rename_map = {
    "Data de entrada": "DATA_ENTRADA",
    "IDENTRADA": "ID_ENTRADA",
    "Banco": "BANCO",
    "Tipo de entrada": "TIPO_ENTRADA",
    "Plano de conta": "PLANO_CONTA",
    "Observação": "OBSERVACAO",
    "Valor": "VALOR"
}
df.rename(columns=rename_map, inplace=True)

# Remover colunas não nomeadas
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

print("Colunas após renomear:", df.columns.tolist())

# Tratar valores numéricos (corrigido)
if "VALOR" in df.columns:
    def limpar_valor(v):
        # Se já for número (float ou int), retorna como está
        if isinstance(v, (float, int)):
            return float(v)
        # Caso contrário, faz limpeza de texto
        if pd.isna(v):
            return 0.0
        v = str(v)
        v = v.replace("R$", "").strip()
        v = v.replace(".", "").replace(",", ".")
        try:
            return float(v)
        except ValueError:
            return 0.0

    df["VALOR"] = df["VALOR"].apply(limpar_valor)

# Adicionar coluna de origem
df["SOURCE_FILE"] = excel_path

# ==============================
# 5. Conectar e carregar no banco
# ==============================
with engine.begin() as conn:
    # Limpa Stage antes de inserir
    conn.execute(sa.text("TRUNCATE TABLE STG.TB_STAGE_ENTRADAS_ANALITICO"))

    # Carrega DataFrame na tabela EXISTENTE
    df.to_sql("TB_STAGE_ENTRADAS_ANALITICO", conn, schema="STG", if_exists="append", index=False)

print(f"✅ Carga concluída com sucesso! {len(df)} registros inseridos na STG.TB_STAGE_ENTRADAS_ANALITICO.")