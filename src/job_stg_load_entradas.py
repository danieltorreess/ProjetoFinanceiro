import os
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
from urllib.parse import quote_plus

# ======================================
# 1️⃣ Carregar variáveis do .env
# ======================================
load_dotenv()
SERVER = os.getenv("DB_SERVER")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")
excel_path = os.getenv("EXCEL_PATH")
sheet_name = "EntradasAnalitico"

# ======================================
# 2️⃣ Criar engine segura
# ======================================
conn_str = (
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DB};"
    f"UID={USER};PWD={PWD};Encrypt=no;TrustServerCertificate=yes;"
)
engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")

# ======================================
# 3️⃣ Ler Excel
# ======================================
print("📘 Lendo planilha:", excel_path)
df = pd.read_excel(excel_path, sheet_name=sheet_name)
print("Colunas originais:", df.columns.tolist())

rename_map = {
    "Data de entrada": "DATA_ENTRADA",
    "IDENTRADA": "ID_ENTRADA",
    "Banco": "BANCO",
    "Tipo de entrada": "TIPO_ENTRADA",
    "Plano de conta": "PLANO_CONTA",
    "Observação": "OBSERVACAO",
    "Valor": "VALOR",
}
df.rename(columns=rename_map, inplace=True)
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
print("Colunas após renomear:", df.columns.tolist())

# ======================================
# 4️⃣ Tratar valores numéricos
# ======================================
if "VALOR" in df.columns:
    def limpar_valor(v):
        if isinstance(v, (float, int)):
            return float(v)
        if pd.isna(v):
            return 0.0
        v = str(v).replace("R$", "").replace(".", "").replace(",", ".").strip()
        try:
            return float(v)
        except ValueError:
            return 0.0
    df["VALOR"] = df["VALOR"].apply(limpar_valor)

df["SOURCE_FILE"] = excel_path

# ======================================
# 5️⃣ Carga no banco
# ======================================
try:
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE STG.TB_STAGE_ENTRADAS_ANALITICO"))
        df.to_sql("TB_STAGE_ENTRADAS_ANALITICO", conn, schema="STG", if_exists="append", index=False)
        print(f"🚀 {len(df)} registros carregados em STG.TB_STAGE_ENTRADAS_ANALITICO")
except Exception as e:
    print("❌ Erro ao carregar dados na Stage:", e)