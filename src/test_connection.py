import os
from dotenv import load_dotenv
import sqlalchemy as sa
from urllib.parse import quote_plus

# ==============================
# 1️⃣ Carregar variáveis do .env
# ==============================
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")

# ==============================
# 2️⃣ Montar a connection string
# ==============================
connection_string = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DB};"
    f"UID={USER};"
    f"PWD={PWD};"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
)

# Codifica a string pra evitar erro com caracteres especiais na senha
connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"

# Criar engine SQLAlchemy
engine = sa.create_engine(connection_url)

# ==============================
# 3️⃣ Testar a conexão
# ==============================
try:
    with engine.begin() as conn:
        result = conn.execute(sa.text("SELECT DB_NAME()"))
        print("✅ Conectado com sucesso ao banco:", result.scalar())
except Exception as e:
    print("❌ Erro de conexão:", e)