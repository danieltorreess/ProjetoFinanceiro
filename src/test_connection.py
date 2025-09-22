import os
from dotenv import load_dotenv
import sqlalchemy as sa

# Carregar variáveis do .env
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")

# Criar conexão via SQLAlchemy
engine = sa.create_engine(
    f"mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}"
    f"?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
)

try:
    with engine.begin() as conn:
        result = conn.execute(sa.text("SELECT DB_NAME()"))
        print("Conectado com sucesso ao banco: ", result.scalar())
except Exception as e:
    print("Erro de conexão: ", e)