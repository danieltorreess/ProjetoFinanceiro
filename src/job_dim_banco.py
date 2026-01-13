import os
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

# ======================================
# 1. Carregar variáveis do .env
# ======================================
load_dotenv()
SERVER = os.getenv("DB_SERVER")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")

# ======================================
# 2. Criar engine SQLAlchemy
# ======================================
engine = sa.create_engine(
    f"mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}"
    f"?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
)

# ======================================
# 3. Criar tabela DIM se não existir
# ======================================
create_table_sql = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'DIM_BANCO' AND s.name = 'DIM'
)
BEGIN
    CREATE TABLE DIM.DIM_BANCO (
        ID_BANCO INT IDENTITY(1,1) PRIMARY KEY,
        NM_BANCO NVARCHAR(100) NOT NULL
    );
END;
"""
with engine.begin() as conn:
    conn.execute(sa.text(create_table_sql))

# ======================================
# 4. Buscar bancos distintos da ODS
# ======================================
query = """
SELECT DISTINCT NM_BANCO
FROM ODS.TB_ODS_SAIDAS_ANALITICO
WHERE NM_BANCO IS NOT NULL
"""
df = pd.read_sql(query, engine)

if df.empty:
    print("❌ Nenhum banco encontrado na ODS.")
else:
    print(f"✅ {len(df)} bancos distintos encontrados.")
    df = df.sort_values("NM_BANCO").reset_index(drop=True)

    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE DIM.DIM_BANCO"))
        df.to_sql("DIM_BANCO", conn, schema="DIM", if_exists="append", index=False)
    print("🚀 Carga da DIM_BANCO concluída com sucesso!")