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

# ======================================
# 2️⃣ Criar engine segura
# ======================================
conn_str = (
    f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DB};"
    f"UID={USER};PWD={PWD};Encrypt=no;TrustServerCertificate=yes;"
)
engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}")

# ======================================
# 3️⃣ Criar tabela DIM se não existir
# ======================================
create_table_sql = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'DIM_TIPO_CONTA' AND s.name = 'DIM'
)
BEGIN
    CREATE TABLE DIM.DIM_TIPO_CONTA (
        ID_TIPO_CONTA INT IDENTITY(1,1) PRIMARY KEY,
        NM_TIPO_CONTA NVARCHAR(100) NOT NULL
    );
END;
"""
with engine.begin() as conn:
    conn.execute(sa.text(create_table_sql))

# ======================================
# 4️⃣ Buscar tipos de conta distintos
# ======================================
query = """
SELECT DISTINCT NM_TIPO_CONTA AS NM_TIPO_CONTA
FROM ODS.TB_ODS_SAIDAS_ANALITICO
WHERE NM_TIPO_CONTA IS NOT NULL
UNION
SELECT DISTINCT NM_TIPO_ENTRADA AS NM_TIPO_CONTA
FROM ODS.TB_ODS_ENTRADAS_ANALITICO
WHERE NM_TIPO_ENTRADA IS NOT NULL
UNION
SELECT DISTINCT NM_TIPO_CONTA AS NM_TIPO_CONTA
FROM ODS.TB_ODS_INVESTIMENTO_ANALITICO
WHERE NM_TIPO_CONTA IS NOT NULL;
"""
df = pd.read_sql(query, engine)

if df.empty:
    print("⚠️ Nenhum tipo de conta encontrado nas ODS.")
else:
    print(f"✅ {len(df)} tipos de conta distintos encontrados.")
    df = df.sort_values("NM_TIPO_CONTA").reset_index(drop=True)

    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE DIM.DIM_TIPO_CONTA"))
        df.to_sql("DIM_TIPO_CONTA", conn, schema="DIM", if_exists="append", index=False)
    print("🚀 Carga da DIM_TIPO_CONTA concluída com sucesso!")