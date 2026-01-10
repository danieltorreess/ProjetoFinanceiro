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
# 2. Criar engine SQLAlchemy
# ==============================
engine = sa.create_engine(
    f"mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}"
    f"?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
)

# ==============================
# 3. Criar tabela ODS se não existir
# ==============================
create_table_sql = """
IF NOT EXISTS(
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'TB_ODS_SAIDAS_ANALITICO'
        AND s.name = 'ODS'
)
BEGIN
    CREATE TABLE ODS.TB_ODS_SAIDAS_ANALITICO(
        NR_ID_SAIDA INT NOT NULL PRIMARY KEY,
        DT_ABERTURA_CARTAO DATE NULL,
        DT_FECHAMENTO DATE NULL,
        DT_LANCAMENTO DATE NOT NULL,
        NM_BANCO NVARCHAR(100) NOT NULL,
        NM_TIPO_CONTA NVARCHAR(100) NULL,
        NM_PLANO_CONTA NVARCHAR(200) NULL,
        NM_OBSERVACAO NVARCHAR(500) NULL,
        NR_PARCELA_ATUAL INT NULL,
        NR_QTD_PARCELAS INT NULL,
        NR_VALOR DECIMAL(18,2) NOT NULL,
        NM_SOURCE_FILE NVARCHAR(260) NULL,
        DT_LOAD_DTTM DATETIME2(3) NOT NULL DEFAULT SYSDATETIME()
    );
END;
"""

with engine.begin() as conn:
    conn.execute(sa.text(create_table_sql))

# ==============================
# 4. Ler dados da Stage
# ==============================
query_stage = "SELECT * FROM STG.TB_STAGE_SAIDAS_ANALITICO"
df_stage = pd.read_sql(query_stage, engine)

if df_stage.empty:
    print("❌ Stage está vazia, nada para carregar.")
else:
    print(f"✅ Linhas encontradas na Stage: {len(df_stage)}")

    # ==============================
    # 5. Truncar a ODS antes da carga
    # ==============================
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE ODS.TB_ODS_SAIDAS_ANALITICO"))
        print("🧹 ODS truncada com sucesso.")

    # ==============================
    # 6. Remover colunas técnicas indesejadas
    # ==============================
    if "LOAD_DTTM" in df_stage.columns:
        df_stage = df_stage.drop(columns=["LOAD_DTTM"])

    # ==============================
    # 7. Renomear colunas Stage → ODS
    # ==============================
    rename_map_ods = {
        "ID_SAIDA": "NR_ID_SAIDA",
        "DATA_ABERTURA_CARTAO": "DT_ABERTURA_CARTAO",
        "DATA_FECHAMENTO": "DT_FECHAMENTO",
        "DATA_LANCAMENTO": "DT_LANCAMENTO",
        "BANCO": "NM_BANCO",
        "TIPO_CONTA": "NM_TIPO_CONTA",
        "PLANO_CONTA": "NM_PLANO_CONTA",
        "OBSERVACAO": "NM_OBSERVACAO",
        "PARCELA_ATUAL": "NR_PARCELA_ATUAL",
        "QTD_PARCELAS": "NR_QTD_PARCELAS",
        "VALOR": "NR_VALOR",
        "SOURCE_FILE": "NM_SOURCE_FILE"
    }
    df_stage = df_stage.rename(columns=rename_map_ods)

    # ==============================
    # 8. Inserir todos os registros na ODS
    # ==============================
    df_stage.to_sql("TB_ODS_SAIDAS_ANALITICO", engine, schema="ODS", if_exists="append", index=False)
    print(f"🚀 {len(df_stage)} registros inseridos na ODS com sucesso!")

    # ==============================
    # 9. Truncar a Stage após carga
    # ==============================
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE STG.TB_STAGE_SAIDAS_ANALITICO"))
        print("✅ Stage truncada após carga.")