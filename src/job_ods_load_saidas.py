import os
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

SERVER = os.getenv("DB_SERVER")
DB = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PWD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER")

# Criar engine SQLAlchemy
engine = sa.create_engine(
    f"mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}"
    f"?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
)

# Criar tabela ODS se não existir
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

# Carregar dados da Stage
query_stage = "SELECT * FROM STG.TB_STAGE_SAIDAS_ANALITICO"
df_stage = pd.read_sql(query_stage, engine)

if df_stage.empty:
    print("Stage está vazia, nada para carregar")
else:
    print(f"Linha na Stage: {len(df_stage)}")

    # Verificar IDs já existentes na ODS
    query_ods_ids = "SELECT NR_ID_SAIDA FROM ODS.TB_ODS_SAIDAS_ANALITICO"
    df_ods_ids = pd.read_sql(query_ods_ids, engine)
    existing_ids = set(df_ods_ids["NR_ID_SAIDA"].tolist())

    # Filtrar apenas novos registros (Stage que não estão na ODS)
    df_new = df_stage[~df_stage["ID_SAIDA"].isin(existing_ids)]

    if df_new.empty:
        print("Nenhum registro novo para inserir na ODS.")
    else:
        print(f"Inserindo {len(df_new)} novos registros na ODS...")

    # Remover coluna técnica da Stage que não vai para a ODS
    if "LOAD_DTTM" in df_new.columns:
        df_new = df_new.drop(columns=["LOAD_DTTM"])

        # Remapear colunas da Stage -> ODS
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
        df_new = df_new.rename(columns=rename_map_ods)

        # Inserir os novos registros na ODS
        df_new.to_sql("TB_ODS_SAIDAS_ANALITICO", engine, schema="ODS", if_exists="append", index=False)
        print("Carga na ODS concluída com sucesso!")

    # Truncar a Stage
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE STG.TB_STAGE_SAIDAS_ANALITICO"))
        print("Stage truncada após carga.")