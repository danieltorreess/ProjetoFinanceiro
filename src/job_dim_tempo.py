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

# Criar tabela DIM_TEMPO se não existir
create_table_sql = """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'DIM_TEMPO'
      AND s.name = 'DIM'
)
BEGIN
    CREATE TABLE DIM.DIM_TEMPO(
        DT_DATA DATE NOT NULL PRIMARY KEY,
        NR_ANO INT NOT NULL,
        NR_MES INT NOT NULL,
        NM_MES NVARCHAR(20) NOT NULL,
        NR_DIA INT NOT NULL,
        NM_DIA_SEMANA NVARCHAR(20) NOT NULL,
        NR_TRIMESTRE INT NOT NULL,
        NR_SEMANA_MES INT NOT NULL
    );
END;
"""

with engine.begin() as conn:
    conn.execute(sa.text(create_table_sql))

# Buscar menor e maior data da ODS
query_dates = """
SELECT 
    MIN(DT_LANCAMENTO) AS DataMin,
    MAX(DT_LANCAMENTO) AS DataMax
FROM ODS.TB_ODS_SAIDAS_ANALITICO;
"""

df_dates = pd.read_sql(query_dates, engine)
data_min = df_dates["DataMin"].iloc[0]
data_max = df_dates["DataMax"].iloc[0]

if pd.isna(data_min) or pd.isna(data_max):
    print("Não há dados na ODS para gerar a dimensão tempo.")
else:
    # Criar calendário
    df_calendario = pd.DataFrame({
        "DT_DATA": pd.date_range(start=data_min, end=data_max, freq="D")
    })

    # Colunas derivadas
    df_calendario["NR_ANO"] = df_calendario["DT_DATA"].dt.year
    df_calendario["NR_MES"] = df_calendario["DT_DATA"].dt.month  # Número do mês

    # Traduzir meses
    meses = {
        "January": "Janeiro",
        "February": "Fevereiro",
        "March": "Março",
        "April": "Abril",
        "May": "Maio",
        "June": "Junho",
        "July": "Julho",
        "August": "Agosto",
        "September": "Setembro",
        "October": "Outubro",
        "November": "Novembro",
        "December": "Dezembro"
    }
    df_calendario["NM_MES"] = df_calendario["DT_DATA"].dt.strftime("%B").map(meses)

    # Dias
    df_calendario["NR_DIA"] = df_calendario["DT_DATA"].dt.day

    # Traduzir dias da semana
    dias_semana = {
        "Monday": "Segunda-feira",
        "Tuesday": "Terça-feira",
        "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira",
        "Friday": "Sexta-feira",
        "Saturday": "Sábado",
        "Sunday": "Domingo"
    }
    df_calendario["NM_DIA_SEMANA"] = df_calendario["DT_DATA"].dt.strftime("%A").map(dias_semana)

    # Trimestre
    df_calendario["NR_TRIMESTRE"] = df_calendario["DT_DATA"].dt.quarter

    # Calcular número da semana dentro do mês
    df_calendario["ANO_MES"] = df_calendario["DT_DATA"].dt.to_period("M")
    df_calendario["NR_SEMANA_MES"] = df_calendario.groupby("ANO_MES")["DT_DATA"] \
        .transform(lambda x: ((x.dt.day - 1) // 7) + 1)
    df_calendario.drop(columns=["ANO_MES"], inplace=True)

    # Inserir na dimensão (evitar duplicados)
    with engine.begin() as conn:
        for _, row in df_calendario.iterrows():
            conn.execute(sa.text("""
                IF NOT EXISTS (
                    SELECT 1 FROM DIM.DIM_TEMPO WHERE DT_DATA = :data
                )
                INSERT INTO DIM.DIM_TEMPO
                (DT_DATA, NR_ANO, NR_MES, NM_MES, NR_DIA, NM_DIA_SEMANA, NR_TRIMESTRE, NR_SEMANA_MES)
                VALUES (:data, :ano, :mes, :nm_mes, :dia, :nm_dia, :trimestre, :semana_mes)
            """), {
                "data": row["DT_DATA"],
                "ano": row["NR_ANO"],
                "mes": row["NR_MES"],
                "nm_mes": row["NM_MES"],
                "dia": row["NR_DIA"],
                "nm_dia": row["NM_DIA_SEMANA"],
                "trimestre": row["NR_TRIMESTRE"],
                "semana_mes": row["NR_SEMANA_MES"]
            })

    print("Carga da dimensão tempo concluída com sucesso!")