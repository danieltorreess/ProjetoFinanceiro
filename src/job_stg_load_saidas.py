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

# Caminho do Excel
excel_path = os.getenv("EXCEL PATH")
sheet_name = "SaidasAnalitico"

# Criar engine SQLAlchemy
engine = sa.create_engine(
    f"mssql+pyodbc://{USER}:{PWD}@{SERVER}/{DB}"
    f"?driver={DRIVER}&Encrypt=no&TrustServerCertificate=yes"
)

# Ler Excel
df = pd.read_excel(excel_path, sheet_name=sheet_name)

print("Colunas originais do Excel:", df.columns.tolist())

# Renomear colunas (Excel → SQL)
rename_map = {
    "Data abertura cartão": "DATA_ABERTURA_CARTAO",
    "Data fechamento": "DATA_FECHAMENTO",
    "Data de lançamento": "DATA_LANCAMENTO",
    "IDSAIDA": "ID_SAIDA",
    "Banco": "BANCO",
    "Tipo de conta": "TIPO_CONTA",
    "Plano de conta": "PLANO_CONTA",
    "Observação": "OBSERVACAO",
    "Parcela atual": "PARCELA_ATUAL",
    "Qtd. Parcelas": "QTD_PARCELAS",
    "Valor": "VALOR"
}
df.rename(columns=rename_map, inplace=True)

print("Colunas após renomear:", df.columns.tolist())

# Tratar valores numéricos (remover "R$", trocar vírgula por ponto)
if "VALOR" in df.columns:
    df["VALOR"] = (
        df["VALOR"]
        .astype(str)
        .str.replace("R$", "", regex=False)
        .str.replace(".", "", regex=False) # Remove milhar
        .str.replace(",", ".", regex=False) # Troca vírgula por ponto
        .astype(float)
    )

# Adicionar coluna de origem
df["SOURCE_FILE"] = excel_path

# Conectar e carregar no banco
with engine.begin() as conn:
    # Limpa Stage antes de inserir
    conn.execute(sa.text("TRUNCATE TABLE STG.TB_STAGE_SAIDAS_ANALITICO"))

    # Carrega DataFrame na tabela
    df.to_sql("TB_STAGE_SAIDAS_ANALITICO", conn, schema="STG", if_exists="append", index=False)

print("Carga concluída com sucesso!")
