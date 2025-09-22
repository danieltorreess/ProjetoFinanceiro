IF DB_ID(N'FinanceiroPessoal') IS NULL
    CREATE DATABASE FinanceiroPessoal;
GO

USE FinanceiroPessoal;
GO

-- Stage: dados brutos que vêm do Excel
CREATE SCHEMA stg;
GO

-- ODS: dados tratados e padronizados
CREATE SCHEMA ods;
GO

-- Dimensões (datas, categorias, contas, etc.)
CREATE SCHEMA dim;
GO

-- Fatos (transações, saldos, etc.)
CREATE SCHEMA fact;
GO

-- Views finais para consumo (Power BI, relatórios)
CREATE SCHEMA frame;
GO