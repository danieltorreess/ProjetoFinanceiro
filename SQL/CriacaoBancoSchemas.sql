-- Cria o banco principal do projeto
CREATE DATABASE DB_FINANCEIRO_DANIEL;
GO

-- Usa o banco recém-criado
USE DB_FINANCEIRO_DANIEL;
GO

-- Cria os schemas para organizar os dados
CREATE SCHEMA STG;
GO

CREATE SCHEMA ODS;
GO

CREATE SCHEMA DIM;
GO