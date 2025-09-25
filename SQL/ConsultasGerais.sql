-- Verificando tipagem da tabela
USE FinanceiroPessoal;
GO
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'STG'
  AND TABLE_NAME = 'TB_STAGE_SAIDAS_ANALITICO';

-- Validando valores na Stage Saídas
USE FinanceiroPessoal;
GO
SELECT * FROM STG.TB_STAGE_SAIDAS_ANALITICO;
GO

-- Validando valores na ODS Saídas
USE FinanceiroPessoal;
GO
SELECT * FROM ODS.TB_ODS_SAIDAS_ANALITICO;
GO

-- Validando valores na DIM Tempo
USE FinanceiroPessoal;
GO
SELECT * FROM DIM.DIM_TEMPO;
GO