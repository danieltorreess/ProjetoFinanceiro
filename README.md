# Projeto Financeiro SQL

Pipeline financeiro com **SQL Server (Docker)** e **Python** para organizar e automatizar cargas de dados em Staging, ODS, Dimensões e Fatos.  
Futuramente integrado ao Power BI para relatórios automáticos.

---

## Estrutura do projeto
# Iniciando o GitHub dentro do meu projeto
git init
git branch -M main
git remote add origin https://github.com/danieltorreess/ProjetoFinanceiro.git
git config --global user.name "Daniel Torres"
git config --global user.email "cicerodaniel.torres@hotmail.com"
git config --global --list
git add .
git commit -m "Estrutura inicial do projeto financeiro"
git push -u origin main

# Estrutura do projeto

ProjetoFinanceiroSQL/
│
├── SQL/                         # Scripts SQL de criação e consultas
│   ├── CriacaoBancoSchemas.sql
│   ├── CriacaoTabelas.sql
│   └── ConsultasGerais.sql
│
├── src/                         # Scripts de carga e integração Python
│   ├── app.py                   # Executa todas as cargas em sequência
│   ├── job_stg_load_saidas.py
│   ├── job_ods_load_saidas.py
│   ├── job_stg_load_entradas.py
│   ├── job_ods_load_entradas.py
│   ├── job_dim_tempo.py
│   ├── job_dim_tipo_conta.py
│   ├── job_dim_banco.py
│   ├── job_dim_plano_conta.py
│   └── test_connection.py
│
├── venv/                        # Ambiente virtual (ignorado no Git)
│
├── .env                         # Variáveis de ambiente (não versionado)
├── .env.example                 # Exemplo de configuração
├── .gitignore                   # Regras de exclusão do Git
├── requirements.txt             # Dependências do projeto
├── README.md                    # Documentação do projeto
└── MIS33301 - Gerencial Financeiro.pbix  # Dashboard Power BI (ignorado no Git)

---

## Como rodar localmente

1. Clone o repositório:
   ```bash
   git clone https://github.com/danieltorreess/ProjetoFinanceiro.git
   cd ProjetoFinanceiroSQL


# Instalar ODBC e drivers da Microsoft
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18 unixodbc
odbcinst -q -d

# Criação do ambiente virtual
python -m venv venv
source venv/bin/activate

# Bibliotecas usadas
pip install pandas openpyxl sqlalchemy pyodbc python-dotenv
pip install --upgrade pip
pip install requests python-dotenv
pip install msal requests python-dotenv

# Criando meu arquivo Requirements.txt
pip freeze > requirements.txt
pip install --upgrade -r requirements.txt
pip install -r requirements.txt

# Criação dos meus arquivos .env e .gitignore

# Configuração do .env
Copie o arquivo `.env.example` para `.env` e preencha com suas credenciais locais:

```bash
cp .env.example .env

# Instalar o unixodbc e drivers da Microsoft para carregar as bibliotecas unixODBC - Driver SQL
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update

# Instala o unixODBC + driver ODBC da Microsoft
HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18 unixodbc

# Verificar se o Driver foi instalado
odbcinst -q -d

# Colocar o Docker em funcionamento 
Abrir o Docker na máquina e iniciar ele...
Abra o terminal e digite:
docker ps 
docker start mssql2022

# Ver o novo IP do Mac para conexão da VM - PBI e Docker (rodar no terminal do mac)
ipconfig getifaddr en0