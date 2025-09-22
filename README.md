# Foi feita a criação do banco dentro da pasta SQL juntamente das consultas que usei


# Estrutura do projeto

ProjetoFinanceiroSQL/
│
├── SQL/
│   ├── CriacaoBancoSchemas.sql
│   └── CriacaoTabelas.sql
│
├── src/
│   └── test_connection.py
│
├── venv/
├── .env
└── .gitignore


# Criação do ambiente virtual
python -m venv venv
source venv/bin/activate

# Bibliotecas usadas
pip install pandas openpyxl sqlalchemy pyodbc python-dotenv

# Criação dos meus arquivos .env e .gitignore

# Instalar o unixodbc e drivers da Microsoft para carregar as bibliotecas unixODBC - Driver SQL
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update

# Instala o unixODBC + driver ODBC da Microsoft
HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18 unixodbc

# Verificar se o Driver foi instalado
odbcinst -q -d