import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

user = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']
engine = create_engine(f'mssql+pyodbc:///?odbc_connect=Driver={{ODBC Driver 18 for SQL Server}};Server=sluweather.database.windows.net,1433;Database=Weather;UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;')

with engine.connect() as conn:
    # Check table columns
    cols = pd.read_sql(text("SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'linear_model_predictions'"), conn)
    print('Table columns:')
    print(cols)
    
    # Get count
    count = conn.execute(text("SELECT COUNT(*) FROM linear_model_predictions")).scalar()
    print(f'\nRow count: {count}')
    
    # Try dbo prefix
    count2 = conn.execute(text("SELECT COUNT(*) FROM dbo.linear_model_predictions")).scalar()
    print(f'dbo prefix count: {count2}')