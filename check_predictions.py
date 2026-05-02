import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

user = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']
engine = create_engine(f'mssql+pyodbc:///?odbc_connect=Driver={{ODBC Driver 18 for SQL Server}};Server=sluweather.database.windows.net,1433;Database=Weather;UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;')

with engine.connect() as conn:
    df = pd.read_sql(text('SELECT TOP 5 city, lead_days, predicted_temp, valid_date FROM dbo.linear_model_predictions'), conn)
    print('Predictions:')
    print(df)
    print(f'\nTotal: {len(df)} rows')