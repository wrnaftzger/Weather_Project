import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import os
from getpass import getpass

server = "sluweather.database.windows.net"
database = "Weather"
driver = "ODBC Driver 18 for SQL Server"

username = ""
password = ""

conn_str = (
    f"Driver={{{driver}}};"
    f"Server=tcp:{server},1433;"
    f"Database={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

print("Connecting to Azure SQL Database...")
print(f"Server: {server}")
print(f"Database: {database}")
print(f"Username: {username}")

try:
    conn = pyodbc.connect(conn_str)
    print("Connected successfully!")
    
    engine = create_engine("mssql+pyodbc://", creator=lambda: conn)

    df = pd.read_csv("worldcities.csv")
    df.to_sql("weather_table", engine, if_exists="replace", index=False)

    print("Uploaded")
    
except pyodbc.Error as e:
    print(f"\n[ERROR] Connection failed: {e}")

    
except Exception as e:
    print(f"\n[ERROR] An error occurred: {e}")
    
finally:
    if 'conn' in locals():
        conn.close()
        print("\nConnection closed.")