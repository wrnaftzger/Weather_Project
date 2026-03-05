import os
import pyodbc
from sqlalchemy import create_engine

server = "sluweather.database.windows.net"
database = "Weather"
username = "CloudSA651686c0"
driver = "ODBC Driver 18 for SQL Server"

password = os.getenv("AZURE_SQL_PASSWORD")
if password is None:
    raise RuntimeError("AZURE_SQL_PASSWORD not set")

def get_engine():
    conn_str = (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    conn = pyodbc.connect(conn_str)
    return create_engine("mssql+pyodbc://", creator=lambda: conn)


if __name__ == "__main__":
    engine = get_engine()
    print("Connected to Azure SQL Database")