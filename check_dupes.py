import pandas as pd, os, urllib.parse
from sqlalchemy import create_engine, text

# ── Check CSV for exact duplicates ──────────────────────────────────────────
df = pd.read_csv('cities_with_distance_to_sea.csv',
                 usecols=['city_ascii', 'iso2'], dtype=str)
df['city_ascii'] = df['city_ascii'].str.strip()
df['iso2']       = df['iso2'].str.strip()

exact = df[df.duplicated(subset=['city_ascii', 'iso2'], keep=False)]
print(f"Total rows      : {len(df)}")
print(f"Exact dupes     : {len(exact)}")
if len(exact):
    print(exact[['city_ascii', 'iso2']].head(6).to_string())

# ── Check DB state ──────────────────────────────────────────────────────────
user = os.environ.get('AZURE_SQL_USER', '')
pwd  = os.environ.get('AZURE_SQL_PASSWORD', '')
if user and pwd:
    p = urllib.parse.quote_plus(
        'Driver={ODBC Driver 18 for SQL Server};'
        'Server=tcp:sluweather.database.windows.net,1433;'
        'Database=Weather;'
        f'UID={user};PWD={pwd};'
        'Encrypt=yes;TrustServerCertificate=no;'
    )
    eng = create_engine(f'mssql+pyodbc:///?odbc_connect={p}', fast_executemany=True)
    with eng.connect() as con:
        cities_n  = con.execute(text('SELECT COUNT(*) FROM dbo.cities')).scalar()
        stg_chk   = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='stg_cities'"
        stg_exists = bool(con.execute(text(stg_chk)).scalar())
        print(f"\ndbo.cities rows : {cities_n}")
        print(f"stg_cities exists in DB: {stg_exists}")
        if cities_n > 0:
            sample = con.execute(text(
                "SELECT TOP 3 city_ascii, iso2 FROM dbo.cities ORDER BY city_ascii"
            )).fetchall()
            print("Sample rows:", sample)
else:
    print('\nEnv vars not set — open a new PowerShell and run: python check_dupes.py')
