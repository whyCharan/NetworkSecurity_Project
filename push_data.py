import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import urllib.parse

# --- Step 1: Ensure Database Exists ---
conn = mysql.connector.connect(
    host='127.0.0.1',
    user='root',
    password='Charan@22'
)
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS Innomatics;")
conn.close()
print("✅ Database ready!")

# --- Step 2: Load CSV ---
df = pd.read_csv('../NetworkSecurity_Project/Network_Data/phisingData.csv')

# --- Step 3: Connect via SQLAlchemy ---
username = 'root'
password = urllib.parse.quote_plus('Charan@22')
host = '127.0.0.1'
port = '3306'
database = 'Innomatics'

engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")

# --- Step 4: Push CSV to MySQL ---
df.to_sql('phishing_data', con=engine, if_exists='replace', index=False)
print("✅ CSV loaded successfully into MySQL!")

# --- Step 5: Verify ---
check_df = pd.read_sql("SELECT * FROM phishing_data;", con=engine)
print(len(check_df))
