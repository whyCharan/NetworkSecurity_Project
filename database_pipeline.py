
import os
import sys
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
import mysql.connector

# Import your custom modules
from networksecurity.logging import logger
from networksecurity.exception.exception import NetworkSecurityException

# -----------------------------
# CONFIGURATION
# -----------------------------
CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'Charan@22',
    'database': 'Innomatics',
    'port': '3306',
    'csv_path': '../NetworkSecurity_Project/Network_Data/phisingData.csv',
    'table_name': 'phishing_data'
}

# -----------------------------
# STEP 1: Ensure Database Exists
# -----------------------------
def create_database_if_not_exists(config):
    try:
        logger.info("Checking if database exists...")
        conn = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['database']};")
        conn.close()
        logger.info(f"✅ Database '{config['database']}' is ready.")
    except Exception as e:
        logger.error("❌ Error while creating database.")
        raise NetworkSecurityException(e, sys)


# -----------------------------
# STEP 2: Load CSV File
# -----------------------------
def load_csv_data(csv_path):
    try:
        logger.info(f"Loading CSV from path: {csv_path}")
        if not os.path.exists(csv_path):
            logger.error(f"❌ CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        df = pd.read_csv(csv_path)
        logger.info(f"✅ CSV loaded successfully with {len(df)} rows.")
        return df
    except Exception as e:
        raise NetworkSecurityException(e, sys)


# -----------------------------
# STEP 3: Create SQLAlchemy Engine
# -----------------------------
def get_engine(config):
    try:
        logger.info("Creating SQLAlchemy engine...")
        password_encoded = urllib.parse.quote_plus(config['password'])
        connection_str = (
            f"mysql+mysqlconnector://{config['user']}:{password_encoded}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )
        engine = create_engine(connection_str)
        logger.info("✅ SQLAlchemy engine created successfully.")
        return engine
    except Exception as e:
        raise NetworkSecurityException(e, sys)


# -----------------------------
# STEP 4: Load DataFrame to MySQL
# -----------------------------
def load_to_mysql(df, engine, table_name):
    try:
        logger.info(f"Uploading data to MySQL table '{table_name}' ...")
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"✅ Data uploaded successfully to '{table_name}'.")
    except Exception as e:
        raise NetworkSecurityException(e, sys)


# -----------------------------
# STEP 5: Verify Data in Database
# -----------------------------
def verify_load(engine, table_name):
    try:
        logger.info(f"Verifying data load for table '{table_name}'...")
        query = f"SELECT COUNT(*) AS total FROM {table_name};"
        result = pd.read_sql(query, con=engine)
        total = result['total'][0]
        logger.info(f"✅ Verification complete: {total} rows in table '{table_name}'.")
    except Exception as e:
        raise NetworkSecurityException(e, sys)


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():
    try:
        logger.info("🚀 Starting Data Pipeline Execution...")
        create_database_if_not_exists(CONFIG)
        df = load_csv_data(CONFIG['csv_path'])
        engine = get_engine(CONFIG)
        load_to_mysql(df, engine, CONFIG['table_name'])
        verify_load(engine, CONFIG['table_name'])
        logger.info("🎯 Pipeline executed successfully end-to-end!")
    except Exception as e:
        logger.error("❌ Pipeline execution failed.")
        raise NetworkSecurityException(e, sys)


if __name__ == "__main__":
    main()
