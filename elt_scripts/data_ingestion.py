#This script serves to ingest the raw data files into a database. 

import os
import pandas as pd
import sqlite3
from datetime import datetime

#First define the SQLite database path.
db_path = "../data/icon_health.db"

#Then identify the files and their corresponding table names.
data_files = {
    "patients": "../data/patients.csv",
    "providers": "../data/providers.csv",
    "encounters": "../data/encounters_details.csv",
    "referrals": "../data/referrals.csv",
    "adverse_events": "../data/adverse_events.csv",
    "employer_network": "../data/employer_in_network.csv",
}

#Connect to the db.
conn = sqlite3.connect(db_path)

#Ingest the files into the database.
def ingest_data():
    for table, filepath in data_files.items():
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)

                df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                df.to_sql(table, conn, if_exists="append", index=False)

                print(f"✅ Successfully loaded {filepath} into '{table}' table")
            except Exception as e:
                print(f"❌ Error loading {filepath} into '{table}': {e}")
        else:
            print(f"⚠️ File not found: {filepath}")

#Run the ingestion process.
if __name__ == "__main__":
    ingest_data()

#Finally, closes the db connection.
conn.close()
