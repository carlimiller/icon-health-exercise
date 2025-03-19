#As data comes in, especially depending the format (e.g., ADT feeds as JSON blobs getting streamed), Fivetran can enforce a basic schema structure
#for ingested data. Normally, if data needed to be unpivoted like the employer_in_network that would be performed in its mapped model (bronze layer)
#as part of the normalization process. SQLite doesn't support this, so this python script is used instead.

import sqlite3
import pandas as pd

#First, connect to the SQLite database (a proxy for storage of raw source data ingested using the data_ingestion.py script). 
conn = sqlite3.connect('./data/icon_health.db')

#Create a dataframe.
employer_network_df = pd.read_sql_query("SELECT * FROM employer_network", conn)

#Close the db connection.
conn.close()

#Quick peek at the data.
print(employer_network_df.head())

#The array of 'in_network_npis' are comma separated, so they can be split and expanded into separate rows.
employer_network_unpivoted = employer_network_df.set_index('employer_id')['in_network_npis'].str.split(',', expand=True).stack().reset_index(name='npi')

# Reset the index and drop the level of the multi-index (if needed)
employer_network_unpivoted = employer_network_unpivoted.drop('level_1', axis=1)

# Remove any extra spaces around the NPIs (if there are any)
employer_network_unpivoted['npi'] = employer_network_unpivoted['npi'].str.strip()

#Add an updated_at column to identify when the data was ingested and written.
employer_network_unpivoted['updated_at'] = pd.to_datetime('now')

#Reconnect to the SQLite db.
conn = sqlite3.connect('./data/icon_health.db')

#Write the unpivoted data to a new table in SQLite. This is what will be queried in the mapped model for this data.
employer_network_unpivoted.to_sql('employer_network_unpivot', conn, if_exists='append', index=False)

#Close the db connection.
conn.close()

#Quick data check to see the intended results.
print(employer_network_unpivoted.head())