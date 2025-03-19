#When modeling data from scratch as opposed to inheriting existing architecture, I explore the source data to 
#aid preliminary design conceptualization. I check data types, the presence of nulls, uniqueness, whether an
#attribute appears to be dimensional/descriptive/a business entity vs. a fact/quantitative measure. This exploration
#helps surface where DQ checks need to be built in to each model, what values need to be parsed, which attributes
#are candidates for indexing, and many other schema design considerations.

import sqlite3
import pandas as pd

#I want to expore each table, so first I created a list of table names in order to create dataframes iteratively.
tables = ['patients', 'providers', 'encounters', 'referrals', 'employer_network', 'adverse_events']

#A dictionary to store the dataframes.
dataframes = {}

#Connection to the SQLite database the data was ingested into.
conn = sqlite3.connect('./data/icon_health.db')

#Dynamic creation of the dataframes.
for table in tables:
    query = f"SELECT * FROM {table}" 
    dataframes[table] = pd.read_sql_query(query, conn) 

#Database connection closed.
conn.close()

#The data exploration that follows for each dataframe looks at a sample of rows, checks for nulls, 
#checked for uniqueness, examines how data is distributed across categories (counts per category),
#and for quantitative measures runs basic descriptive statistics like measures of central tendency 
#(mean, median) and meeasure of variability (standard deviation, range).
print("View sample of rows:")
print(dataframes['patients'].head())
print(dataframes['providers'].head())
print(dataframes['encounters'].head())
print(dataframes['referrals'].head())
print(dataframes['employer_network'].head())
print(dataframes['adverse_events'].head())

print("Null values in each column: PATIENTS")
print(dataframes['patients'].isnull().sum())
print("\n")

print("Null values in each column: ENCOUNTERS")
print(dataframes['encounters'].isnull().sum())
print("\n")

print("Null values in each column: REFERRALS")
print(dataframes['referrals'].isnull().sum())
print("\n")

print("Null values in each column: EMPLOYER_NETWORK")
print(dataframes['employer_network'].isnull().sum())
print("\n")

print("Null values in each column: ADVERSE_EVENTS")
print(dataframes['adverse_events'].isnull().sum())
print("\n")

# Check for duplicates
print("Number of duplicate rows:")
print(dataframes['patients'].duplicated().sum())

# Check the data types
print("Data types of each column:")
print(dataframes['patients'].dtypes)

# Check duplicates per column (frequency of each value)
print("Duplicates per column:")
for column in dataframes['patients'].columns:
    print(f"\nColumn: {column}")
    value_counts = dataframes['patients'][column].value_counts()  # Get value counts for the column
    duplicates = value_counts[value_counts > 1]  # Filter for values that appear more than once
    print(duplicates)

print("Duplicates per column:")
for column in dataframes['encounters'].columns:
    print(f"\nColumn: {column}")
    value_counts = dataframes['encounters'][column].value_counts()  # Get value counts for the column
    duplicates = value_counts[value_counts > 1]  # Filter for values that appear more than once
    print(duplicates)

dataframes['encounters']['cpt'] = dataframes['encounters']['cpt'].astype(str)

has_whitespace = dataframes['encounters']['cpt'].str.strip() != dataframes['encounters']['cpt']
print(has_whitespace.any())

dataframes['encounters']['cpt'] = dataframes['encounters']['cpt'].astype(str)

# Loop through all columns except 'cpt'
for column in dataframes['encounters'].columns:
    print(f"\nColumn: {column}")
    
    # Get value counts for the column
    value_counts = dataframes['encounters'][column].value_counts()
    
    # Filter for values that appear more than once
    duplicates = value_counts[value_counts > 1]
    
    print(duplicates)

counts = dataframes['employer_network']['employer_id'].value_counts()

# Print the result
print(counts)

counts = dataframes['employer_network'].groupby('employer_id').size()
print(counts)

import sqlite3
import pandas as pd

# Specify the table you want to explore (e.g., 'patients')
table_name = 'aggregate_provider_metrics'

# Connection to the SQLite database where the data is ingested.
conn = sqlite3.connect('./data/icon_health.db')

# Query the single table and store it in a dataframe.
query = f"SELECT * FROM {table_name}" 
df = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Data exploration: Checking the first few rows
print(f"Sample rows from {table_name}:")
print(df.head(10))
