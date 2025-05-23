import pandas as pd
import duckdb
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Define paths
csv_dir = "/Users/jpetrides/Documents/Customers/Hotel/MGM"
duckdb_path = "/Users/jpetrides/Documents/Demo Data/Hotels/main/data/hotel_reservations.duckdb"

# Function to convert CSV to Parquet
def csv_to_parquet(csv_file, parquet_file):
    df = pd.read_csv(csv_file)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

# Convert CSV files to Parquet
csv_files = ["Bradbourne_Data.csv"]
parquet_files = [file.replace(".csv", ".parquet") for file in csv_files]

for csv_file, parquet_file in zip(csv_files, parquet_files):
    csv_path = os.path.join(csv_dir, csv_file)
    parquet_path = os.path.join(csv_dir, parquet_file)
    csv_to_parquet(csv_path, parquet_path)
    print(f"Converted {csv_file} to {parquet_file}")

# Connect to DuckDB
conn = duckdb.connect(duckdb_path)

# Function to create table and insert data from Parquet
def insert_parquet_to_duckdb(parquet_file, table_name):
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM parquet_scan('{parquet_file}')")
    print(f"Inserted data into {table_name} table")

# Insert data into DuckDB
table_names = ["Bradbourne_Data"]

for parquet_file, table_name in zip(parquet_files, table_names):
    parquet_path = os.path.join(csv_dir, parquet_file)
    insert_parquet_to_duckdb(parquet_path, table_name)

# Close the connection
conn.close()

# Delete Parquet files
for parquet_file in parquet_files:
    parquet_path = os.path.join(csv_dir, parquet_file)
    os.remove(parquet_path)
    print(f"Deleted {parquet_file}")

print("Process completed. Data has been inserted into DuckDB and Parquet files have been deleted.")