import os
import duckdb
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode

# DuckDB connection and query
db_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/hotel_reservations.duckdb'
table_name = 'hotel_reservations.main.Dim_Palonia_Cust'
print(table_name)
print(db_path)

# Output file paths
parquet_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Dim_Palonia_Cust.parquet'
print(parquet_file_path)

def duckdb_to_parquet():
    try:
        conn = duckdb.connect(db_path)
        print("Connected to database")
        query = f"COPY (SELECT * FROM {table_name}) TO '{parquet_file_path}' (FORMAT 'parquet')"
        print(f"Executing query: {query}")
        conn.execute(query)
        conn.close()
        print(f"Parquet file created successfully: {parquet_file_path}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Call the function
if __name__ == "__main__":
    print("Starting duckdb_to_parquet function")
    duckdb_to_parquet()
    print("duckdb_to_parquet function completed")