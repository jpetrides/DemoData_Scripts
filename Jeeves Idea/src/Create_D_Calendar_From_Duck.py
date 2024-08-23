import os
import duckdb
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode

# DuckDB connection and query
db_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/hotel_reservations.duckdb'
table_name = 'hotel_reservations.main.calendar_reference'

# Output file paths
parquet_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Dim_Calendar.parquet'
hyper_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Dim_Calendar.hyper'

def duckdb_to_parquet():
    conn = duckdb.connect(db_path)
    query = f"COPY (SELECT * FROM {table_name}) TO '{parquet_file_path}' (FORMAT 'parquet')"
    conn.execute(query)
    conn.close()
    print(f"Temporary Parquet file created successfully: {parquet_file_path}")

def parquet_to_hyper():
    # Start the Hyper process
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Create and connect to the Hyper file
        with Connection(endpoint=hyper.endpoint,
                        database=hyper_file_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            
            # Execute command to create table from Parquet file
            connection.execute_command(f"CREATE TABLE Dim_Calendar AS (SELECT * FROM external('{parquet_file_path}'))")
            
            print(f"Hyper file created successfully: {hyper_file_path}")

def delete_temp_parquet():
    try:
        os.remove(parquet_file_path)
        print(f"Temporary Parquet file deleted: {parquet_file_path}")
    except OSError as e:
        print(f"Error deleting temporary Parquet file: {e}")

if __name__ == "__main__":
    try:
        duckdb_to_parquet()
        parquet_to_hyper()
        delete_temp_parquet()
        print("Script completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        # If an error occurs, ensure we try to delete the temporary file
        delete_temp_parquet()