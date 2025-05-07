import duckdb

# Define the paths
parquet_file_path = "/Users/jpetrides/Documents/Customers/Hotel/MGM/MGM_CrossChannel_Touchpoints1.parquet"
duckdb_path = "/Users/jpetrides/Documents/Demo Data/Hotels/main/data/hotel_reservations.duckdb"
table_name = "TTH_F_Cross_Channel_Tchpnt"

# Connect to the DuckDB database
con = duckdb.connect(duckdb_path)

try:
    # Create the table from the Parquet file
    con.execute(f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM parquet_scan('{parquet_file_path}')
    """)
    
    print(f"Successfully created table {table_name} from {parquet_file_path}")

    # Optionally, you can verify the data
    result = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    print(f"Number of rows in {table_name}: {result[0]}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    con.close()