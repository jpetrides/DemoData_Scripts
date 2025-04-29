import duckdb
import os

# Connect to the DuckDB database
db_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/hotel_reservations.duckdb'
conn = duckdb.connect(db_path)

# Define the path where your parquet files are stored
parquet_dir = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Casino/'

# Dictionary mapping parquet files to desired table names
table_mapping = {
    'machines.parquet': 'Dim_Machines',
    'sessions.parquet': 'F_Slots_Sessions',
    'transactions.parquet': 'F_Slots_Transactions'
}

for parquet_file, table_name in table_mapping.items():
    file_path = os.path.join(parquet_dir, parquet_file)
    
    # Create the table and load data from parquet
    conn.execute(f"""
    CREATE OR REPLACE TABLE {table_name} AS 
    SELECT * FROM parquet_scan('{file_path}')
    """)
    
    # Get and print row count
    row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Loaded {row_count} rows into table '{table_name}'")

# Create an index on SessionID in the F_Slots_Transactions table to improve query performance
conn.execute("CREATE INDEX IF NOT EXISTS idx_f_slots_transactions_sessionid ON F_Slots_Transactions(SessionID)")

# Create an index on MachineID in the F_Slots_Sessions table
conn.execute("CREATE INDEX IF NOT EXISTS idx_f_slots_sessions_machineid ON F_Slots_Sessions(MachineID)")

# Close the connection
conn.close()

print("Data loading complete!")