import duckdb
import os

# Connect to the DuckDB database
db_path = '/Users/jpetrides/Documents/Demo Data/Hotels/main/data/hotel_reservations.duckdb'
conn = duckdb.connect(db_path)

# Define the path where your parquet files are stored
parquet_dir = '~/Documents/customers/hotel/mgm'

# Dictionary mapping parquet files to desired table names
table_mapping = {
    'F_Engagement_Events.parquet': 'F_TTH_Web_Engagement_Events',
    'F_events_reservations.parquet': 'F_TTH_Web_Events_reservations',
    'F_Page_View.parquet': 'F_TTH_Web_Page_view'
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
#conn.execute("CREATE INDEX IF NOT EXISTS idx_f_slots_transactions_sessionid ON F_Slots_Transactions(SessionID)")

# Create an index on MachineID in the F_Slots_Sessions table
#conn.execute("CREATE INDEX IF NOT EXISTS idx_f_slots_sessions_machineid ON F_Slots_Sessions(MachineID)")

# Close the connection
conn.close()

print("Data loading complete!")




def create_event_reference_table():
    """Create the event reference table in DuckDB."""
    print("Creating event reference table...")
    
    # Event reference data
    event_ref_data = [
        (2, "Login"),
        (3, "Add to Cart"),
        (4, "Property View"),
        (5, "Property Details"),
        (6, "remove from cart"),
        (7, "search"),
        (8, "date selection"),
        (9, "add payment method"),
        (10, "booking"),
        (11, "view entertainment"),
        (12, "view dining")
    ]
    
    # Path to your DuckDB file
    db_path = '/Users/jpetrides/Documents/Demo Data/Hotels/main/data/hotel_reservations.duckdb'
    
    # Make sure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # Create the event reference table
    conn.execute("""
    CREATE TABLE IF NOT EXISTS D_TTH_Web_Event_Ref (
        Event_ID INTEGER,
        Event_Name VARCHAR
    )
    """)
    
    # Check if table already has data and clear it if needed
    count = conn.execute("SELECT COUNT(*) FROM D_TTH_Web_Event_Ref").fetchone()[0]
    if count > 0:
        conn.execute("DELETE FROM D_TTH_Web_Event_Ref")
        print("Cleared existing data from D_TTH_Web_Event_Ref")
    
    # Insert the data
    for event_id, event_name in event_ref_data:
        conn.execute("""
        INSERT INTO D_TTH_Web_Event_Ref (Event_ID, Event_Name)
        VALUES (?, ?)
        """, (event_id, event_name))
    
    # Verify the data
    result = conn.execute("SELECT * FROM D_TTH_Web_Event_Ref ORDER BY Event_ID").fetchall()
    print("Event reference table created with the following records:")
    for row in result:
        print(f"Event_ID: {row[0]}, Event_Name: {row[1]}")
    
    # Close the connection
    conn.close()
    
    print("Event reference table creation completed.")

# Run this directly
if __name__ == "__main__":
    create_event_reference_table()