import duckdb
import os

# Connect to DuckDB
duckdb_path = "/Users/jpetrides/Documents/Demo Data/Hotels/main/data/hotel_reservations.duckdb"
con = duckdb.connect(duckdb_path)

# Set the table name you want to convert
table_name = "hotel_reservations.main.Page_View_Sankey"  # Change this to your desired table name
#hotel_reservations.main.TTH_D_Customer
#hotel_reservations.main.TTH_D_Property
#hotel_reservations.main.TTH_F_Reservation

# Set output directory to Downloads/Air_Parquet
output_dir = os.path.expanduser("/Users/jpetrides/Documents/Customers/Hotel/MGM")
os.makedirs(output_dir, exist_ok=True)

# Create output path with table name
output_path = os.path.join(output_dir, f"{table_name}.parquet")

# Execute the COPY statement
con.execute(f"""
    COPY (SELECT * FROM {table_name}) 
    TO '{output_path}' (FORMAT 'parquet')
""")
print(f"Converted {table_name} to {output_path}")

con.close()
print("Conversion complete!")