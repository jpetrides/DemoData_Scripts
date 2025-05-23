import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from databricks import sql
from datetime import datetime
import tableauhyperapi as hyper
import subprocess
import sys
import pantab
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# Load .env file from the main directory (two levels up from common)
load_dotenv(find_dotenv())

# Authentication parameters
databricks_host = os.environ.get("databricks_host")
databricks_token = os.environ.get("databricks_token")
http_path = os.environ.get("http_path")



# Output file configuration
output_dir = os.path.expanduser("~/Documents/databricks")
output_filename = f"databricks_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
output_path = os.path.join(output_dir, output_filename)

# Output file configuration
output_dir = os.path.expanduser("~/Documents/databricks")
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
parquet_filename = f"databricks_export_{timestamp}.parquet"
hyper_filename = f"databricks_export_{timestamp}.hyper"
parquet_path = os.path.join(output_dir, parquet_filename)
hyper_path = os.path.join(output_dir, hyper_filename)

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Query to execute (replace with your table)
query = "SELECT * EXCEPT (_rescued_data) FROM tableau_solutions_engineering.tth.air_d_aircraft"

# Step 1: Download data from Databricks as Parquet
print("Connecting to Databricks SQL...")
try:
    with sql.connect(
        server_hostname=databricks_host,
        http_path=http_path,
        access_token=databricks_token
    ) as connection:
        print("Connection successful")
        print(f"Executing query: {query}")
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            
            # Fetch results as Arrow Table
            arrow_table = cursor.fetchall_arrow()
            
            # Write to Parquet as intermediate format
            #print(f"Writing data to intermediate Parquet file: {parquet_path}")
            #pq.write_table(arrow_table, parquet_path)
            
            row_count = len(arrow_table)
            print(f"Query returned {row_count} rows")
            
            # Convert Arrow Table to Pandas for Hyper conversion
            df = arrow_table.to_pandas()
            
except Exception as e:
    print(f"Error querying Databricks: {str(e)}")
    raise


# Step 2: Convert to Hyper using pantab (much simpler!)
print(f"Converting to Tableau Hyper format using pantab: {hyper_path}")
try:
    # Write DataFrame directly to Hyper
    pantab.frame_to_hyper(df, hyper_path, table="Extract")
    print(f"Successfully converted to Hyper file with {row_count} rows")
    
    # Optional: Remove the intermediate Parquet file
    # os.remove(parquet_path)
    # print(f"Removed intermediate Parquet file")
            
except Exception as e:
    print(f"Error converting to Hyper format: {str(e)}")
    raise

print(f"Hyper file saved to: {hyper_path}")
print("You can now import this Hyper file directly into Tableau Prep or Tableau Desktop")