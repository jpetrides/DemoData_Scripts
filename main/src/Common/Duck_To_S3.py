import duckdb
import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

load_dotenv(find_dotenv())
# -------------- CONFIGURATION --------------


local_directory = '/Users/jpetrides/Documents/Demo Data/S3sync'
s3_file_name = 'D_TTH_Web_Event_Ref.parquet'

# DuckDB and table details
duckdb_path = os.environ.get("duckdb_path") 
table_name = "hotel_reservations.main.D_TTH_Web_Event_Ref"

# S3 details
aws_access_key_id = os.environ.get("aws_access_key_id") 
aws_secret_access_key = os.environ.get("aws_secret_access_key")
s3_bucket_name = os.environ.get ("s3_bucket_name")


# Local file path for temporary storage
local_file_path = 'temp_parquet.parquet'

# Connect to DuckDB
conn = duckdb.connect(duckdb_path)

try:
    # Export table to local Parquet file
    conn.execute(f"COPY {table_name} TO '{local_file_path}' (FORMAT PARQUET)")
    print(f"Table exported to {local_file_path}")

    # Upload file to S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_client.upload_file(local_file_path, s3_bucket_name, s3_file_name)
    print(f"File uploaded to S3: s3://{s3_bucket_name}/{s3_file_name}")

except ClientError as e:
    print(f"An error occurred: {e}")

finally:
    # Close DuckDB connection
    conn.close()

    # Delete local file
    if os.path.exists(local_file_path):
        os.remove(local_file_path)
        print(f"Local file {local_file_path} deleted")

print("Process completed")