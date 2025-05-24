import boto3
import os
import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

load_dotenv(find_dotenv())
# -------------- CONFIGURATION --------------

aws_access_key = os.environ.get("aws_access_key_id") 
aws_secret_key = os.environ.get("aws_secret_access_key")
bucket_name = os.environ.get ("s3_bucket_name") 
local_directory = '/Users/jpetrides/Documents/Demo Data/S3sync'

def download_parquet_files(bucket_name, local_directory, aws_access_key, aws_secret_key):
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Create local directory if it doesn't exist
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    try:
        # List all objects in bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)

        # Download each parquet file
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith('.parquet'):
                        # Create subdirectories if needed
                        local_file_path = os.path.join(local_directory, obj["Key"])
                        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                        
                        print(f"Downloading {obj['Key']}...")
                        s3_client.download_file(
                            bucket_name,
                            obj["Key"],
                            local_file_path
                        )
        print("Download completed!")
        
    except Exception as e:
        print(f"Error: {str(e)}")



download_parquet_files(bucket_name, local_directory, aws_access_key, aws_secret_key)