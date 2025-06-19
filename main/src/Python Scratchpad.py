import duckdb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get database and S3 configuration from .env
DB_PATH = os.getenv('duckdb_path')
AWS_ACCESS_KEY = os.getenv('aws_access_key_id')
AWS_SECRET_KEY = os.getenv('aws_secret_access_key')
S3_BUCKET = os.getenv('s3_bucket_name2')

# Create connection
conn = duckdb.connect(DB_PATH)

# Configure S3 credentials in DuckDB
conn.execute(f"""
    SET s3_access_key_id='{AWS_ACCESS_KEY}';
    SET s3_secret_access_key='{AWS_SECRET_KEY}';
    SET s3_region='us-east-2';
""")

# Use main schema
conn.execute("USE main")

# List of tables to export
tables = [
    'hotel_reservations.main.TTH_D_Sourcing_brand_tiers',
    'hotel_reservations.main.TTH_D_Sourcing_vendors',
    'hotel_reservations.main.TTH_D_Sourcing_categories',
    'hotel_reservations.main.TTH_D_Sourcing_products',
    'hotel_reservations.main.TTH_F_Sourcing_pricing',
    'hotel_reservations.main.TTH_F_Sourcing_orders',
    'hotel_reservations.main.TTH_F_Sourcing_order_items'
]

# Export each table to S3
print(f"Starting export to S3 bucket: {S3_BUCKET}\n")

for table in tables:
    s3_path = f"s3://{S3_BUCKET}/{table}.parquet"
    
    try:
        print(f"Exporting {table}...")
        
        # Get row count for verification
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        # Export to S3 as parquet
        conn.execute(f"""
            COPY {table} 
            TO '{s3_path}' 
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """)
        
        print(f"✓ Exported {table} ({row_count:,} rows) to {s3_path}")
        
    except Exception as e:
        print(f"✗ Error exporting {table}: {str(e)}")

conn.close()
print("\nExport completed!")