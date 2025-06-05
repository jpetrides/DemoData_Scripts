import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import random
import string
from faker import Faker
import gc
import boto3
from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import os

# Initialize Faker for realistic data
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Configuration
TOTAL_RECORDS = 80_000_000
CHUNK_SIZE = 1_000_000  # Process in chunks to manage memory
OUTPUT_FILE = os.path.expanduser("~/Downloads/large_ecommerce_dataset_fixed.parquet")

# Pre-generate some lookup data for performance
print("Generating lookup data...")
n_customers = 2_000_000
n_products = 50_000
n_stores = 5_000
n_employees = 10_000

# Generate base data
customer_ids = [f"CUST_{i:08d}" for i in range(n_customers)]
product_ids = [f"PROD_{i:06d}" for i in range(n_products)]
store_ids = [f"STORE_{i:04d}" for i in range(n_stores)]
employee_ids = [f"EMP_{i:05d}" for i in range(n_employees)]

# Product categories and subcategories
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food', 'Beauty']
subcategories = {
    'Electronics': ['Phones', 'Laptops', 'TVs', 'Cameras', 'Audio'],
    'Clothing': ['Men', 'Women', 'Kids', 'Shoes', 'Accessories'],
    'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Garden', 'Tools'],
    'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Winter Sports'],
    'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'E-books'],
    'Toys': ['Action Figures', 'Board Games', 'Dolls', 'Educational', 'Outdoor'],
    'Food': ['Snacks', 'Beverages', 'Organic', 'International', 'Frozen'],
    'Beauty': ['Skincare', 'Makeup', 'Hair Care', 'Fragrances', 'Tools']
}

# Payment methods and statuses
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Gift Card', 'Cash']
order_statuses = ['Completed', 'Processing', 'Shipped', 'Delivered', 'Cancelled', 'Refunded']
shipment_carriers = ['FedEx', 'UPS', 'USPS', 'DHL', 'Amazon Logistics']

# Countries and regions
countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'China']
regions = ['North', 'South', 'East', 'West', 'Central']

def generate_chunk(chunk_num, chunk_size):
    """Generate a chunk of data"""
    print(f"Generating chunk {chunk_num + 1}...")
    
    # Base timestamp for this chunk
    base_date = datetime(2020, 1, 1) + timedelta(days=chunk_num * 10)
    
    # Generate timestamps as strings first to avoid timezone issues
    timestamps = []
    for _ in range(chunk_size):
        ts = base_date + timedelta(
            days=np.random.randint(0, 365),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60),
            seconds=np.random.randint(0, 60)
        )
        timestamps.append(ts.strftime('%Y-%m-%d %H:%M:%S'))
    
    data = {
        # Transaction identifiers
        'transaction_id': [f"TXN_{chunk_num:04d}_{i:08d}" for i in range(chunk_size)],
        'order_id': [f"ORD_{chunk_num:04d}_{i:08d}" for i in range(chunk_size)],
        'customer_id': np.random.choice(customer_ids, chunk_size),
        'product_id': np.random.choice(product_ids, chunk_size),
        'store_id': np.random.choice(store_ids, chunk_size),
        'employee_id': np.random.choice(employee_ids, chunk_size),
        
        # Timestamp as string (will be converted properly later)
        'transaction_timestamp': timestamps,
        
        # Product details
        'product_category': np.random.choice(categories, chunk_size),
        'product_name': [fake.catch_phrase() for _ in range(chunk_size)],
        'product_brand': [fake.company() for _ in range(chunk_size)],
        'sku': [f"SKU-{''.join(random.choices(string.ascii_uppercase + string.digits, k=8))}" for _ in range(chunk_size)],
        
        # Pricing and quantities
        'unit_price': np.round(np.random.uniform(0.99, 999.99, chunk_size), 2),
        'quantity': np.random.randint(1, 10, chunk_size),
        'discount_percentage': np.random.choice([0, 5, 10, 15, 20, 25, 30], chunk_size),
        'tax_rate': np.round(np.random.uniform(0, 0.15, chunk_size), 4),
        'shipping_cost': np.round(np.random.uniform(0, 50, chunk_size), 2),
        
        # Customer details
        'customer_segment': np.random.choice(['Premium', 'Regular', 'New', 'VIP'], chunk_size),
        'customer_lifetime_value': np.round(np.random.uniform(100, 50000, chunk_size), 2),
        'customer_age': np.random.randint(18, 80, chunk_size),
        'customer_gender': np.random.choice(['M', 'F', 'Other', None], chunk_size),
        
        # Location data
        'billing_country': np.random.choice(countries, chunk_size),
        'shipping_country': np.random.choice(countries, chunk_size),
        'billing_state': [fake.state_abbr() if np.random.random() > 0.1 else None for _ in range(chunk_size)],
        'shipping_state': [fake.state_abbr() if np.random.random() > 0.1 else None for _ in range(chunk_size)],
        'billing_zip': [fake.zipcode() for _ in range(chunk_size)],
        'shipping_zip': [fake.zipcode() for _ in range(chunk_size)],
        'store_region': np.random.choice(regions, chunk_size),
        
        # Payment and fulfillment
        'payment_method': np.random.choice(payment_methods, chunk_size),
        'payment_status': np.random.choice(['Approved', 'Pending', 'Failed'], chunk_size, p=[0.95, 0.03, 0.02]),
        'order_status': np.random.choice(order_statuses, chunk_size),
        'shipment_carrier': np.random.choice(shipment_carriers, chunk_size),
        'tracking_number': [f"{''.join(random.choices(string.digits, k=20))}" if np.random.random() > 0.1 else None for _ in range(chunk_size)],
        
        # Marketing and analytics
        'marketing_channel': np.random.choice(['Organic', 'Paid Search', 'Social Media', 'Email', 'Direct', 'Referral'], chunk_size),
        'device_type': np.random.choice(['Desktop', 'Mobile', 'Tablet'], chunk_size, p=[0.4, 0.5, 0.1]),
        'browser': np.random.choice(['Chrome', 'Safari', 'Firefox', 'Edge', 'Other'], chunk_size),
        'is_repeat_customer': np.random.choice([True, False], chunk_size, p=[0.3, 0.7]),
        'days_since_last_purchase': np.random.randint(0, 365, chunk_size),
        
        # Additional metrics
        'product_rating': np.round(np.random.uniform(1, 5, chunk_size), 1),
        'review_count': np.random.randint(0, 1000, chunk_size),
        'return_flag': np.random.choice([True, False], chunk_size, p=[0.05, 0.95]),
        'fraud_score': np.round(np.random.uniform(0, 1, chunk_size), 4)
    }
    
    # Calculate derived fields
    df = pd.DataFrame(data)
    df['product_subcategory'] = df['product_category'].apply(lambda x: np.random.choice(subcategories[x]))
    df['subtotal'] = df['unit_price'] * df['quantity']
    df['discount_amount'] = df['subtotal'] * (df['discount_percentage'] / 100)
    df['tax_amount'] = (df['subtotal'] - df['discount_amount']) * df['tax_rate']
    df['total_amount'] = df['subtotal'] - df['discount_amount'] + df['tax_amount'] + df['shipping_cost']
    
    # Add some text fields
    df['order_notes'] = [fake.sentence() if np.random.random() > 0.8 else None for _ in range(chunk_size)]
    df['promo_code'] = [f"PROMO{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}" if np.random.random() > 0.7 else None for _ in range(chunk_size)]
    
    # Convert timestamp string to datetime with proper format
    df['transaction_timestamp'] = pd.to_datetime(df['transaction_timestamp'])
    
    return df

# Create the parquet file with proper schema
print(f"Starting to generate {TOTAL_RECORDS:,} records...")
writer = None

# Define explicit schema to ensure timestamp compatibility
def create_pyarrow_schema():
    return pa.schema([
        ('transaction_id', pa.string()),
        ('order_id', pa.string()),
        ('customer_id', pa.string()),
        ('product_id', pa.string()),
        ('store_id', pa.string()),
        ('employee_id', pa.string()),
        ('transaction_timestamp', pa.timestamp('us')),  # microsecond precision
        ('product_category', pa.string()),
        ('product_subcategory', pa.string()),
        ('product_name', pa.string()),
        ('product_brand', pa.string()),
        ('sku', pa.string()),
        ('unit_price', pa.float64()),
        ('quantity', pa.int64()),
        ('discount_percentage', pa.int64()),
        ('tax_rate', pa.float64()),
        ('shipping_cost', pa.float64()),
        ('customer_segment', pa.string()),
        ('customer_lifetime_value', pa.float64()),
        ('customer_age', pa.int64()),
        ('customer_gender', pa.string()),
        ('billing_country', pa.string()),
        ('shipping_country', pa.string()),
        ('billing_state', pa.string()),
        ('shipping_state', pa.string()),
        ('billing_zip', pa.string()),
        ('shipping_zip', pa.string()),
        ('store_region', pa.string()),
        ('payment_method', pa.string()),
        ('payment_status', pa.string()),
        ('order_status', pa.string()),
        ('shipment_carrier', pa.string()),
        ('tracking_number', pa.string()),
        ('marketing_channel', pa.string()),
        ('device_type', pa.string()),
        ('browser', pa.string()),
        ('is_repeat_customer', pa.bool_()),
        ('days_since_last_purchase', pa.int64()),
        ('product_rating', pa.float64()),
        ('review_count', pa.int64()),
        ('return_flag', pa.bool_()),
        ('fraud_score', pa.float64()),
        ('subtotal', pa.float64()),
        ('discount_amount', pa.float64()),
        ('tax_amount', pa.float64()),
        ('total_amount', pa.float64()),
        ('order_notes', pa.string()),
        ('promo_code', pa.string())
    ])

try:
    n_chunks = TOTAL_RECORDS // CHUNK_SIZE
    schema = create_pyarrow_schema()
    writer = pq.ParquetWriter(OUTPUT_FILE, schema, compression='snappy')
    
    for i in range(n_chunks):
        # Generate chunk
        chunk_df = generate_chunk(i, CHUNK_SIZE)
        
        # Convert to PyArrow table with explicit schema
        table = pa.Table.from_pandas(chunk_df, schema=schema)
        
        # Write chunk
        writer.write_table(table)
        
        # Clean up memory
        del chunk_df
        del table
        gc.collect()
        
        print(f"Completed chunk {i + 1}/{n_chunks}")
    
    # Handle any remaining records
    remaining = TOTAL_RECORDS % CHUNK_SIZE
    if remaining > 0:
        chunk_df = generate_chunk(n_chunks, remaining)
        table = pa.Table.from_pandas(chunk_df, schema=schema)
        writer.write_table(table)
        print(f"Completed final chunk with {remaining:,} records")

finally:
    if writer:
        writer.close()

print(f"\nDataset generation complete!")
print(f"File saved as: {OUTPUT_FILE}")
print(f"Total records: {TOTAL_RECORDS:,}")
print("\nYou can now load this file into Databricks using:")
print(f"df = spark.read.parquet('{OUTPUT_FILE}')")

# After the file generation is complete, add this:
print(f"\nDataset generation complete!")
print(f"File saved as: {OUTPUT_FILE}")
print(f"Total records: {TOTAL_RECORDS:,}")

# Upload to S3
print("\nUploading to S3...")

# Load environment variables
load_dotenv(find_dotenv())

# S3 Configuration
aws_access_key = os.environ.get("aws_access_key_id") 
aws_secret_key = os.environ.get("aws_secret_access_key")
bucket_name = os.environ.get("s3_bucket_name")

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key
)

# Define S3 key (path in bucket)
s3_key = f"stage/{os.path.basename(OUTPUT_FILE)}"  # This will extract just the filename

try:
    # Get file size for progress tracking
    file_size = os.path.getsize(OUTPUT_FILE)
    print(f"File size: {file_size / (1024**3):.2f} GB")
    
    # Upload with progress callback
    def upload_callback(bytes_transferred):
        percentage = (bytes_transferred / file_size) * 100
        print(f"\rUpload progress: {percentage:.1f}%", end='', flush=True)
    
    # Upload the file
    s3_client.upload_file(
        OUTPUT_FILE, 
        bucket_name, 
        s3_key,
        Callback=upload_callback
    )
    
    print(f"\n\nSuccessfully uploaded to S3!")
    print(f"S3 location: s3://{bucket_name}/{s3_key}")
    print(f"\nYou can now load this file in Databricks using:")
    print(f"df = spark.read.parquet('s3://{bucket_name}/{s3_key}')")
    
    # Optional: Delete local file after successful upload
    # os.remove(OUTPUT_FILE)
    # print(f"Local file {OUTPUT_FILE} deleted")
    
except Exception as e:
    print(f"\nError uploading to S3: {str(e)}")
    print("The file is still available locally at:", os.path.abspath(OUTPUT_FILE))