import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

# Configuration
num_feeds = 5
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 7, 1)
records_per_day = 24
output_dir = '/Users/jpetrides/Documents/Demo Data/OTA'  # Specify your directory

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Feed Names
feed_names = [
    "CustomerDataFeed",
    "ProductCatalogUpdates",
    "SalesTransactions",
    "InventorySync",
    "MarketingCampaignResults"
]

# Function to generate random error messages
def generate_error_message():
    errors = [
        "Connection to database lost.",
        "Schema mismatch detected.",
        "Data validation failed: Invalid product ID.",
        "Rate limit exceeded.",
        "Timeout error.",
        "Disk full.",
        "Invalid data format."
    ]
    return random.choice(errors)

# Data Generation
data = []
current_date = start_date
while current_date <= end_date:
    for feed_name in feed_names:
        for hour in range(records_per_day):
            timestamp = current_date + timedelta(hours=hour)
            
            # Simulate Feed Status
            status_choices = ["Healthy"] * 8 + ["Delayed"] * 1 + ["Failed"] * 1
            status = random.choice(status_choices)

            # Simulate Data Freshness
            freshness_timestamp = timestamp - timedelta(minutes=random.randint(0, 30)) if status == "Healthy" else timestamp - timedelta(hours=random.randint(1, 24))

            # Simulate Error Logs
            error_log = ""
            if status == "Failed" or status == "Delayed":
                num_errors = random.randint(1, 3)
                error_log = ", ".join([generate_error_message() for _ in range(num_errors)])
            
            # Simulate Latency (in seconds)
            latency = random.randint(300, 1800) if status == "Healthy" else random.randint(3600, 7200) if status == "Delayed" else random.randint(10, 60)

            # Simulate Completion Rate
            completion_rate = 100 if status == "Healthy" else random.randint(50, 99) if status == "Delayed" else 0

            data.append([
                feed_name,
                status,
                freshness_timestamp,
                error_log,
                latency,
                completion_rate,
                timestamp
            ])
            
    current_date += timedelta(days=1)

# Create Pandas DataFrame
df = pd.DataFrame(data, columns=[
    "Feed Name",
    "Feed Status",
    "Data Freshness",
    "Error Logs",
    "Feed Latency (seconds)",
    "Completion Rate",
    "Timestamp"
])

# Convert Timestamp columns to datetime objects
df['Data Freshness'] = pd.to_datetime(df['Data Freshness'])
df['Timestamp'] = pd.to_datetime(df['Timestamp'])

# Save to CSV in the specified directory
csv_filepath = os.path.join(output_dir, "data_feed_monitoring_data.csv")  # Construct full path
df.to_csv(csv_filepath, index=False)

# Save to Parquet in the specified directory
parquet_filepath = os.path.join(output_dir, "data_feed_monitoring_data.parquet")  # Construct full path
df.to_parquet(parquet_filepath, index=False)

print(f"Generated {len(df)} records.")
print(f"Data saved to {csv_filepath} and {parquet_filepath}")

# Sample of the data (first 10 rows)
print("\nSample Data:")
print(df.head(10))