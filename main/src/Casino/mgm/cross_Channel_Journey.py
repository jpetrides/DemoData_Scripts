import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random
import uuid
import time
from tqdm import tqdm

# Start timer
start_time = time.time()

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# File path
output_path = "/Users/jpetrides/Documents/Customers/Hotel/MGM"
output_file = "MGM_CrossChannel_Touchpoints.parquet"

# Define parameters
num_customers = 150000
start_date = datetime(2025, 3, 1)
end_date = datetime(2025, 4, 30)
date_range = (end_date - start_date).days + 1

# Define touchpoints with precisely calculated frequencies that sum to exactly 1
touchpoints = {
    # Marketing Engagement (~20%)
    "Email Open": 0.1,
    "Email Click": 0.05,
    "Loyalty App Push Notification": 0.03,
    "Social Media Ad Click": 0.02,
    
    # Online Actions (~70%)
    "Website Visit": 0.5,
    "Room Search": 0.09,
    "Room Booking": 0.03,
    "Restaurant Reservation": 0.03,
    "Show Ticket Purchase": 0.02,
    "Casino App Activity": 0.04,  # Reduced from 0.05 to 0.04
    
    # Offline Touchpoints (~10%)
    "Call Center Interaction": 0.03,
    "Front Desk Check-in": 0.02,
    "Casino Floor Activity": 0.01,
    "Restaurant Visit": 0.01,
    "Show Attendance": 0.01,
    "Loyalty Desk Interaction": 0.01
}

# Channel mapping
channel_mapping = {
    "Email Open": "Email",
    "Email Click": "Email",
    "Loyalty App Push Notification": "Mobile App",
    "Social Media Ad Click": "Social Media",
    "Website Visit": "Web",
    "Room Search": "Web",
    "Room Booking": "Web",
    "Restaurant Reservation": "Web",
    "Show Ticket Purchase": "Web",
    "Casino App Activity": "Mobile App",
    "Call Center Interaction": "Call Center",
    "Front Desk Check-in": "Property",
    "Casino Floor Activity": "Property",
    "Restaurant Visit": "Property",
    "Show Attendance": "Property",
    "Loyalty Desk Interaction": "Property"
}

# Calculate and print the sum to verify
probability_sum = sum(touchpoints.values())
print(f"Sum of probabilities: {probability_sum}")
assert probability_sum == 1.0, f"Touchpoint probabilities sum to {probability_sum}, not 1.0"

# Probability of customer having any interaction
customer_activity_prob = 0.85

# Average number of interactions per active customer
avg_interactions_per_customer = 30

# Optimized touchpoint data generation
def generate_touchpoint_data_optimized():
    print(f"Generating MGM customer touchpoint data for {num_customers:,} customers...")
    
    # Pre-calculate customer activity (binary mask for which customers are active)
    activity_mask = np.random.random(num_customers) < customer_activity_prob
    active_customers = np.sum(activity_mask)
    print(f"Active customers: {active_customers:,} ({active_customers/num_customers*100:.1f}%)")
    
    # Pre-generate approximate total number of interactions
    # Using log-normal distribution for interactions per customer
    avg_log = np.log(avg_interactions_per_customer)
    interactions_per_customer = np.random.lognormal(mean=avg_log, sigma=0.8, size=active_customers)
    interactions_per_customer = np.clip(interactions_per_customer, 1, 200).astype(int)
    total_interactions_est = np.sum(interactions_per_customer)
    print(f"Estimated total interactions: {total_interactions_est:,}")
    
    # Using estimated total as buffer size for pre-allocating arrays
    # Pre-generate UUIDs (much faster in bulk)
    print("Pre-generating UUIDs...")
    uuid_buffer = [str(uuid.uuid4()) for _ in range(total_interactions_est)]
    
    # Process in chunks to manage memory
    chunk_size = 10000  # Adjust based on available memory
    num_chunks = (num_customers + chunk_size - 1) // chunk_size
    
    all_records = []
    
    print(f"Processing customers in {num_chunks} chunks...")
    active_customer_idx = 0
    
    for chunk_idx in tqdm(range(num_chunks)):
        start_idx = chunk_idx * chunk_size
        end_idx = min((chunk_idx + 1) * chunk_size, num_customers)
        chunk_customers = end_idx - start_idx
        
        # Get active customers in this chunk
        chunk_activity = activity_mask[start_idx:end_idx]
        chunk_active_count = np.sum(chunk_activity)
        
        if chunk_active_count == 0:
            continue
        
        # Process each active customer in chunk
        records = []
        uuid_idx = 0
        
        for i in range(chunk_customers):
            if not chunk_activity[i]:
                continue
                
            customer_id = f"Customer_{start_idx + i + 1}"
            num_interactions = interactions_per_customer[active_customer_idx]
            active_customer_idx += 1
            
            if num_interactions == 0:
                continue
                
            # Generate first timestamp for this customer (random day in range)
            random_day = random.randint(0, date_range - 1)
            first_date = start_date + timedelta(days=random_day)
            hour = random.randint(8, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            first_timestamp = first_date.replace(hour=hour, minute=minute, second=second)
            
            # Generate subsequent timestamps
            timestamps = [first_timestamp]
            current_ts = first_timestamp
            
            # Pre-select touchpoints for this customer
            touchpoint_names = list(touchpoints.keys())
            touchpoint_probs = list(touchpoints.values())
            customer_touchpoints = np.random.choice(
                touchpoint_names,
                size=num_interactions,
                p=touchpoint_probs
            )
            
            # Generate subsequent timestamps
            for j in range(1, num_interactions):
                # Exponential distribution for days between events
                day_gap = min(14, max(0, int(np.random.exponential(scale=2))))
                
                if day_gap == 0:
                    # Same day, just add hours/minutes
                    hour_gap = random.randint(0, 5)
                    minute_gap = random.randint(1, 59)
                    new_ts = current_ts + timedelta(hours=hour_gap, minutes=minute_gap)
                else:
                    # Different day
                    new_ts = current_ts + timedelta(days=day_gap)
                    hour = random.randint(8, 23)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    new_ts = new_ts.replace(hour=hour, minute=minute, second=second)
                
                if new_ts <= end_date:
                    timestamps.append(new_ts)
                    current_ts = new_ts
                
                if current_ts > end_date:
                    break
            
            # Make sure we don't have more timestamps than touchpoints
            actual_interactions = min(len(timestamps), num_interactions)
            
            # Build records for this customer
            for j in range(actual_interactions):
                touchpoint = customer_touchpoints[j]
                records.append({
                    'customer_id': customer_id,
                    'event_id': uuid_buffer[uuid_idx],
                    'touchpoint': touchpoint,
                    'timestamp': timestamps[j],
                    'channel': channel_mapping[touchpoint]
                })
                uuid_idx += 1
        
        # Sort records by timestamp
        records.sort(key=lambda x: (x['customer_id'], x['timestamp']))
        all_records.extend(records)
        
        # Periodically convert to DataFrame to save memory
        if len(all_records) > 1000000:
            print(f"Intermediate chunk: {len(all_records):,} records")
            chunk_df = pd.DataFrame(all_records)
            
            # If first chunk, write with header
            mode = 'w' if chunk_idx == 0 else 'a'
            temp_path = os.path.join(output_path, "temp_" + output_file)
            
            # Write to temporary file
            chunk_df.to_parquet(temp_path, index=False)
            all_records = []
    
    # Process any remaining records
    final_df = pd.DataFrame(all_records) if all_records else None
    
    # Combine all chunks (if we wrote intermediate chunks)
    if os.path.exists(os.path.join(output_path, "temp_" + output_file)):
        if final_df is not None:
            temp_path = os.path.join(output_path, "temp2_" + output_file)
            final_df.to_parquet(temp_path, index=False)
            
            # Read and combine
            chunks = [pd.read_parquet(os.path.join(output_path, "temp_" + output_file)),
                     pd.read_parquet(os.path.join(output_path, "temp2_" + output_file))]
            final_df = pd.concat(chunks, ignore_index=True)
            
            # Clean up temp files
            os.remove(os.path.join(output_path, "temp_" + output_file))
            os.remove(os.path.join(output_path, "temp2_" + output_file))
        else:
            # Just rename the temp file
            temp_path = os.path.join(output_path, "temp_" + output_file)
            final_df = pd.read_parquet(temp_path)
            os.remove(temp_path)
    else:
        # We have all records in memory
        if final_df is None:
            print("No records generated!")
            return None
    
    # Ensure the directory exists
    os.makedirs(output_path, exist_ok=True)
    
    # Save to parquet
    full_path = os.path.join(output_path, output_file)
    final_df.to_parquet(full_path, index=False)
    
    print(f"Generated {len(final_df):,} touchpoint records for {final_df['customer_id'].nunique():,} customers")
    print(f"Data saved to {full_path}")
    
    # Return statistics
    touchpoint_counts = final_df['touchpoint'].value_counts()
    channel_counts = final_df['channel'].value_counts()
    
    return {
        "total_records": len(final_df),
        "unique_customers": final_df['customer_id'].nunique(),
        "date_range": f"{start_date.date()} to {end_date.date()}",
        "touchpoint_distribution": touchpoint_counts.to_dict(),
        "channel_distribution": channel_counts.to_dict()
    }

# Run generation
stats = generate_touchpoint_data_optimized()

# Display stats
print("\nDataset Statistics:")
print(f"Total Records: {stats['total_records']:,}")
print(f"Unique Customers: {stats['unique_customers']:,}")
print(f"Date Range: {stats['date_range']}")

print("\nTouchpoint Distribution:")
for touchpoint, count in sorted(stats['touchpoint_distribution'].items(), key=lambda x: x[1], reverse=True):
    percentage = (count / stats['total_records']) * 100
    print(f"  {touchpoint}: {count:,} ({percentage:.1f}%)")

print("\nChannel Distribution:")
for channel, count in sorted(stats['channel_distribution'].items(), key=lambda x: x[1], reverse=True):
    percentage = (count / stats['total_records']) * 100
    print(f"  {channel}: {count:,} ({percentage:.1f}%)")

# Print execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"\nExecution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")