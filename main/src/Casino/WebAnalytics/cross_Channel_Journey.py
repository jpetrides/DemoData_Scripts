import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random
import time
from tqdm import tqdm

# Set random seed for reproducibility
np.random.seed(42)

# File path
output_path = "/Users/jpetrides/Documents/Customers/Hotel/MGM"
output_file = "MGM_CrossChannel_Touchpoints.parquet"

# Define parameters
num_customers = 150000  # Full size for production
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
    
    # Online Actions (~69%)
    "Website Visit": 0.5,
    "Room Search": 0.09,
    "Room Booking": 0.03,
    "Restaurant Reservation": 0.03,
    "Show Ticket Purchase": 0.02,
    "Casino App Activity": 0.04,
    
    # Offline Touchpoints (~10%)
    "Call Center Interaction": 0.03,
    "Front Desk Check-in": 0.02,
    "Casino Floor Activity": 0.01,
    "Restaurant Visit": 0.01,
    "Show Attendance": 0.01,
    "Loyalty Desk Interaction": 0.01
}

# Calculate and print the sum to verify
probability_sum = sum(touchpoints.values())
print(f"Sum of probabilities: {probability_sum}")

# Double check with exact comparison
assert probability_sum == 1.0, f"Touchpoint probabilities sum to {probability_sum}, not 1.0"

# Probability of customer having any interaction (85% will have at least one)
customer_activity_prob = 0.85

# Average number of interactions per active customer (log-normal distribution)
avg_interactions_per_customer = 30  # This will give us ~3.8M records

# Generate data
def generate_touchpoint_data():
    print(f"Generating MGM customer touchpoint data for {num_customers:,} customers...")
    data = []
    
    # For progress tracking
    active_customers = int(num_customers * customer_activity_prob)
    
    # Global event counter for sequential IDs
    event_counter = 1
    
    for i in tqdm(range(num_customers)):
        customer_id = f"Customer_{i+1}"
        
        # Determine if this customer has any interactions
        if random.random() < customer_activity_prob:
            # Log-normal distribution for number of interactions
            num_interactions = int(np.random.lognormal(mean=np.log(avg_interactions_per_customer), sigma=0.8))
            num_interactions = max(1, min(num_interactions, 200))  # Cap at 200 interactions
            
            # Generate touchpoint sequence with time dependency
            previous_date = None
            journey_touchpoints = []
            
            for _ in range(num_interactions):
                # Select touchpoint based on probabilities
                touchpoint = np.random.choice(
                    list(touchpoints.keys()),
                    p=list(touchpoints.values())
                )
                
                # Generate timestamp
                if previous_date is None:
                    # First interaction is uniformly distributed across the date range
                    random_day = random.randint(0, date_range - 1)
                    timestamp = start_date + timedelta(days=random_day)
                    # Add random hour/minute
                    timestamp += timedelta(
                        hours=random.randint(8, 23),
                        minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59)
                    )
                else:
                    # Subsequent interactions follow the previous one (within 0-14 days)
                    day_gap = np.random.exponential(scale=2)  # Most interactions happen within a few days
                    day_gap = min(14, max(0, int(day_gap)))  # Cap at 14 days
                    
                    # Some interactions happen on the same day
                    if day_gap == 0:
                        hour_gap = random.randint(0, 5)
                        minute_gap = random.randint(1, 59)
                        timestamp = previous_date + timedelta(hours=hour_gap, minutes=minute_gap)
                    else:
                        timestamp = previous_date + timedelta(days=day_gap)
                        timestamp = timestamp.replace(
                            hour=random.randint(8, 23),
                            minute=random.randint(0, 59),
                            second=random.randint(0, 59)
                        )
                
                # Make sure timestamp is within our date range
                if start_date <= timestamp <= end_date:
                    # Generate sequential event_id
                    event_id = event_counter
                    event_counter += 1
                    
                    # Calculate epoch timestamp (seconds since 1970-01-01)
                    epoch_time = int(timestamp.timestamp())
                    
                    # Add to journey touchpoints
                    journey_touchpoints.append({
                        'customer_id': customer_id,
                        'event_id': event_id,
                        'touchpoint': touchpoint,
                        'timestamp': timestamp,
                        'epoch_time': epoch_time
                    })
                    
                    previous_date = timestamp
            
            # Sort by timestamp after all touchpoints are created
            journey_touchpoints.sort(key=lambda x: x['timestamp'])
            
            # Add all touchpoints to main data
            data.extend(journey_touchpoints)
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add a channel column
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
    df['channel'] = df['touchpoint'].map(channel_mapping)
    
    # Ensure the directory exists
    os.makedirs(output_path, exist_ok=True)
    
    # Save to parquet
    full_path = os.path.join(output_path, output_file)
    df.to_parquet(full_path, index=False)
    
    print(f"Generated {len(df):,} touchpoint records for {df['customer_id'].nunique():,} customers")
    print(f"Data saved to {full_path}")
    
    # Return some statistics
    touchpoint_counts = df['touchpoint'].value_counts()
    channel_counts = df['channel'].value_counts()
    
    return {
        "total_records": len(df),
        "unique_customers": df['customer_id'].nunique(),
        "date_range": f"{start_date.date()} to {end_date.date()}",
        "touchpoint_distribution": touchpoint_counts.to_dict(),
        "channel_distribution": channel_counts.to_dict()
    }

# Track execution time
start_time = time.time()

# Run generation
stats = generate_touchpoint_data()

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