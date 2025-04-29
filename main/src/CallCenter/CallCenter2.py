import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from faker import Faker
from tableauhyperapi import HyperProcess, Connection, SqlType, TableDefinition, Telemetry, Inserter, CreateMode
import pyarrow as pa
import pyarrow.parquet as pq
import sys

# Set random seed for reproducibility
np.random.seed(42)
fake = Faker()
Faker.seed(42)

# Specify the base directory
BASE_DIR = "/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Call_Center/"
print(f"Using base directory: {BASE_DIR}")
sys.stdout.flush()

# Generate Agent data
num_agents = 50
agent_data = []

for i in range(1, num_agents + 1):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}_{last_name.lower()}@autopalonia.com"
    agent_data.append({
        'AgentID': i,
        'FirstName': first_name,
        'LastName': last_name,
        'Email': email
    })

agents = pd.DataFrame(agent_data)

# Generate Call data
num_calls = 500000  # Adjust as needed
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

calls = pd.DataFrame({
    'CallID': range(1, num_calls + 1),
    'CustomerID': [f'Customer_{np.random.randint(1, 150001)}' for _ in range(num_calls)],
    'PropertyID': [f'Property_Code_{np.random.randint(1, 101)}' for _ in range(num_calls)],
    'ReservationID': np.random.choice([np.nan] + list(range(1, 1900001)), num_calls, p=[0.3] + [0.7/1900000]*1900000),
    'CallStartTime': [start_date + timedelta(seconds=np.random.randint(0, int((end_date - start_date).total_seconds()))) for _ in range(num_calls)],
    'CallType': np.random.choice(['Inquiry', 'Complaint', 'Reservation', 'Other'], num_calls),
    'WaitTime': np.random.randint(0, 600, num_calls),  # Wait time in seconds (0 to 10 minutes)
    'AbandonFlag': np.random.choice([True, False], num_calls, p=[0.05, 0.95])  # 5% abandon rate
})

# Process abandon flags and add agent info
calls['AgentID'] = np.where(calls['AbandonFlag'], np.nan, np.random.randint(1, num_agents + 1, num_calls))
calls['CallStatus'] = np.where(calls['AbandonFlag'], 'Abandoned', 
                               np.random.choice(['Resolved', 'Escalated', 'Follow-up Required'], num_calls))

# Calculate call duration and end time for non-abandoned calls
calls['CallDuration'] = np.where(calls['AbandonFlag'], 0, np.random.randint(60, 1800, num_calls))  # 1 to 30 minutes
calls['CallEndTime'] = calls.apply(lambda row: row['CallStartTime'] + timedelta(seconds=row['WaitTime'] + row['CallDuration']) if not row['AbandonFlag'] else row['CallStartTime'] + timedelta(seconds=row['WaitTime']), axis=1)

# Add First Call Resolution
calls['FirstCallResolution'] = np.where(calls['AbandonFlag'], False, 
                                        np.random.choice([True, False], num_calls, p=[0.7, 0.3]))  # 70% FCR for non-abandoned calls

# Ensure consistency between FirstCallResolution and CallStatus
calls.loc[(calls['FirstCallResolution'] == True) & (calls['AbandonFlag'] == False), 'CallStatus'] = 'Resolved'

# Generate CallRating data (only for non-abandoned calls)
call_ratings = pd.DataFrame({
    'RatingID': range(1, num_calls + 1),
    'CallID': calls['CallID'],
    'Rating': np.where(calls['AbandonFlag'], np.nan, np.random.randint(1, 6, num_calls)),
    'Feedback': np.where(calls['AbandonFlag'], None, 
                         np.random.choice([None, 'Good service', 'Poor service', 'Long wait time', 'Very helpful', 'Needs improvement'], num_calls))
})

# Generate CallIssue data (only for non-abandoned calls)
call_issues = pd.DataFrame({
    'IssueID': range(1, num_calls + 1),
    'CallID': calls['CallID'],
    'IssueType': np.where(calls['AbandonFlag'], None,
                          np.random.choice(['Booking', 'Cancellation', 'Amenities', 'Billing', 'Cleanliness', 'Noise', 'Other'], num_calls)),
    'IssueDescription': np.where(calls['AbandonFlag'], None,
                                 np.random.choice(['Customer reported issue with booking', 'Request for cancellation', 'Inquiry about hotel amenities', 'Billing discrepancy', 'Cleanliness concern', 'Noise complaint', 'Other issue'], num_calls)),
    'ResolutionStatus': np.where(calls['AbandonFlag'], None,
                                 calls['CallStatus'])
})


# Generate AgentPerformance data
date_range = pd.date_range(start=start_date, end=end_date)
agent_performance = pd.DataFrame({
    'PerformanceID': range(1, len(date_range) * num_agents + 1),
    'AgentID': np.repeat(agents['AgentID'], len(date_range)),
    'Date': np.tile(date_range, num_agents),
    'CallsHandled': np.random.randint(20, 100, len(date_range) * num_agents),
    'AvgCallDuration': np.random.randint(120, 600, len(date_range) * num_agents),
    'AvgRating': np.random.uniform(3.0, 5.0, len(date_range) * num_agents).round(2)
})

# Save data to CSV files
# agents.to_csv('/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Call_Center/agents.csv', index=False)
# calls.to_csv('/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Call_Center/calls.csv', index=False)
# call_ratings.to_csv('/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Call_Center/call_ratings.csv', index=False)
# call_issues.to_csv('/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Call_Center/call_issues.csv', index=False)
# agent_performance.to_csv('/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Call_Center/agent_performance.csv', index=False)

#print("Data generation complete. Files saved: agents.csv, calls.csv, call_ratings.csv, call_issues.csv, agent_performance.csv")



# Your existing data generation code here
# ...

# Function to save DataFrame to Parquet
def df_to_parquet(df, parquet_file):
    full_path = os.path.join(BASE_DIR, parquet_file)
    
    # Convert timestamp columns to microsecond precision
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].dt.floor('us')  # This truncates to microseconds so we can import to databricks
    
    table = pa.Table.from_pandas(df)
    
    # Write with Parquet options
    pq.write_table(table, full_path, 
                   coerce_timestamps='ms',  # 'ms' for millisecond precision
                   allow_truncated_timestamps=True)
    
    print(f"Parquet file {full_path} has been created with adjusted timestamp precision.")
    sys.stdout.flush()

# Function to create Hyper file from Parquet
def parquet_to_hyper(parquet_file, hyper_file, table_name):
    parquet_path = os.path.join(BASE_DIR, parquet_file)
    hyper_path = os.path.join(BASE_DIR, hyper_file)
    
    print(f"Converting {parquet_path} to {hyper_path}")
    sys.stdout.flush()
    
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint,
                        database=hyper_path,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            
            # Create table from Parquet file
            connection.execute_command(f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM external('{parquet_path}')
            """)
            
            print(f"The hyper file {hyper_path} has been created from {parquet_path}.")
            sys.stdout.flush()

# Assume you have these DataFrames already: agents, calls, call_ratings, call_issues, agent_performance

print("Starting to save DataFrames to Parquet files...")
sys.stdout.flush()

# Save DataFrames to Parquet files
df_to_parquet(agents, 'Dim_CC_agents.parquet')
df_to_parquet(calls, 'F_CC_calls.parquet')
df_to_parquet(call_ratings, 'F_CC_call_ratings.parquet')
df_to_parquet(call_issues, 'F_CC_call_issues.parquet')
df_to_parquet(agent_performance, 'F_CC_agent_performance.parquet')

print("All Parquet files created. Starting conversion to Hyper files...")
sys.stdout.flush()

# Convert Parquet files to Hyper files
# This isn't actually necessary since you can publish the parquet file directly.
parquet_to_hyper('Dim_CC_agents.parquet', 'D_CC_agents.hyper', 'Agents')
parquet_to_hyper('F_CC_calls.parquet', 'F_CC_calls.hyper', 'Calls')
parquet_to_hyper('F_CC_call_ratings.parquet', 'F_CC_call_ratings.hyper', 'CallRatings')
parquet_to_hyper('F_CC_call_issues.parquet', 'F_CC_call_issues.hyper', 'CallIssues')
parquet_to_hyper('F_CC_agent_performance.parquet', 'F_CC_agent_performance.hyper', 'AgentPerformance')

print(f"Data conversion complete. Parquet and Hyper files saved in: {BASE_DIR}")
sys.stdout.flush()

