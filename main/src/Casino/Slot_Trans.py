import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import random
import time
import gc

# Set random seed for reproducibility
np.random.seed(42)

# Constants
START_DATE = datetime(2023, 10, 1)
END_DATE = START_DATE + timedelta(days=13*30)  # Approximately 13 months
TOTAL_MACHINES = 100
CARDED_PLAY_PERCENTAGE = 0.7
MIN_BET_RANGE = (1, 20)
MAX_BET_RANGE = (10, 500)
PAYOUT_RANGE = (0.88, 0.92)
AVG_SESSIONS_PER_DAY = 6000
TOTAL_CUSTOMERS = 160000

# Real slot machine manufacturers and models
MACHINES = [
    ("IGT", "Wheel of Fortune"),
    ("IGT", "Megabucks"),
    ("IGT", "Double Diamond"),
    ("Aristocrat", "Buffalo"),
    ("Aristocrat", "Dragon Link"),
    ("Aristocrat", "Lightning Link"),
    ("Bally", "Quick Hit"),
    ("Bally", "Hot Shot"),
    ("Bally", "88 Fortunes"),
    ("WMS", "Jungle Wild"),
    ("WMS", "Reel 'em In"),
    ("WMS", "Zeus"),
    ("Konami", "China Shores"),
    ("Konami", "Dragon's Law"),
    ("Konami", "Fortune Stacks"),
    ("Ainsworth", "Mustang Money"),
    ("Ainsworth", "Twice the Money"),
    ("Ainsworth", "Eagle Bucks"),
    ("Scientific Games", "88 Fortunes"),
    ("Scientific Games", "Dancing Drums"),
    ("Scientific Games", "Lock It Link"),
    ("Novomatic", "Book of Ra"),
    ("Novomatic", "Lucky Lady's Charm"),
    ("Novomatic", "Sizzling Hot"),
    ("Everi", "Black Diamond"),
    ("Everi", "White Hot Diamonds"),
    ("Everi", "Triple Jackpot Gems"),
    ("Incredible Technologies", "Crazy Money"),
    ("Incredible Technologies", "Fate of the 8"),
    ("Incredible Technologies", "Sky Ball")
]
print("start generate_machines")
def generate_machines(num_machines):
    machines = []
    for i in range(num_machines):
        manufacturer, model = random.choice(MACHINES)
        machine = {
            'MachineID': f'MACHINE_{i+1:03d}',
            'Model': model,
            'Manufacturer': manufacturer,
            'Location': f'Section_{random.randint(1, 10)}',
            'MinBet': round(random.uniform(*MIN_BET_RANGE), 2),
            'MaxBet': round(random.uniform(*MAX_BET_RANGE), 2),
            'PayoutPercentage': round(random.uniform(*PAYOUT_RANGE), 4)
        }
        machines.append(machine)
    return pd.DataFrame(machines)

print("start generate_sessions")
def generate_sessions(start_date, end_date, machines_df):
    date_range = pd.date_range(start_date, end_date, freq='D')
    sessions = []
    
    for date in date_range:
        # Adjust number of sessions based on month
        month = date.month
        if month == 1:
            adj_factor = 1.06  # January: 6% above average
        elif month in [5, 12]:
            adj_factor = 1.03  # May and December: 3% above average
        elif month in [4, 6]:
            adj_factor = 0.97  # April and June: 3% below average
        else:
            adj_factor = 1.0

        num_sessions = int(np.random.normal(AVG_SESSIONS_PER_DAY, AVG_SESSIONS_PER_DAY * 0.1) * adj_factor)
        
        for _ in range(num_sessions):
            machine = machines_df.sample(1).iloc[0]
            start_time = date + pd.Timedelta(seconds=random.randint(0, 86399))
            
            # Adjust session frequency based on time of day
            hour = start_time.hour
            if 17 <= hour < 2 or hour == 0:
                duration_factor = 1.2  # Busy hours
            elif 7 <= hour < 11:
                duration_factor = 0.8  # Light hours
            else:
                duration_factor = 1.0  # Average hours
            
            duration = timedelta(minutes=random.randint(5, 60) * duration_factor)
            end_time = start_time + duration
            
            total_bets = round(random.uniform(machine['MinBet'], machine['MaxBet']) * random.randint(10, 100), 2)
            total_payouts = round(total_bets * machine['PayoutPercentage'] * random.uniform(0.9, 1.1), 2)
            
            session = {
                'SessionID': f'SESSION_{len(sessions) + 1}',
                'MachineID': machine['MachineID'],
                'StartTime': start_time,
                'EndTime': end_time,
                'TotalBets': total_bets,
                'TotalPayouts': total_payouts
            }
            
            if random.random() < CARDED_PLAY_PERCENTAGE:
                session['CustomerID'] = f'Customer_{random.randint(1, TOTAL_CUSTOMERS)}'
            
            sessions.append(session)
    
    return pd.DataFrame(sessions)

print("start generate_transactions")

def generate_transactions(sessions_df):
    transactions = []
    for _, session in sessions_df.iterrows():
        num_transactions = random.randint(5, 50)
        for _ in range(num_transactions):
            bet_amount = round(session['TotalBets'] / num_transactions, 2)
            payout_amount = round(random.uniform(0, bet_amount * 2), 2)
            transaction = {
                'TransactionID': f'TRANS_{len(transactions) + 1}',
                'SessionID': session['SessionID'],
                'Timestamp': session['StartTime'] + pd.Timedelta(seconds=random.randint(0, (session['EndTime'] - session['StartTime']).seconds)),
                'BetAmount': bet_amount,
                'PayoutAmount': payout_amount
            }
            transactions.append(transaction)
    return pd.DataFrame(transactions)

print("start generate_daily_revenue")

def generate_daily_revenue(sessions_df):
    daily_revenue = sessions_df.groupby(sessions_df['StartTime'].dt.date).agg({
        'TotalBets': 'sum',
        'TotalPayouts': 'sum'
    }).reset_index()
    daily_revenue['NetRevenue'] = daily_revenue['TotalBets'] - daily_revenue['TotalPayouts']
    daily_revenue = daily_revenue.rename(columns={'StartTime': 'Date'})
    return daily_revenue

# Generate data
print("let's buckle up")
print("generate machines - should be fast")
machines_df = generate_machines(TOTAL_MACHINES)
print("generate sessions - should be the longest")
sessions_df = generate_sessions(START_DATE, END_DATE, machines_df)
print("generate transactions - actually this should be the longest")
transactions_df = generate_transactions(sessions_df)
print("generate daily revenue - should be somewhat quick")
daily_revenue_df = generate_daily_revenue(sessions_df)

print("start write to parquet files")

# Save to parquet files
pq.write_table(pa.Table.from_pandas(machines_df), '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Casino/machines.parquet')
pq.write_table(pa.Table.from_pandas(sessions_df), '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Casino/sessions.parquet')
pq.write_table(pa.Table.from_pandas(transactions_df), '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Casino/transactions.parquet')
pq.write_table(pa.Table.from_pandas(daily_revenue_df), '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Casino/daily_revenue.parquet')

print("Data generation complete. Parquet files have been created.")