import itertools
import random
import pandas as pd

def random_partition(total, parts):
    """
    Randomly partition the integer 'total' into 'parts' integers
    such that each part is at least 1 and the sum equals 'total'.

    This is done by first subtracting 1 from each part to meet the minimum
    requirement, then partitioning the remaining (total - parts) using the
    "stars and bars" method.
    """
    # Subtract one per bin to ensure a minimum of 1
    extras = total - parts
    if parts == 1:
        return [total]
    # Choose parts-1 random divider points from 0 to extras (inclusive)
    dividers = sorted(random.sample(range(0, extras + 1), parts - 1))
    partition = []
    previous = 0
    for divider in dividers:
        partition.append(divider - previous)
        previous = divider
    # Append the final portion
    partition.append(extras - previous)
    # Add 1 back to every part to meet the minimum count requirement
    partition = [x + 1 for x in partition]
    return partition

# Define the dimension values
channels = ["Mailer", "In-flight", "Social", "Online", "Telesales"]
credit_cards = ["Yes", "No"]
geographies = ["Northeast", "Southeast", "Southwest", "Northwest", "Great Plains"]
revenues = ["0-2000", "2000-5000", "5000-10000", "10000+"]
day_flags = ["Weekday", "Weekend"]

# Compute all combinations (should be 5 * 2 * 5 * 4 * 2 = 400 combinations)
combos = list(itertools.product(channels, credit_cards, geographies, revenues, day_flags))
num_combos = len(combos)

# Create a list of month strings from January 2024 to February 2025 (inclusive)
months = pd.date_range(start="2024-01-01", end="2025-02-01", freq="MS")

# Prepare a list to hold all records
all_rows = []
sequence_number = 1  # Initialize sequence number

# For each month, generate the records
for month in months:
    # Set a monthly target total signups between 2000 and 5000
    monthly_target = random.randint(2000, 5000)
    # Distribute the monthly target among the 400 dimension combinations
    partition = random_partition(monthly_target, num_combos)
    
    # Loop over each combination and assign the corresponding partition value
    for (combo, signups) in zip(combos, partition):
        channel, credit_card, geography, revenue, day_flag = combo
        all_rows.append({
            "ID": sequence_number,
            "Month": month.strftime("%Y-%m"),  # Format: YYYY-MM
            "Channel": channel,
            "Credit_Card": credit_card,
            "Geography": geography,
            "Predicted_Annual_Revenue": revenue,
            "Day_Flag": day_flag,
            "Total_signups": signups
        })
        sequence_number += 1  # Increment sequence number

# Create a DataFrame from the rows
df = pd.DataFrame(all_rows)

# Add these lines right after creating the DataFrame
print("First 5 rows of ID column:", df['ID'].head())
print("Last 5 rows of ID column:", df['ID'].tail())
print("Number of unique IDs:", df['ID'].nunique())
print("Total number of rows:", len(df))
# Write the DataFrame to a CSV in the Downloads folder (for Mac user jpetrides)
csv_file_path = "/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Airline/rewards_signups.csv"
df.to_csv(csv_file_path, index=False)
print(f"Dataset successfully written to {csv_file_path}")


# Parquet output
parquet_file_path = "/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Airline/rewards_signups.parquet"
df.to_parquet(parquet_file_path, index=False)
print(f"Dataset successfully written to {parquet_file_path}")