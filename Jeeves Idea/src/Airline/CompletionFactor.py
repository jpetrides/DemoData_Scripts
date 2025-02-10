import itertools
import random
import pandas as pd

# Define the dimension values
route_names = [
    "KABQ - KPDX",
    "KABQ - KSEA",
    "KATL - KSEA",
    "KAUS - KSAN",
    "KBNA - KSEA",
    "KBOI - KAUS",
    "KBOI - KLAX",
    "KBOI - KORD",
    "KDCA - KSFO"
]

# Southwest Airlines aircraft types
aircraft_types = [
    "Boeing 737-700",  # Older model in SWA's fleet
    "Boeing 737-800",  # Common model in SWA's fleet
    "Boeing 737 MAX 8" # Newer model in SWA's fleet
]

flight_shifts = ["Morning", "Afternoon", "Evening", "Night"]

# New dimension: AOG Events bucketed values (Aircraft on Ground events)
aog_events = ["0", "1-2", "3-5", "6+"]

# Create a list of month strings from January 2024 to February 2025 (inclusive)
months = pd.date_range(start="2024-01-01", end="2025-02-01", freq="MS")

# Prepare the list to hold all records
records = []
sequence_number = 1  # Initialize sequence number

for month in months:
    month_str = month.strftime("%Y-%m")
    # For each combination of route, aircraft type, and flight shift
    for route, aircraft, shift in itertools.product(route_names, aircraft_types, flight_shifts):
        # Scheduled flights between 20 and 100
        total_scheduled = random.randint(20, 100)
        # Ensure completed flights are at least 80% of total scheduled for realism
        lower_bound = max(1, int(total_scheduled * 0.8))
        completed = random.randint(lower_bound, total_scheduled)
        # Calculate the factor (ensure division by total_scheduled is safe)
        completion_factor = round(completed / total_scheduled, 2)
        
        # Generate a random training completion rate between 0.0 and 1.0 (rounded to 2 decimals)
        training_completion_rate = round(random.uniform(0.0, 1.0), 2)
        # Determine if weather adversely impacted the flight (roughly 30% chance True)
        weather_impact = random.choices([True, False], weights=[0.3, 0.7])[0]
        # Randomly select an AOG Events bucket
        aog_bucket = random.choice(aog_events)
        
        records.append({
            "ID": sequence_number,
            "Month": month_str,
            "Route_Name": route,
            "Aircraft_Type": aircraft,
            "Flight_Shift": shift,
            "Total_Scheduled_Flights": total_scheduled,
            "Completed_Flights": completed,
            "Completion_Factor": completion_factor,
            "Training_Completion_Rate": training_completion_rate,
            "Weather_Impact": weather_impact,
            "AOG_Events": aog_bucket
        })
        sequence_number += 1  # Increment sequence number

# Convert the list of records into a DataFrame
df = pd.DataFrame(records)

# Output options - comment out the one you don't want to use
# CSV output
#csv_file_path = "/Users/jpetrides/Downloads/flight_metrics_dataset.csv"
#df.to_csv(csv_file_path, index=False)
#print(f"Dataset successfully written to {csv_file_path}")

# Parquet output
parquet_file_path = "/Users/jpetrides/Downloads/flight_metrics_dataset.parquet"
df.to_parquet(parquet_file_path, index=False)
print(f"Dataset successfully written to {parquet_file_path}")