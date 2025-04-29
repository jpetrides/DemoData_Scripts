import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)

# Generate date range
start_date = datetime(2023, 1, 1)
end_date = datetime(2025, 4, 1)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Define dimensions
airports = [
    ('ATL', 'Atlanta Hartsfield-Jackson', 'Southeast'), 
    ('LAX', 'Los Angeles International', 'West'),
    ('ORD', 'Chicago O\'Hare', 'Midwest'),
    ('DFW', 'Dallas/Fort Worth', 'Southwest'),
    ('DEN', 'Denver International', 'Mountain'),
    ('JFK', 'New York Kennedy', 'Northeast'),
    ('SFO', 'San Francisco International', 'West'),
    ('SEA', 'Seattle-Tacoma', 'Northwest'),
    ('LAS', 'Las Vegas McCarran', 'West'),
    ('MCO', 'Orlando International', 'Southeast'),
    ('MIA', 'Miami International', 'Southeast'),
    ('CLT', 'Charlotte Douglas', 'Southeast'),
    ('PHX', 'Phoenix Sky Harbor', 'Southwest'),
    ('IAH', 'Houston George Bush', 'Southwest'),
    ('BOS', 'Boston Logan', 'Northeast'),
    ('EWR', 'Newark Liberty', 'Northeast'),
    ('MSP', 'Minneapolis-Saint Paul', 'Midwest'),
    ('DTW', 'Detroit Metropolitan', 'Midwest'),
    ('PHL', 'Philadelphia International', 'Northeast'),
    ('LGA', 'New York LaGuardia', 'Northeast')
]

aircraft_types = [
    ('B737', 'Narrow-body'),
    ('B787', 'Wide-body'),
    ('A320', 'Narrow-body'),
    ('A330', 'Wide-body'),
    ('E175', 'Regional'),
    ('CRJ7', 'Regional'),
    ('B777', 'Wide-body'),
    ('A350', 'Wide-body'),
    ('B757', 'Narrow-body'),
    ('A321', 'Narrow-body')
]

weather_conditions = ['Clear', 'Rain', 'Snow', 'Fog', 'Thunderstorm', 'Wind']
shifts = ['Morning', 'Afternoon', 'Night']
handling_companies = ['GlobalAir Services', 'AeroGround', 'AirportPro Handlers', 'FastTrack Ground', 'UniGround']
phases = ['Arrival', 'Departure', 'Towing', 'Fueling', 'Baggage Loading', 'Catering', 'Maintenance']
damage_types = ['Vehicle Collision', 'Jet Bridge Contact', 'Ground Equipment Impact', 'Loading Error', 'Pushback Incident', 'Towing Mishap', 'Weather-Related']
damage_severities = ['Minor', 'Moderate', 'Severe']

# Create empty list to store records
data = []

# Base departure counts by airport size (daily)
airport_sizes = {
    'ATL': (800, 1000), 'LAX': (600, 800), 'ORD': (700, 900), 'DFW': (600, 800), 'DEN': (500, 700),
    'JFK': (400, 600), 'SFO': (400, 600), 'SEA': (350, 550), 'LAS': (300, 500), 'MCO': (350, 550),
    'MIA': (300, 500), 'CLT': (400, 600), 'PHX': (350, 550), 'IAH': (300, 500), 'BOS': (300, 500),
    'EWR': (250, 450), 'MSP': (250, 450), 'DTW': (250, 450), 'PHL': (200, 400), 'LGA': (200, 400)
}

# Base incident rates per aircraft type (per 1000 departures)
base_incident_rates = {
    'B737': 1.4, 'B787': 1.2, 'A320': 1.3, 'A330': 1.1, 'E175': 1.8,
    'CRJ7': 1.9, 'B777': 1.0, 'A350': 0.9, 'B757': 1.5, 'A321': 1.3
}

# Seasonal factors by month (1-indexed)
seasonal_factors = {
    1: 1.2,   # January (winter)
    2: 1.15,  # February (winter)
    3: 1.0,   # March
    4: 0.95,  # April
    5: 0.9,   # May
    6: 0.85,  # June
    7: 0.9,   # July (summer travel)
    8: 0.95,  # August (summer travel)
    9: 0.9,   # September
    10: 0.95, # October
    11: 1.1,  # November (holiday)
    12: 1.25  # December (winter, holiday)
}

# Weather impact on incident rates
weather_factors = {
    'Clear': 0.8, 'Rain': 1.3, 'Snow': 1.8, 'Fog': 1.4, 'Thunderstorm': 1.6, 'Wind': 1.3
}

# Shift impact on incident rates
shift_factors = {
    'Morning': 0.9, 'Afternoon': 1.0, 'Night': 1.4
}

# Handler company quality
handler_factors = {
    'GlobalAir Services': 0.9,
    'AeroGround': 1.0,
    'AirportPro Handlers': 1.1,
    'FastTrack Ground': 1.3,
    'UniGround': 0.8
}

# Generate departureS data
print("Generating base departure data...")
row_count = 0

for date in date_range:
    # Add seasonal traffic variations
    season_traffic_multiplier = 1.0
    
    # Summer traffic increase
    if date.month in [6, 7, 8]:
        season_traffic_multiplier = 1.15
    
    # Holiday season
    if date.month == 12 and date.day >= 15:
        season_traffic_multiplier = 1.2
    
    # Weekend multiplier
    if date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        weekend_multiplier = 0.85
    else:
        weekend_multiplier = 1.05
        
    # Incorporate a growth trend over time
    days_since_start = (date - start_date).days
    growth_factor = 1.0 + (days_since_start / 1000)  # Slow traffic growth over time
    
    # Generate departures for each airport and aircraft type combination
    for airport_code, airport_name, region in airports:
        min_departures, max_departures = airport_sizes[airport_code]
        airport_departures_today = int(np.random.randint(min_departures, max_departures) * 
                                      season_traffic_multiplier * weekend_multiplier * growth_factor)
        
        # Distribute departures across aircraft types based on airport and aircraft type
        aircraft_type_distribution = {}
        for ac_type, category in aircraft_types:
            if category == 'Regional' and airport_code in ['ATL', 'ORD', 'DFW', 'CLT']:
                # Major hubs have more regional flights
                aircraft_type_distribution[ac_type] = np.random.uniform(0.1, 0.2)
            elif category == 'Wide-body' and airport_code in ['JFK', 'LAX', 'SFO', 'MIA']:
                # International gateways have more wide-bodies
                aircraft_type_distribution[ac_type] = np.random.uniform(0.15, 0.25)
            elif category == 'Narrow-body':
                # Narrow-bodies are common everywhere
                aircraft_type_distribution[ac_type] = np.random.uniform(0.05, 0.15)
            else:
                aircraft_type_distribution[ac_type] = np.random.uniform(0.02, 0.08)
                
        # Normalize to ensure distribution sums to 1
        total = sum(aircraft_type_distribution.values())
        for ac_type in aircraft_type_distribution:
            aircraft_type_distribution[ac_type] /= total
            
        # Create records for each aircraft type
        for ac_type, category in aircraft_types:
            # Calculate departures for this aircraft type
            type_departures = int(airport_departures_today * aircraft_type_distribution[ac_type])
            
            if type_departures > 0:
                # Weather condition for the day (airport-specific)
                weather = np.random.choice(weather_conditions, p=[0.6, 0.15, 0.05, 0.05, 0.05, 0.1])
                
                # For each shift
                for shift in shifts:
                    shift_proportion = {
                        'Morning': 0.4,
                        'Afternoon': 0.4,
                        'Night': 0.2
                    }
                    
                    shift_departures = int(type_departures * shift_proportion[shift])
                    if shift_departures == 0:
                        continue
                    
                    # Handling company (airport may have preferred vendors)
                    handler = np.random.choice(handling_companies)
                    
                    # Base incident rate considering all factors
                    base_rate = base_incident_rates[ac_type]
                    seasonal_factor = seasonal_factors[date.month]
                    weather_factor = weather_factors[weather]
                    shift_factor = shift_factors[shift]
                    handler_factor = handler_factors[handler]
                    
                    # Improve AGD over time (safety program effect starting 2024)
                    if date >= datetime(2024, 1, 1):
                        improvement_factor = 0.95 - (0.05 * (date - datetime(2024, 1, 1)).days / 365)
                        # Cap the improvement
                        improvement_factor = max(0.75, improvement_factor)
                    else:
                        improvement_factor = 1.0
                    
                    # Calculate adjusted incident rate per 1000 departures
                    adjusted_rate = base_rate * seasonal_factor * weather_factor * shift_factor * handler_factor * improvement_factor
                    
                    # Expected incidents for this grouping
                    expected_incidents = (adjusted_rate * shift_departures) / 1000
                    
                    # Actual incidents (Poisson distribution around the expected rate)
                    incidents = np.random.poisson(expected_incidents)
                    
                    # Add a record even if no incidents
                    agd_rate = (incidents / shift_departures) * 1000 if shift_departures > 0 else 0
                    
                    record = {
                        'date': date.strftime('%Y-%m-%d'),
                        'airport_code': airport_code,
                        'airport_name': airport_name,
                        'airport_region': region,
                        'aircraft_type': ac_type,
                        'aircraft_category': category,
                        'weather_condition': weather,
                        'shift': shift,
                        'handling_company': handler,
                        'departures': shift_departures,
                        'incidents': incidents,
                        'agd_rate': agd_rate
                    }
                    
                    data.append(record)
                    row_count += 1
                    
                    # If we have incidents, create detailed incident records
                    if incidents > 0:
                        for i in range(incidents):
                            phase = np.random.choice(phases)
                            damage_type = np.random.choice(damage_types)
                            
                            # Correlate damage type with phase
                            if phase == 'Arrival' and damage_type == 'Pushback Incident':
                                damage_type = 'Jet Bridge Contact'
                            elif phase == 'Fueling' and damage_type == 'Jet Bridge Contact':
                                damage_type = 'Ground Equipment Impact'
                                
                            severity = np.random.choice(damage_severities, p=[0.6, 0.3, 0.1])
                            
                            # Estimate costs based on severity
                            if severity == 'Minor':
                                repair_cost = np.random.uniform(5000, 20000)
                                delay_minutes = np.random.randint(0, 60)
                            elif severity == 'Moderate':
                                repair_cost = np.random.uniform(20000, 100000)
                                delay_minutes = np.random.randint(60, 180)
                            else:  # Severe
                                repair_cost = np.random.uniform(100000, 500000)
                                delay_minutes = np.random.randint(180, 720)
                                
                            incident_record = record.copy()
                            incident_record['phase_of_operation'] = phase
                            incident_record['damage_type'] = damage_type
                            incident_record['damage_severity'] = severity
                            incident_record['repair_cost'] = round(repair_cost, 2)
                            incident_record['delay_minutes'] = delay_minutes
                            
                            # Add record ID for incident records
                            incident_record['record_type'] = 'incident'
                            incident_record['incident_id'] = f"INC-{date.strftime('%Y%m%d')}-{airport_code}-{i+1}"
                            
                            data.append(incident_record)
                            row_count += 1

print(f"Generated {row_count} records")

# Create DataFrame
df = pd.DataFrame(data)

# Add one-off incident spikes for certain airports to create outlier events
outlier_events = [
    {'date': '2023-07-15', 'airport': 'ORD', 'factor': 3.0, 'reason': 'Severe thunderstorm'},
    {'date': '2023-12-23', 'airport': 'DEN', 'factor': 2.5, 'reason': 'Major snowstorm'},
    {'date': '2024-03-10', 'airport': 'ATL', 'factor': 2.0, 'reason': 'Ground staff strike'},
    {'date': '2024-08-05', 'airport': 'MIA', 'factor': 2.5, 'reason': 'Hurricane warning'},
    {'date': '2024-11-28', 'airport': 'LGA', 'factor': 1.8, 'reason': 'Thanksgiving travel surge'}
]

# Function to inject outliers
def inject_outliers(df, outlier_events):
    for event in outlier_events:
        # Find records for this date and airport
        mask = (df['date'] == event['date']) & (df['airport_code'] == event['airport'])
        
        # Get indices of records to modify
        indices = df[mask].index
        
        # Multiply incidents by the factor
        for idx in indices:
            if 'incidents' in df.loc[idx]:
                old_incidents = df.loc[idx, 'incidents']
                new_incidents = int(old_incidents * event['factor'])
                additional = new_incidents - old_incidents
                
                if additional > 0:
                    df.loc[idx, 'incidents'] = new_incidents
                    
                    # Recalculate AGD rate
                    departures = df.loc[idx, 'departures']
                    if departures > 0:
                        df.loc[idx, 'agd_rate'] = (new_incidents / departures) * 1000
                    
                    # Add additional incident records
                    base_record = df.loc[idx].to_dict()
                    if 'record_type' not in base_record or base_record['record_type'] != 'incident':
                        for i in range(additional):
                            phase = np.random.choice(phases)
                            damage_type = np.random.choice(damage_types)
                            severity = np.random.choice(damage_severities, p=[0.4, 0.4, 0.2])  # Higher severity for outlier events
                            
                            if severity == 'Minor':
                                repair_cost = np.random.uniform(5000, 20000)
                                delay_minutes = np.random.randint(0, 60)
                            elif severity == 'Moderate':
                                repair_cost = np.random.uniform(20000, 100000)
                                delay_minutes = np.random.randint(60, 180)
                            else:  # Severe
                                repair_cost = np.random.uniform(100000, 500000)
                                delay_minutes = np.random.randint(180, 720)
                                
                            incident_record = base_record.copy()
                            incident_record['phase_of_operation'] = phase
                            incident_record['damage_type'] = damage_type
                            incident_record['damage_severity'] = severity
                            incident_record['repair_cost'] = round(repair_cost, 2)
                            incident_record['delay_minutes'] = delay_minutes
                            incident_record['record_type'] = 'incident'
                            incident_record['incident_id'] = f"INC-{event['date'].replace('-', '')}-{event['airport']}-OUTLIER-{i+1}"
                            incident_record['outlier_event'] = event['reason']
                            
                            # Append to DataFrame
                            df = pd.concat([df, pd.DataFrame([incident_record])], ignore_index=True)
    
    return df

# Inject outliers
df = inject_outliers(df, outlier_events)

# Save the dataset
df.to_csv('aircraft_ground_damage_data.csv', index=False)
print(f"Dataset saved with {len(df)} rows")

# Print sample
print("\nSample data:")
print(df.sample(5))

# Summary statistics
print("\nAGD Rate by Year:")
df['year'] = pd.to_datetime(df['date']).dt.year
yearly_agd = df.groupby('year')['agd_rate'].mean()
print(yearly_agd)

print("\nRow counts:")
print(df.shape[0], "total rows")
print(df[df['incidents'] > 0].shape[0], "rows with incidents")

# Example SQL queries you might use to build stories
print("\nSQL query examples for your demo:")
print("""
-- Get monthly AGD rates across all airports
SELECT 
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    SUM(incidents) AS total_incidents,
    SUM(departures) AS total_departures,
    (SUM(incidents)*1000.0/SUM(departures)) AS monthly_agd_rate
FROM aircraft_ground_damage_data
GROUP BY 1, 2
ORDER BY 1, 2;

-- Find top 5 airports with highest AGD rates in 2024
SELECT 
    airport_code,
    airport_name,
    SUM(incidents) AS total_incidents,
    SUM(departures) AS total_departures,
    (SUM(incidents)*1000.0/SUM(departures)) AS agd_rate
FROM aircraft_ground_damage_data
WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY 1, 2
ORDER BY agd_rate DESC
LIMIT 5;

-- Compare handling companies' performance
SELECT 
    handling_company,
    SUM(incidents) AS total_incidents,
    SUM(departures) AS total_departures,
    (SUM(incidents)*1000.0/SUM(departures)) AS agd_rate
FROM aircraft_ground_damage_data
GROUP BY 1
ORDER BY agd_rate;

-- Analyze impact of weather conditions
SELECT 
    weather_condition,
    SUM(incidents) AS total_incidents,
    SUM(departures) AS total_departures,
    (SUM(incidents)*1000.0/SUM(departures)) AS agd_rate
FROM aircraft_ground_damage_data
GROUP BY 1
ORDER BY agd_rate DESC;
""")