import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Set random seed for reproducibility
np.random.seed(42)

# Generate Sellers data
def generate_sellers(num_sellers):
    sellers = []
    for _ in range(num_sellers):
        sellers.append({
            'SellerID': fake.unique.random_number(digits=5),
            'SellerName': fake.name(),
            'SellerEmail': fake.email(),
            'SellerPhone': fake.phone_number()
        })
    return pd.DataFrame(sellers)

# Generate Leads data
def generate_leads(num_leads, sellers):
    leads = []
    sources = ['Website', 'Referral', 'Social Media', 'Direct Mail', 'Phone Inquiry']
    statuses = ['New', 'In Progress', 'Closed']
    for _ in range(num_leads):
        leads.append({
            'LeadID': fake.unique.random_number(digits=6),
            'LeadSource': np.random.choice(sources),
            'AssignedSellerID': np.random.choice(sellers['SellerID']),
            'DateAssigned': fake.date_between(start_date='-1y', end_date='today'),
            'Status': np.random.choice(statuses, p=[0.2, 0.6, 0.2])
        })
    return pd.DataFrame(leads)

# Generate Touchpoints data
def generate_touchpoints(leads, sellers):
    touchpoints = []
    types = ['Call', 'Email']
    statuses = ['Attempted', 'Successful']
    for _, lead in leads.iterrows():
        num_touchpoints = np.random.randint(1, 6)
        for _ in range(num_touchpoints):
            touchpoints.append({
                'TouchpointID': fake.unique.random_number(digits=7),
                'LeadID': lead['LeadID'],
                'SellerID': lead['AssignedSellerID'],
                'TouchpointType': np.random.choice(types),
                'TouchpointDate': fake.date_between(start_date=lead['DateAssigned'], end_date='today'),
                'TouchpointStatus': np.random.choice(statuses, p=[0.3, 0.7])
            })
    return pd.DataFrame(touchpoints)

# Generate Tours data
def generate_tours(leads, sellers):
    tours = []
    statuses = ['Booked', 'Actualized', 'Cancelled']
    for _, lead in leads.iterrows():
        if np.random.random() < 0.4:  # 40% chance of booking a tour
            tours.append({
                'TourID': fake.unique.random_number(digits=6),
                'LeadID': lead['LeadID'],
                'SellerID': lead['AssignedSellerID'],
                'TourDate': fake.date_between(start_date=lead['DateAssigned'], end_date='+30d'),
                'TourStatus': np.random.choice(statuses, p=[0.2, 0.6, 0.2])
            })
    return pd.DataFrame(tours)

# Generate Contracts data
def generate_contracts(tours):
    contracts = []
    statuses = ['Pending', 'Good Pending', 'Shaky Pending', 'Actualized', 'Cancelled']
    cancellation_reasons = ['Resting Period', 'Customer Care Contract Relief', 'Changed Mind', 'Financial Issues']
    for _, tour in tours.iterrows():
        if tour['TourStatus'] == 'Actualized' and np.random.random() < 0.7:  # 70% chance of contract for actualized tour
            status = np.random.choice(statuses, p=[0.1, 0.2, 0.1, 0.5, 0.1])
            contracts.append({
                'ContractID': fake.unique.random_number(digits=7),
                'LeadID': tour['LeadID'],
                'SellerID': tour['SellerID'],
                'ContractDate': tour['TourDate'] + timedelta(days=np.random.randint(0, 7)),
                'ContractStatus': status,
                'DepositReceived': status in ['Actualized', 'Good Pending'],
                'CancellationReason': np.random.choice(cancellation_reasons) if status == 'Cancelled' else None
            })
    return pd.DataFrame(contracts)

# Generate all data
sellers = generate_sellers(20)
leads = generate_leads(10000, sellers)
touchpoints = generate_touchpoints(leads, sellers)
tours = generate_tours(leads, sellers)
contracts = generate_contracts(tours)

# Save to CSV files
sellers.to_csv('sellers.csv', index=False)
leads.to_csv('leads.csv', index=False)
touchpoints.to_csv('touchpoints.csv', index=False)
tours.to_csv('tours.csv', index=False)
contracts.to_csv('contracts.csv', index=False)

print("Sample data generated and saved to CSV files.")