import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)
fake = Faker()

def generate_d_account(num_accounts=100):
    data = []
    
    for i in range(1, num_accounts + 1):
        account_id = f"ACCT_{i:02d}"
        geo_id = f"GEO_{random.randint(1, 35)}"
        
        row = {
            'Account_Name': f"{fake.first_name()} {fake.last_name()}",
            'Account_ID': account_id,
            'Geo_ID': geo_id,
            'Prioritization_Score': random.uniform(1, 100),
            'Open_CTAs': random.randint(20, 80),
            'Open_Cases': random.randint(20, 80),
            'Open_Claims': random.randint(20, 80),
            'Activities': random.randint(20, 80),
            'Leads': random.randint(20, 80),
            'Bookings_L365': random.randint(50, 210),
            'GBV_L365': random.randint(5200, 5200000),
            'Nights_L365': random.randint(180, 365),
            'Avg_Review_Score_L365': random.uniform(3.5, 5.0),
            'Forward_Occupancy_12W': random.uniform(0.5, 0.9),
            'Forward_Availability_12W': random.uniform(0.5, 0.9),
            'Forward_Availability_52W': random.uniform(0.5, 0.9),
            'Host_Retention_Score': random.randint(50, 100),
            'Total_Open_Items': random.randint(1, 200)
        }
        data.append(row)
    
    df = pd.DataFrame(data)
    df.to_csv("/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Vacation_Rental/D_Account.csv", index=False)
    return df

def generate_d_listing(d_account_df, num_listings=500):
    data = []
    statuses = ['Active', 'Inactive', 'Pending', 'Suspended']
    
    for i in range(1, num_listings + 1):
        account_id = random.choice(d_account_df['Account_ID'].values)
        
        row = {
            'Listing_Name': fake.company(),
            'Listing_ID': f"LIST_{i:03d}",
            'Account_ID': account_id,
            'Guest_Favorite': random.choice([True, False]),
            'Geo_ID': f"GEO_{random.randint(1, 35)}",
            'Host_Quality_System_Status': random.choice(statuses),
            'Avg_Overall_Rating_Lifetime': random.uniform(3.5, 5.0),
            'Avg_Overall_Rating_L365': random.uniform(3.5, 5.0),
            'Bookings_L365': random.randint(10, 100),
            'GBV_L365': random.randint(1000, 100000),
            'Nights_L365': random.randint(30, 365),
            'Share_Available_Nights_12W': random.uniform(0.3, 0.9),
            'Forward_Nights_Available_12W': random.randint(30, 84),
            'Forward_Nights_Available_52W': random.randint(100, 365)
        }
        data.append(row)
    
    df = pd.DataFrame(data)
    df.to_csv("/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Vacation_Rental/D_Listing.csv", index=False)
    return df

def generate_d_hotel(d_account_df, num_hotels=50):
    data = []
    
    for i in range(1, num_hotels + 1):
        hotel_id = f"HOTEL_{i:02d}"
        account_id = random.choice(d_account_df['Account_ID'].values)
        
        # Generate booking window mix that sums to 100%
        same_day = random.uniform(0.1, 0.3)
        one_to_seven = random.uniform(0.3, 0.5)
        eight_plus = 1 - (same_day + one_to_seven)
        
        row = {
            'Hotel_Name': f"{fake.company()} Hotel",
            'Listing_ID': hotel_id,
            'Account_ID': account_id,
            'Geo_ID': f"GEO_{random.randint(1, 35)}",
            'Bookings_MTD': random.randint(50, 200),
            'Bookings_QTD': random.randint(150, 600),
            'Bookings_YTD': random.randint(600, 2400),
            'GBV_MTD': random.randint(5000, 20000),
            'GBV_QTD': random.randint(15000, 60000),
            'GBV_YTD': random.randint(60000, 240000),
            'Room_Nights_MTD': random.randint(100, 400),
            'Room_Nights_QTD': random.randint(300, 1200),
            'Room_Nights_YTD': random.randint(1200, 4800),
            'Avg_LOS': random.uniform(1.5, 5.0),
            'Booking_Window_Same_Day': same_day,
            'Booking_Window_1_7_Days': one_to_seven,
            'Booking_Window_8_Plus_Days': eight_plus
        }
        data.append(row)
    
    df = pd.DataFrame(data)
    df.to_csv("/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Vacation_Rental/D_Hotel.csv", index=False)
    return df

def generate_d_reservation(d_hotel_df):
    data = []
    start_date = datetime.now() - timedelta(days=25*30)  # 25 months ago
    
    for hotel_id in d_hotel_df['Listing_ID'].values:
        # Generate between 500-1000 reservations per hotel
        num_reservations = random.randint(500, 1000)
        
        for _ in range(num_reservations):
            reservation_date = start_date + timedelta(days=random.randint(0, 750))
            length_of_stay = random.randint(1, 14)
            check_in_date = reservation_date + timedelta(days=random.randint(1, 90))
            check_out_date = check_in_date + timedelta(days=length_of_stay)
            
            row = {
                'Reservation_ID': f"RES_{fake.uuid4()[:8]}",
                'Hotel_ID': hotel_id,
                'Check_in_Date': check_in_date.date(),
                'Check_out_Date': check_out_date.date(),
                'Length_of_Stay': length_of_stay,
                'Reservation_Date': reservation_date.date(),
                'Rate_Code': f"R_{random.randint(1, 100):02d}"
            }
            data.append(row)
    
    df = pd.DataFrame(data)
    df.to_csv("/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Vacation_Rental/D_Reservation.csv", index=False)
    return df

# Generate all tables
d_account_df = generate_d_account()
d_listing_df = generate_d_listing(d_account_df)
d_hotel_df = generate_d_hotel(d_account_df)
d_reservation_df = generate_d_reservation(d_hotel_df)