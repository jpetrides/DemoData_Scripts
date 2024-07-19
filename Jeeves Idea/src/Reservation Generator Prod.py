import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import duckdb

# Set random seed for reproducibility
np.random.seed(42)

# Constants
NUM_HOTELS = 100
ROOMS_PER_HOTEL = 100
DAYS_IN_YEAR = 365
TOTAL_DAYS = 2 * DAYS_IN_YEAR  # 2 years of data
OCCUPANCY_MIN = 0.70
OCCUPANCY_MAX = 0.90
ADR = np.linspace(500, 100, NUM_HOTELS)  # Generate ADR values for 100 hotels
BOOKING_CHANNELS = ['OTA', 'Direct', 'Group']
BOOKING_CHANNEL_PROB = [0.45, 0.45, 0.10]  # Adjusted probabilities
AVG_LENGTH_OF_STAY = 3
MAX_LENGTH_OF_STAY = 20
AVG_LEAD_TIME = 7
MAX_LEAD_TIME = 200
NUM_CUSTOMERS = 150000

# Generate data
records = []
reservation_id = 1

for hotel_id in range(1, NUM_HOTELS + 1):
    adr = ADR[hotel_id - 1]
    num_rooms = ROOMS_PER_HOTEL
    occupancy_rate = np.random.uniform(OCCUPANCY_MIN, OCCUPANCY_MAX)
    total_room_nights = int(num_rooms * TOTAL_DAYS * occupancy_rate)
    
    room_nights_count = 0
    daily_occupancy = np.zeros(TOTAL_DAYS)  # Track daily occupancy to prevent overbooking
    
    while room_nights_count < total_room_nights:
        customer_id = np.random.randint(1, NUM_CUSTOMERS + 1)
        booking_channel = np.random.choice(BOOKING_CHANNELS, p=BOOKING_CHANNEL_PROB)
        
        checkin_date = datetime(2023, 1, 1) + timedelta(days=np.random.randint(0, TOTAL_DAYS))
        length_of_stay = np.random.poisson(AVG_LENGTH_OF_STAY)
        if length_of_stay < 1:
            length_of_stay = 1
        if length_of_stay > MAX_LENGTH_OF_STAY:
            length_of_stay = MAX_LENGTH_OF_STAY
        
        checkout_date = checkin_date + timedelta(days=length_of_stay)
        
        # Ensure no overbooking
        checkin_day = (checkin_date - datetime(2023, 1, 1)).days
        checkout_day = (checkout_date - datetime(2023, 1, 1)).days
        if checkout_day > TOTAL_DAYS:
            checkout_day = TOTAL_DAYS
        
        if np.all(daily_occupancy[checkin_day:checkout_day] < num_rooms):
            daily_occupancy[checkin_day:checkout_day] += 1
            room_nights_count += length_of_stay
            
            lead_time = int(np.random.exponential(AVG_LEAD_TIME))
            lead_time = min(lead_time, MAX_LEAD_TIME)
            reservation_date = checkin_date - timedelta(days=lead_time)
            
            records.append({
                'ReservationID': reservation_id,
                'HotelID': hotel_id,
                'CustomerID': customer_id,
                'BookingChannel': booking_channel,
                'CheckinDate': checkin_date,
                'CheckoutDate': checkout_date,
                'ReservationDate': reservation_date,
                'ADR': adr,
                'LengthOfStay': length_of_stay
            })
            reservation_id += 1

# Create DataFrame
df = pd.DataFrame(records)

# Connect to DuckDB
db_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/hotel_reservations.duckdb'
con = duckdb.connect(db_path)

# Create table if not exists
con.execute('''
CREATE TABLE IF NOT EXISTS Reservations3 (
    ReservationID INTEGER,
    HotelID INTEGER,
    CustomerID INTEGER,
    BookingChannel VARCHAR,
    CheckinDate TIMESTAMP,
    CheckoutDate TIMESTAMP,
    ReservationDate TIMESTAMP,
    ADR DOUBLE,
    LengthOfStay INTEGER
)
''')

# Insert data into the table
con.execute('''
INSERT INTO Reservations3
SELECT * FROM df
''')

# SQL to clear out excess occupancy
excess_occupancy_sql = '''
WITH occupancy_cte AS (
    SELECT cast(c.date as date) as Hotel_Date,
           r.HotelID,
           r.ReservationID,
           COUNT(*) OVER (PARTITION BY cast(c.date as date), r.HotelID) as daily_bookings,
           ROW_NUMBER() OVER (PARTITION BY cast(c.date as date), r.HotelID 
                              ORDER BY r.ReservationID DESC) as row_num
    FROM hotel_reservations.main.calendar_reference c 
    INNER JOIN hotel_reservations.main.Reservations3 r
    ON c.date BETWEEN r.CheckinDate AND r.CheckoutDate
),
reservations_to_delete AS (
    SELECT DISTINCT ReservationID
    FROM occupancy_cte
    WHERE daily_bookings > 100 AND row_num > 100
)
DELETE FROM hotel_reservations.main.Reservations3
WHERE ReservationID IN (SELECT ReservationID FROM reservations_to_delete);
'''

# Execute the SQL to clear out excess occupancy
con.execute(excess_occupancy_sql)

#Set brand per ADR
con.execute('''
alter table hotel_reservations.main.Reservations3 add Brand varchar(50);
UPDATE hotel_reservations.main.Reservations3 set Brand =  
case 
when ADR <=120 then 'Sleep Ease'
when ADR <=170 then 'Breezeway Hotels'
when ADR <=250 then 'Citrus and Sage'
when ADR <=450 then 'Monarch Plaza'
ELSE 'Empyrean Collection'
end;
''')

#Add child and Adult Counts
con.execute('''
alter table hotel_reservations.main.Reservations3 add adult_count integer;
alter table hotel_reservations.main.Reservations3 add child_count integer;
alter table hotel_reservations.main.Reservations3 add total_guests integer;
UPDATE hotel_reservations.main.Reservations3 SET adult_count = (RANDOM() * 3 + 1)::INTEGER;
update hotel_reservations.main.Reservations3 set child_count = (RANDOM() * 0 + 4)::INTEGER;
update hotel_reservations.main.Reservations3 set total_guests = adult_count + child_count;
''')

#mix up the ADR a little bit so the hotels within a brand down't all have the same ADR
#RANDOM() generates a random number between 0 and 1.
#Multiplying it by 0.2 gives a random number between 0 and 0.2.
#Subtracting 0.1 shifts this to a range between -0.1 and 0.1.
#Adding 1 gives us a multiplier between 0.9 and 1.1.
#Multiplying the original ADR by this factor will increase or decrease it by up to 10%.
con.execute('''
UPDATE hotel_reservations.main.Reservations3 SET ADR = ADR * (1 + (RANDOM() * 0.2 - 0.1));''')

#add row level Rooms revenue to each reservation record
con.execute ('''alter table hotel_reservations.main.Reservations3 add RM_Revenue decimal (10,2);
update hotel_reservations.main.Reservations3 SET RM_Revenue = ADR * LengthOfStay ;''')

#fix reservation date that is throwing off the Total bookings metric
#reservations should be between yesterday and 150 days ago
con.execute(
'''UPDATE hotel_reservations.main.Reservations3
SET ReservationDate = CheckinDate - INTERVAL (1 + RANDOM() % 150) DAY;''')

#Create hotel Revenue Daily Table
con.execute ('''
-- what is my daily occupancy, ADR, and RevPAR?
Create Table hotel_reservations.main.Hotel_Revenue_Daily as (
select cast(c.date as date) as Hotel_Date
,r.HotelID 
,sum(r.ADR) as revenue
,count(*) as "Rm_Nights"
,sum(total_guests) as total_guests
,100 as Available_Rooms
from hotel_reservations.main.calendar_reference c 
inner join  hotel_reservations.main.Reservations3 r
on c.date between  r.CheckinDate and r.CheckoutDate 
group by 1,2
);

-- Where are my ADR and Room nights coming from?
Create Table hotel_reservations.main.Hotel_Revenue_Daily_DTL as (
select cast(c.date as date) as Hotel_Date
,r.HotelID 
,r.ReservationID
,sum(r.ADR) as revenue
,count(*) as "Rm_Nights"
from hotel_reservations.main.calendar_reference c 
inner join  hotel_reservations.main.Reservations3 r
on c.date between  r.CheckinDate and r.CheckoutDate 
group by 1,2,3
);
''') 

# Close the connection to duckdb
con.close()