import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random
from faker import Faker
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

# Create output directory if it doesn't exist
output_dir = os.path.expanduser("~/Documents/customers/hotel/mgm")
os.makedirs(output_dir, exist_ok=True)

# Constants
NUM_VISITORS = 50000
NUM_SESSIONS = 150000  # This will result in ~3 million page views with our parameters
BATCH_SIZE = 1000  # Process sessions in batches of 1000
START_DATE = datetime(2025, 4, 1)
END_DATE = datetime(2025, 4, 30)
CONVERSION_RATE = 0.10

# Page names and their probabilities of being viewed
PAGES = {
    "Home Page": 0.25,
    "Login": 0.10,
    "Calendar Select": 0.15,
    "Room Browse": 0.20,
    "Review Order": 0.12,
    "Payment": 0.08,
    "Reservation Confirmation": 0.05,
    "Entertainment": 0.15,
    "Dining": 0.15,
}

# Define traffic sources and their conversion rates
TRAFFIC_SOURCES = {
    "google": {"weight": 0.35, "conversion_factor": 1.0},
    "facebook": {"weight": 0.20, "conversion_factor": 0.9},
    "instagram": {"weight": 0.15, "conversion_factor": 0.8},
    "display": {"weight": 0.10, "conversion_factor": 0.7},
    "OTA": {"weight": 0.10, "conversion_factor": 1.1},
    "direct": {"weight": 0.10, "conversion_factor": 1.5},  # Higher conversion for direct traffic
}

# Campaigns
CAMPAIGNS = ["Campaign_" + str(i) for i in range(1, 21)]

# Device types and their probabilities
DEVICE_TYPES = {
    "desktop": 0.80,
    "mobile": 0.15,
    "tablet": 0.05,
}

# Browsers and their probabilities
BROWSERS = {
    "chrome": 0.60,
    "safari": 0.30,
    "edge": 0.10,
}

# Operating systems and their probabilities
OPERATING_SYSTEMS = {
    "Windows": 0.50,
    "macOS": 0.40,
    "iOS": 0.05,
    "Android": 0.05,
}

# Countries and their probabilities
COUNTRIES = {
    "US": 0.55,
    "Europe": 0.20,
    "Asia": 0.20,
    "Africa": 0.05,
}

# Common user journey patterns
JOURNEY_PATTERNS = {
    "direct_booking": {
        "weight": 0.30,
        "pages": ["Home Page", "Calendar Select", "Room Browse", "Review Order", "Payment", "Reservation Confirmation"],
        "conversion_chance": 0.90
    },
    "browse_then_book": {
        "weight": 0.20,
        "pages": ["Home Page", "Entertainment", "Dining", "Home Page", "Calendar Select", "Room Browse", "Review Order", "Payment", "Reservation Confirmation"],
        "conversion_chance": 0.70
    },
    "abandoned_cart": {
        "weight": 0.25,
        "pages": ["Home Page", "Calendar Select", "Room Browse", "Review Order"],
        "conversion_chance": 0.0
    },
    "just_browsing": {
        "weight": 0.25,
        "pages": ["Home Page", "Entertainment", "Dining", "Home Page"],
        "conversion_chance": 0.0
    },
}

# Events mapping
EVENTS = {
    2: {"name": "Login", "valid_pages": ["Login"]},
    3: {"name": "Add to Cart", "valid_pages": ["Room Browse"]},
    4: {"name": "Property View", "valid_pages": ["Home Page", "Room Browse"]},
    5: {"name": "Property Details", "valid_pages": ["Room Browse"]},
    6: {"name": "remove from cart", "valid_pages": ["Review Order"]},
    7: {"name": "search", "valid_pages": ["Home Page", "Calendar Select"]},
    8: {"name": "date selection", "valid_pages": ["Calendar Select"]},
    9: {"name": "add payment method", "valid_pages": ["Payment"]},
    10: {"name": "booking", "valid_pages": ["Reservation Confirmation"]},
    11: {"name": "view entertainment", "valid_pages": ["Entertainment"]},
    12: {"name": "view dining", "valid_pages": ["Dining"]},
}

# Helper functions
def generate_timestamp(start_date, end_date):
    """Generate a random timestamp between start_date and end_date."""
    # Weekends are busier
    while True:
        delta = (end_date - start_date).days
        random_day = random.randint(0, delta)
        date = start_date + timedelta(days=random_day)
        
        # If it's a weekend (5=Saturday, 6=Sunday), higher chance of selection
        if date.weekday() >= 5:  # Weekend
            if random.random() < 0.6:  # 60% chance to keep a weekend date
                break
        else:  # Weekday
            if random.random() < 0.3:  # 30% chance to keep a weekday date
                break
    
    # Add random hours, minutes, seconds
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    seconds = random.randint(0, 59)
    
    return date.replace(hour=hours, minute=minutes, second=seconds)

def weighted_choice(choices):
    """Make a weighted random choice from a dictionary of options and weights."""
    options = list(choices.keys())
    weights = [choices[opt] if isinstance(choices[opt], (int, float)) else choices[opt]["weight"] for opt in options]
    return random.choices(options, weights=weights, k=1)[0]

def generate_user_agent(device_type, browser, os):
    """Generate a realistic user agent string."""
    if device_type == "desktop":
        if browser == "chrome":
            version = f"{random.randint(90, 115)}.0.{random.randint(1000, 9999)}.{random.randint(10, 99)}"
            return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36"
        elif browser == "safari":
            version = f"{random.randint(13, 17)}.{random.randint(0, 3)}"
            return f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_{random.randint(1, 7)}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{version} Safari/605.1.15"
        else:  # edge
            version = f"{random.randint(90, 115)}.0.{random.randint(1000, 9999)}.{random.randint(10, 99)}"
            return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version} Safari/537.36 Edg/{version}"
    elif device_type == "mobile":
        if os == "iOS":
            version = f"{random.randint(13, 17)}_{random.randint(0, 6)}"
            return f"Mozilla/5.0 (iPhone; CPU iPhone OS {version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
        else:  # Android
            return f"Mozilla/5.0 (Linux; Android {random.randint(9, 13)}; SM-G{random.randint(900, 999)}U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 115)}.0.{random.randint(1000, 9999)}.{random.randint(10, 99)} Mobile Safari/537.36"
    else:  # tablet
        if os == "iOS":
            version = f"{random.randint(13, 17)}_{random.randint(0, 6)}"
            return f"Mozilla/5.0 (iPad; CPU OS {version} like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1"
        else:  # Android
            return f"Mozilla/5.0 (Linux; Android {random.randint(9, 13)}; SM-T{random.randint(700, 899)}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(90, 115)}.0.{random.randint(1000, 9999)}.{random.randint(10, 99)} Safari/537.36"

def generate_session_data(session_num, event_id_start, transaction_id_start, visitors, visit_counters):
    """Generate data for a single session and return page views, events, and reservations."""
    # Select a visitor
    visitor_id = random.choice(visitors)
    visit_id = visit_counters[visitor_id]
    visit_counters[visitor_id] += 1
    session_id = f"session_{session_num}"
    
    # Select traffic source
    traffic_source = weighted_choice(TRAFFIC_SOURCES)
    source_conversion_factor = TRAFFIC_SOURCES[traffic_source]["conversion_factor"]
    
    # Select device, browser, OS
    device_type = weighted_choice(DEVICE_TYPES)
    browser = weighted_choice(BROWSERS)
    operating_system = weighted_choice(OPERATING_SYSTEMS)
    country = weighted_choice(COUNTRIES)
    
    # Decide on campaign (50% chance of having one)
    campaign = random.choice(CAMPAIGNS) if random.random() < 0.5 else None
    
    # Generate user agent
    user_agent = generate_user_agent(device_type, browser, operating_system)
    
    # Select journey pattern
    journey_pattern = weighted_choice(JOURNEY_PATTERNS)
    journey = JOURNEY_PATTERNS[journey_pattern]["pages"].copy()  # Create a copy of the journey
    
    # Adjust for conversion chance based on traffic source
    will_convert = random.random() < (JOURNEY_PATTERNS[journey_pattern]["conversion_chance"] * source_conversion_factor)
    
    # If not converting, truncate journey before confirmation
    if not will_convert and "Reservation Confirmation" in journey:
        confirmation_index = journey.index("Reservation Confirmation")
        potential_exit_points = range(0, confirmation_index)
        if potential_exit_points:
            exit_point = random.choice(potential_exit_points)
            journey = journey[:exit_point + 1]
    
    # Sometimes users view the same page multiple times or go back
    if random.random() < 0.3 and len(journey) > 2:
        # Insert a repeated page
        repeat_idx = random.randint(0, len(journey) - 1)
        journey.insert(repeat_idx + 1, journey[repeat_idx])
    
    # Add some randomness to journey
    if random.random() < 0.2 and len(journey) > 1:
        # Add a random page in the middle
        random_page_idx = random.randint(1, len(journey) - 1)
        random_page = weighted_choice(PAGES)
        journey.insert(random_page_idx, random_page)
    
    # Generate timestamps for this session (chronological)
    session_start = generate_timestamp(START_DATE, END_DATE)
    timestamps = [session_start]
    
    # Generate time gaps between page views (30s to 5 minutes)
    for i in range(1, len(journey)):
        time_on_page = timedelta(seconds=random.randint(30, 300))
        timestamps.append(timestamps[-1] + time_on_page)
    
    # Create page views for this session
    page_views = []
    engagement_events = []
    reservations = []
    event_id = event_id_start
    transaction_id = transaction_id_start
    
    page_view_ids = []  # Store page view IDs for this session
    reservation_created = False  # Flag to track if a reservation has been created
    
    for i, (page_name, timestamp) in enumerate(zip(journey, timestamps)):
        page_view = {
            "event_id": event_id,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_epoch": int(timestamp.timestamp()),
            "visitor_id": visitor_id,
            "visit_id": visit_id,
            "session_id": session_id,
            "page_name": page_name,
            "page_url": "",
            "referrer_url": "",
            "traffic_source": traffic_source,
            "traffic_medium": "",
            "traffic_campaign": campaign,
            "device_type": device_type,
            "browser": browser,
            "browser_version": "",
            "operating_system": operating_system,
            "country": country,
            "region": None,
            "city": None,
            "user_agent": user_agent,
            "is_entrance": "Y" if i == 0 else "N",
            "is_exit": "Y" if i == len(journey) - 1 else "N",
            "ip_address": fake.ipv4(),
            "hit_sequence": i + 1,
            "javascript_enabled": random.choice([True, False]),
            "cookies_enabled": random.choice([True, False]),
            "page_load_time": round(random.uniform(0.50, 5.67), 2)
        }
        
        page_views.append(page_view)
        page_view_ids.append(event_id)
        event_id += 1
        
        # Generate engagement events
        page_name = page_view["page_name"]
        
        # For each page, potentially generate 0-3 engagement events
        for _ in range(random.randint(0, 3)):
            # Find events valid for this page
            valid_events = [event_id for event_id, event in EVENTS.items() 
                           if page_name in event["valid_pages"]]
            
            if valid_events:
                event_code = random.choice(valid_events)
                
                # Skip booking events if we already have one for this session
                if event_code == 10 and reservation_created:
                    continue
                
                engagement_event = {
                    "event_page_view_id": page_view["event_id"],
                    "session_id": session_id,
                    "event_id": event_code,
                    "event_type": "",
                    "event_category": "",
                    "time_on_page": None,  # Will calculate after
                    "scroll_depth": round(random.random(), 2),
                    "is_bounce": "Y" if len(journey) == 1 else "N"
                }
                
                engagement_events.append(engagement_event)
                
                # If this is a booking event and we haven't created a reservation yet, create one
                if event_code == 10 and not reservation_created:  # Booking
                    product_id = random.randint(1, 5)
                    product_price = round(random.uniform(150, 500), 2)
                    
                    # Determine nights
                    nights_probability = random.random()
                    if nights_probability < 0.60:
                        nights = 2
                    elif nights_probability < 0.70:
                        nights = 1
                    else:
                        nights = random.randint(3, 10)
                    
                    reservation = {
                        "event_id": page_view["event_id"],
                        "product_id": product_id,
                        "product_name": "",
                        "product_category": "Room Reservation",
                        "product_price": product_price,
                        "nights": nights,
                        "transaction_id": transaction_id,
                        "transaction_revenue": round(product_price * nights, 2)
                    }
                    
                    reservations.append(reservation)
                    transaction_id += 1
                    reservation_created = True
    
    # Calculate time on page
    for i in range(len(page_views) - 1):
        current_time = datetime.strptime(page_views[i]["timestamp"], "%Y-%m-%d %H:%M:%S")
        next_time = datetime.strptime(page_views[i+1]["timestamp"], "%Y-%m-%d %H:%M:%S")
        time_diff = (next_time - current_time).total_seconds()
        
        # Update engagement events with time on page
        for j, event in enumerate(engagement_events):
            if event["event_page_view_id"] == page_views[i]["event_id"]:
                engagement_events[j]["time_on_page"] = time_diff
    
    return page_views, engagement_events, reservations, event_id, transaction_id

def generate_data():
    print("Generating synthetic web analytics data...")
    
    # Generate visitors
    visitors = [f"Customer_{i}" for i in range(1, NUM_VISITORS + 1)]
    visit_counters = {visitor: 1 for visitor in visitors}
    
    # Initialize counters
    event_id_counter = 1
    transaction_id_counter = 1
    session_id_counter = 1
    
    # Initialize statistics
    total_page_views = 0
    total_engagement_events = 0
    total_reservations = 0
    
    # Set up file paths
    page_views_path = os.path.join(output_dir, "F_Page_View.parquet")
    engagement_events_path = os.path.join(output_dir, "F_Engagement_Events.parquet")
    reservations_path = os.path.join(output_dir, "F_events_reservations.parquet")
    
    # Process in batches
    for batch_start in tqdm(range(0, NUM_SESSIONS, BATCH_SIZE), desc="Processing session batches"):
        batch_end = min(batch_start + BATCH_SIZE, NUM_SESSIONS)
        
        # Collect data for this batch
        batch_page_views = []
        batch_engagement_events = []
        batch_reservations = []
        
        for session_num in range(batch_start + 1, batch_end + 1):
            page_views, engagement_events, reservations, new_event_id, new_transaction_id = generate_session_data(
                session_num, event_id_counter, transaction_id_counter, visitors, visit_counters
            )
            
            batch_page_views.extend(page_views)
            batch_engagement_events.extend(engagement_events)
            batch_reservations.extend(reservations)
            
            # Update counters
            event_id_counter = new_event_id
            transaction_id_counter = new_transaction_id
            session_id_counter += 1
        
        # Convert batch to dataframes
        df_page_views = pd.DataFrame(batch_page_views)
        df_engagement_events = pd.DataFrame(batch_engagement_events)
        df_reservations = pd.DataFrame(batch_reservations) if batch_reservations else pd.DataFrame(columns=[
            "event_id", "product_id", "product_name", "product_category", 
            "product_price", "nights", "transaction_id", "transaction_revenue"
        ])
        
        # Update statistics
        total_page_views += len(df_page_views)
        total_engagement_events += len(df_engagement_events)
        total_reservations += len(df_reservations)
        
        # Write batch to parquet files (append mode)
        if batch_start == 0:
            # First batch - create new files
            pq.write_table(pa.Table.from_pandas(df_page_views), page_views_path)
            
            if not df_engagement_events.empty:
                pq.write_table(pa.Table.from_pandas(df_engagement_events), engagement_events_path)
            
            if not df_reservations.empty:
                pq.write_table(pa.Table.from_pandas(df_reservations), reservations_path)
        else:
            # Append to existing files
            table = pq.read_table(page_views_path)
            table = pa.concat_tables([table, pa.Table.from_pandas(df_page_views)])
            pq.write_table(table, page_views_path)
            
            if not df_engagement_events.empty:
                if os.path.exists(engagement_events_path):
                    table = pq.read_table(engagement_events_path)
                    table = pa.concat_tables([table, pa.Table.from_pandas(df_engagement_events)])
                    pq.write_table(table, engagement_events_path)
                else:
                    pq.write_table(pa.Table.from_pandas(df_engagement_events), engagement_events_path)
            
            if not df_reservations.empty:
                if os.path.exists(reservations_path):
                    table = pq.read_table(reservations_path)
                    table = pa.concat_tables([table, pa.Table.from_pandas(df_reservations)])
                    pq.write_table(table, reservations_path)
                else:
                    pq.write_table(pa.Table.from_pandas(df_reservations), reservations_path)
        
        # Clear memory
        del df_page_views, df_engagement_events, df_reservations
        del batch_page_views, batch_engagement_events, batch_reservations
    
    print(f"Data generation complete. Files saved to {output_dir}")
    
    # Calculate some final statistics
    try:
        visitors = pq.read_table(page_views_path).column('visitor_id').to_pandas().nunique()
        sessions = pq.read_table(page_views_path).column('session_id').to_pandas().nunique()
        conversion_rate = total_reservations / sessions if sessions > 0 else 0
    except:
        visitors = "Unknown"
        sessions = "Unknown"
        conversion_rate = "Unknown"
    
    stats = {
        "page_views": total_page_views,
        "engagement_events": total_engagement_events,
        "reservations": total_reservations,
        "unique_visitors": visitors,
        "unique_sessions": sessions,
        "conversion_rate": conversion_rate
    }
    
    return stats

# Run data generation
if __name__ == "__main__":
    stats = generate_data()
    print("\nData Generation Summary:")
    for key, value in stats.items():
        if key == "conversion_rate" and isinstance(value, float):
            print(f"{key}: {value:.2%}")
        else:
            print(f"{key}: {value}")