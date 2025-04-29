import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta
import os
from pathlib import Path
import pantab

# Set random seed for reproducibility
np.random.seed(42)

# Define constants
NUM_VISITORS = 1000
MIN_SESSIONS = 2
MAX_SESSIONS = 5
DAYS = 30
CONVERSION_RATES = {
    'view_to_product': 0.4,
    'product_to_cart': 0.2,
    'cart_to_purchase': 0.05
}
TRAFFIC_SOURCES = {
    'google': 0.30,
    'bing': 0.10,
    'direct': 0.25,
    'facebook': 0.10,
    'instagram': 0.05,
    'twitter': 0.02,
    'email': 0.08,
    'referral': 0.10
}
TRAFFIC_MEDIUMS = {
    'organic': 0.40,
    'direct': 0.25,
    'paid': 0.20,
    'social': 0.15
}
DEVICES = ['desktop', 'mobile', 'tablet']
DEVICE_WEIGHTS = [0.55, 0.35, 0.1]  # Default weights

# Create product catalog
PRODUCTS = [
    {'id': f'P{i}', 'name': f'Product {i}', 'category': random.choice(['Electronics', 'Clothing', 'Home', 'Beauty']), 
     'price': round(random.uniform(9.99, 199.99), 2)} 
    for i in range(1, 11)
]

# Define possible page types and their URL patterns
PAGE_TYPES = {
    'home': '/home',
    'category': ['/category/electronics', '/category/clothing', '/category/home', '/category/beauty'],
    'search': '/search',
    'product': [f'/product/{p["id"]}' for p in PRODUCTS],
    'cart': '/cart',
    'checkout': ['/checkout/shipping', '/checkout/payment', '/checkout/review'],
    'confirmation': '/order/confirmation',
    'account': ['/account/login', '/account/register', '/account/profile', '/account/orders']
}

# Define event types
EVENT_TYPES = ['page_view', 'product_view', 'add_to_cart', 'begin_checkout', 'purchase', 'account_create', 'login', 'search']

# Define common user paths with variation
PATH_TEMPLATES = [
    # Direct purchase
    ['home', 'product', 'cart', 'checkout', 'confirmation'],
    # Category browse then purchase
    ['home', 'category', 'product', 'cart', 'checkout', 'confirmation'],
    # Browse multiple products
    ['home', 'category', 'product', 'product', 'product', 'cart', 'checkout', 'confirmation'],
    # Cart abandonment
    ['home', 'product', 'cart'],
    # Browse only
    ['home', 'category', 'product', 'product'],
    # Search then purchase
    ['home', 'search', 'product', 'cart', 'checkout', 'confirmation'],
    # Account activity
    ['home', 'account', 'home', 'product', 'cart', 'checkout', 'confirmation'],
    # Multi-step checkout abandonment
    ['home', 'product', 'cart', 'checkout'],
]

# Generate end date (today) and start date
end_date = datetime.now()
start_date = end_date - timedelta(days=DAYS)

def generate_traffic_source():
    """Generate a random traffic source and medium based on defined distributions"""
    source = random.choices(list(TRAFFIC_SOURCES.keys()), 
                           weights=list(TRAFFIC_SOURCES.values()), 
                           k=1)[0]
    
    # Assign appropriate medium based on source
    if source == 'google' or source == 'bing':
        if random.random() < 0.7:  # 70% organic, 30% paid for search engines
            medium = 'organic'
        else:
            medium = 'paid'
    elif source == 'direct':
        medium = 'direct'
    elif source in ['facebook', 'instagram', 'twitter']:
        medium = 'social'
    elif source == 'email':
        medium = 'email'
    else:
        medium = 'referral'
        
    return source, medium

def get_landing_page():
    """Generate a landing page URL based on traffic source"""
    landing_options = [PAGE_TYPES['home']] * 5 + random.sample(PAGE_TYPES['category'], 1) + [PAGE_TYPES['search']]
    return random.choice(landing_options)

def get_product():
    """Randomly select a product from the catalog"""
    return random.choice(PRODUCTS)

def generate_path(visitor_id, session_id, traffic_source, traffic_medium, device, start_timestamp):
    """Generate a user path for a single session"""
    
    # Choose a path template with some randomness
    template = random.choice(PATH_TEMPLATES)
    
    # Determine if this visitor will convert in this session
    # Higher chance of conversion for returning visitors and certain traffic sources
    is_returning = int(session_id.split('-')[1]) > 1
    conversion_boost = 0.1 if is_returning else 0
    conversion_boost += 0.05 if traffic_medium == 'paid' else 0
    
    will_view_product = random.random() < (CONVERSION_RATES['view_to_product'] + conversion_boost)
    will_add_to_cart = will_view_product and random.random() < (CONVERSION_RATES['product_to_cart'] + conversion_boost)
    will_purchase = will_add_to_cart and random.random() < (CONVERSION_RATES['cart_to_purchase'] + conversion_boost)
    
    # Adjust template based on conversion decisions
    if not will_view_product:
        template = [t for t in template if t != 'product' and t != 'cart' and t != 'checkout' and t != 'confirmation']
    elif not will_add_to_cart:
        template = [t for t in template if t != 'cart' and t != 'checkout' and t != 'confirmation']
    elif not will_purchase:
        template = [t for t in template if t != 'confirmation']
    
    # If template is empty after filtering, use a basic template
    if not template:
        template = ['home']
    
    # Initialize
    events = []
    timestamp = start_timestamp
    landing_page = get_landing_page()
    selected_product = get_product()
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}" if will_purchase else None
    revenue = selected_product['price'] if will_purchase else None
    
    # Generate events for each step in the path
    for i, page_type in enumerate(template):
        # Determine the event type
        if page_type == 'product':
            event_type = 'product_view'
        elif page_type == 'cart' and will_add_to_cart:
            event_type = 'add_to_cart'
        elif page_type == 'checkout' and i < len(template) - 1 and template[i+1] == 'confirmation':
            event_type = 'begin_checkout'
        elif page_type == 'confirmation':
            event_type = 'purchase'
        elif page_type == 'account' and random.random() < 0.3:
            event_type = random.choice(['account_create', 'login'])
        elif page_type == 'search':
            event_type = 'search'
        else:
            event_type = 'page_view'
        
        # Determine the specific page URL
        if page_type == 'product':
            page_url = f"/product/{selected_product['id']}"
        elif page_type in ['category', 'account', 'checkout'] and isinstance(PAGE_TYPES[page_type], list):
            page_url = random.choice(PAGE_TYPES[page_type])
        else:
            page_url = PAGE_TYPES[page_type] if isinstance(PAGE_TYPES[page_type], str) else PAGE_TYPES[page_type][0]
        
        # Create the event
        event = {
            'visitor_id': visitor_id,
            'session_id': session_id,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'traffic_source': traffic_source,
            'traffic_medium': traffic_medium,
            'landing_page': landing_page,
            'event_type': event_type,
            'page_path': page_url,
            'product_id': selected_product['id'] if page_type in ['product', 'cart', 'confirmation'] else None,
            'product_name': selected_product['name'] if page_type in ['product', 'cart', 'confirmation'] else None,
            'product_category': selected_product['category'] if page_type in ['product', 'cart', 'confirmation'] else None,
            'product_price': selected_product['price'] if page_type in ['product', 'cart', 'confirmation'] else None,
            'device_type': device,
            'order_id': order_id if event_type == 'purchase' else None,
            'revenue': revenue if event_type == 'purchase' else None
        }
        
        events.append(event)
        
        # Increment timestamp by a random amount (1 to 5 minutes)
        timestamp += timedelta(seconds=random.randint(30, 300))
    
    return events

def generate_visitor_data(visitor_id):
    """Generate all sessions and events for a visitor"""
    visitor_events = []
    
    # Determine number of sessions for this visitor
    num_sessions = random.randint(MIN_SESSIONS, MAX_SESSIONS)
    
    # Generate preferred device for this visitor with some randomness
    preferred_device = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
    
    # Generate each session
    for session_num in range(1, num_sessions + 1):
        # Generate session ID
        session_id = f"{visitor_id}-{session_num}"
        
        # Mostly use preferred device but occasionally switch
        if random.random() < 0.8:  # 80% chance to use preferred device
            device = preferred_device
        else:
            # Choose from other devices
            other_devices = [d for d in DEVICES if d != preferred_device]
            device = random.choice(other_devices)
        
        # Generate traffic source (returning visitors more likely to be direct)
        if session_num > 1 and random.random() < 0.4:  # 40% chance for returning visitors to come direct
            traffic_source = 'direct'
            traffic_medium = 'direct'
        else:
            traffic_source, traffic_medium = generate_traffic_source()
        
        # Generate random timestamp for session start
        days_ago = random.randint(0, DAYS - 1)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        session_start = end_date - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        
        # Generate session events
        session_events = generate_path(
            visitor_id, 
            session_id, 
            traffic_source, 
            traffic_medium, 
            device, 
            session_start
        )
        
        visitor_events.extend(session_events)
    
    return visitor_events

def generate_dataset():
    """Generate the complete dataset"""
    all_events = []
    
    # Generate visitor IDs
    visitor_ids = [f"V{uuid.uuid4().hex[:8]}" for _ in range(NUM_VISITORS)]
    
    # Generate data for each visitor
    for visitor_id in visitor_ids:
        visitor_events = generate_visitor_data(visitor_id)
        all_events.extend(visitor_events)
    
    # Convert to DataFrame and sort by timestamp
    df = pd.DataFrame(all_events)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    return df

# Generate the dataset
print("Generating e-commerce journey dataset...")
ecommerce_df = generate_dataset()

# Create output directory if it doesn't exist
output_dir = Path(os.path.expanduser("~/Documents/Demo Data/CJA"))
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / "ecommerce_journey_data.hyper"

# Save to Hyper file using pantab
print(f"Saving dataset to {output_file}...")
pantab.frame_to_hyper(ecommerce_df, output_file, table="ecommerce_journey_data")

print(f"Dataset generated with {len(ecommerce_df)} events from {NUM_VISITORS} visitors.")
print(f"Data saved to: {output_file}")

# After creating the Hyper file
csv_output_file = output_dir / "ecommerce_journey_data.csv"
print(f"Saving dataset to {csv_output_file}...")
ecommerce_df.to_csv(csv_output_file, index=False)
print(f"CSV data saved to: {csv_output_file}")

# Display sample of the data
print("\nSample data:")
print(ecommerce_df.head())

# Display summary statistics
print("\nSummary statistics:")
print(f"Total sessions: {ecommerce_df['session_id'].nunique()}")
print(f"Total page views: {len(ecommerce_df[ecommerce_df['event_type'] == 'page_view'])}")
print(f"Total product views: {len(ecommerce_df[ecommerce_df['event_type'] == 'product_view'])}")
print(f"Total add to carts: {len(ecommerce_df[ecommerce_df['event_type'] == 'add_to_cart'])}")
print(f"Total purchases: {len(ecommerce_df[ecommerce_df['event_type'] == 'purchase'])}")
print(f"Total revenue: ${ecommerce_df['revenue'].sum():.2f}")