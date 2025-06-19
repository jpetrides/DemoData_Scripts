import duckdb
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import random

# Load environment variables
load_dotenv()

# Get database path from .env file
DB_PATH = os.getenv('duckdb_path')

# Create connection
conn = duckdb.connect(DB_PATH)

# Use main schema
conn.execute("USE main")

# Drop existing tables if they exist
conn.execute("""
    DROP TABLE IF EXISTS TTH_F_Sourcing_order_items;
    DROP TABLE IF EXISTS TTH_F_Sourcing_orders;
    DROP TABLE IF EXISTS TTH_F_Sourcing_pricing;
    DROP TABLE IF EXISTS TTH_D_Sourcing_products;
    DROP TABLE IF EXISTS TTH_D_Sourcing_categories;
    DROP TABLE IF EXISTS TTH_D_Sourcing_vendors;
    DROP TABLE IF EXISTS TTH_D_Sourcing_brand_tiers;
""")

# Create dimension tables
conn.execute("""
    CREATE TABLE TTH_D_Sourcing_brand_tiers (
        tier_id INTEGER PRIMARY KEY,
        tier_name VARCHAR,
        tier_level INTEGER,
        description VARCHAR
    )
""")

conn.execute("""
    CREATE TABLE TTH_D_Sourcing_vendors (
        vendor_id INTEGER PRIMARY KEY,
        vendor_name VARCHAR,
        preferred_status BOOLEAN,
        contract_number VARCHAR,
        contact_email VARCHAR,
        contact_phone VARCHAR
    )
""")

conn.execute("""
    CREATE TABLE TTH_D_Sourcing_categories (
        category_id INTEGER PRIMARY KEY,
        category_name VARCHAR,
        parent_category_id INTEGER,
        sort_order INTEGER
    )
""")

conn.execute("""
    CREATE TABLE TTH_D_Sourcing_products (
        product_id INTEGER PRIMARY KEY,
        sku VARCHAR UNIQUE,
        product_name VARCHAR,
        category_id INTEGER REFERENCES TTH_D_Sourcing_categories(category_id),
        vendor_id INTEGER REFERENCES TTH_D_Sourcing_vendors(vendor_id),
        brand_tier_id INTEGER REFERENCES TTH_D_Sourcing_brand_tiers(tier_id),
        description TEXT,
        specifications JSON,
        min_order_quantity INTEGER,
        lead_time_days INTEGER,
        warranty_months INTEGER,
        compliance_status VARCHAR,
        unit_of_measure VARCHAR,
        weight_lbs DECIMAL(10,2),
        created_date DATE
    )
""")

# Create fact tables
conn.execute("""
    CREATE TABLE TTH_F_Sourcing_pricing (
        price_id INTEGER PRIMARY KEY,
        product_id INTEGER REFERENCES TTH_D_Sourcing_products(product_id),
        tier_level INTEGER,
        min_quantity INTEGER,
        max_quantity INTEGER,
        unit_price DECIMAL(10,2),
        effective_date DATE,
        expiration_date DATE
    )
""")

conn.execute("""
    CREATE TABLE TTH_F_Sourcing_orders (
        order_id INTEGER PRIMARY KEY,
        property_id VARCHAR,
        property_name VARCHAR,
        order_date DATE,
        total_amount DECIMAL(10,2),
        status VARCHAR
    )
""")

conn.execute("""
    CREATE TABLE TTH_F_Sourcing_order_items (
        order_item_id INTEGER PRIMARY KEY,
        order_id INTEGER REFERENCES TTH_F_Sourcing_orders(order_id),
        product_id INTEGER REFERENCES TTH_D_Sourcing_products(product_id),
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        line_total DECIMAL(10,2)
    )
""")

# Insert sample data

# Brand Tiers
brand_tiers = [
    (1, 'Microtel/Travelodge', 1, 'Economy brands'),
    (2, 'Days Inn/Super 8', 2, 'Economy Plus brands'),
    (3, 'Ramada/Baymont', 3, 'Midscale brands'),
    (4, 'Wyndham/Garden', 4, 'Upper Midscale brands'),
    (5, 'Dolce/Registry', 5, 'Upscale brands')
]

for tier in brand_tiers:
    conn.execute("INSERT INTO TTH_D_Sourcing_brand_tiers VALUES (?, ?, ?, ?)", tier)

# Vendors
vendors = [
    (1, 'Standard Textile Co', True, 'WYN-2024-001', 'sales@standardtextile.com', '800-999-0400'),
    (2, 'Serta Simmons Bedding', True, 'WYN-2024-002', 'hospitality@serta.com', '800-999-0401'),
    (3, 'American Hotel Register', True, 'WYN-2024-003', 'wyndham@ahrco.com', '800-999-0402'),
    (4, 'HD Supply', False, 'WYN-2024-004', 'hotels@hdsupply.com', '800-999-0403'),
    (5, 'Guest Supply', True, 'WYN-2024-005', 'orders@guestsupply.com', '800-999-0404')
]

for vendor in vendors:
    conn.execute("INSERT INTO TTH_D_Sourcing_vendors VALUES (?, ?, ?, ?, ?, ?)", vendor)

# Categories
categories = [
    (1, 'Bedding & Linens', None, 1),
    (2, 'Mattresses', None, 2),
    (3, 'Furniture', None, 3),
    (4, 'Electronics', None, 4),
    (5, 'Bath Amenities', None, 5),
    (6, 'Window Treatments', None, 6),
    (11, 'Bed Linens', 1, 1),
    (12, 'Bath Linens', 1, 2),
    (13, 'Pillows & Protectors', 1, 3),
    (31, 'Bedroom Furniture', 3, 1),
    (32, 'Desk & Seating', 3, 2)
]

for category in categories:
    conn.execute("INSERT INTO TTH_D_Sourcing_categories VALUES (?, ?, ?, ?)", category)

# Products
products = [
    # Bed Linens
    (1, 'STX-FS-K-300W', 'King Fitted Sheet - 300TC White', 11, 1, 2, 
     'Premium 60/40 cotton-poly blend fitted sheet', 
     '{"thread_count": 300, "material": "60/40 Cotton-Poly", "size": "78x80x15", "color": "White"}',
     12, 7, 12, 'Approved', 'Each', 1.2, '2024-01-15'),
    
    (2, 'STX-FS-Q-300W', 'Queen Fitted Sheet - 300TC White', 11, 1, 2,
     'Premium 60/40 cotton-poly blend fitted sheet',
     '{"thread_count": 300, "material": "60/40 Cotton-Poly", "size": "60x80x15", "color": "White"}',
     12, 7, 12, 'Approved', 'Each', 1.0, '2024-01-15'),
    
    (3, 'STX-DUV-K-WHT', 'King Duvet Cover - White', 11, 1, 3,
     'Durable duvet cover with button closure',
     '{"size": "110x96", "closure": "Buttons", "material": "Poly-Cotton", "color": "White"}',
     6, 10, 18, 'Approved', 'Each', 2.5, '2024-01-15'),
    
    # Bath Linens
    (4, 'GS-BTH-27x54-W', 'Bath Towel - 27x54 White', 12, 5, 2,
     'Premium ring-spun cotton bath towel',
     '{"size": "27x54", "weight": "10.5 lbs/dz", "material": "100% Cotton", "color": "White"}',
     24, 5, 12, 'Approved', 'Dozen', 15.0, '2024-01-15'),
    
    (5, 'GS-HND-16x30-W', 'Hand Towel - 16x30 White', 12, 5, 2,
     'Premium ring-spun cotton hand towel',
     '{"size": "16x30", "weight": "3.5 lbs/dz", "material": "100% Cotton", "color": "White"}',
     48, 5, 12, 'Approved', 'Dozen', 8.0, '2024-01-15'),
    
    # Pillows
    (6, 'STX-PLW-FIRM-K', 'King Pillow - Firm Support', 13, 1, 3,
     'Hypoallergenic polyester fill pillow',
     '{"size": "20x36", "fill": "Polyester", "firmness": "Firm", "hypoallergenic": true}',
     12, 7, 12, 'Approved', 'Each', 1.8, '2024-01-15'),
    
    # Mattresses
    (7, 'SRT-MAT-K-PLSH', 'King Plush Mattress - 12"', 2, 2, 3,
     'Premium hospitality plush mattress with 10-year warranty',
     '{"size": "76x80x12", "type": "Innerspring", "firmness": "Plush", "warranty_years": 10}',
     1, 21, 120, 'Approved', 'Each', 85.0, '2024-01-15'),
    
    (8, 'SRT-MAT-Q-FIRM', 'Queen Firm Mattress - 10"', 2, 2, 2,
     'Standard hospitality firm mattress with 10-year warranty',
     '{"size": "60x80x10", "type": "Innerspring", "firmness": "Firm", "warranty_years": 10}',
     1, 21, 120, 'Approved', 'Each', 70.0, '2024-01-15'),
    
    # Furniture
    (9, 'AHR-NST-USB-MAH', 'Nightstand with USB Ports - Mahogany', 31, 3, 3,
     'Modern nightstand with built-in USB charging',
     '{"dimensions": "24x18x24", "usb_ports": 2, "drawers": 2, "finish": "Mahogany Laminate"}',
     2, 30, 60, 'Approved', 'Each', 45.0, '2024-01-15'),
    
    (10, 'AHR-DSKCH-ERG', 'Ergonomic Desk Chair - Black', 32, 3, 3,
     'Adjustable ergonomic desk chair',
     '{"material": "Mesh Back", "adjustable_height": true, "armrests": true, "color": "Black"}',
     1, 14, 36, 'Approved', 'Each', 35.0, '2024-01-15'),
    
    # Electronics
    (11, 'HD-TV-50-SMART', '50" Smart LED TV', 4, 4, 3,
     'Commercial-grade smart TV with Pro:Idiom',
     '{"size": "50 inch", "resolution": "4K UHD", "smart": true, "pro_idiom": true}',
     1, 10, 36, 'Approved', 'Each', 125.0, '2024-01-15'),
    
    (12, 'HD-SAFE-DIGI', 'Digital In-Room Safe', 4, 4, 2,
     'Electronic safe with keycard override',
     '{"dimensions": "20x15x10", "lock_type": "Digital", "override": "Keycard", "mounting": "Floor/Wall"}',
     1, 7, 24, 'Approved', 'Each', 28.0, '2024-01-15')
]

for product in products:
    conn.execute("""
        INSERT INTO TTH_D_Sourcing_products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, product)

# Pricing (Volume-based tiers)
pricing_data = []
price_id = 1

# Define pricing tiers for each product
product_pricing = {
    1: [(12, 49, 18.50), (50, 199, 16.95), (200, 499, 15.50), (500, None, 13.95)],  # King Fitted Sheet
    2: [(12, 49, 17.50), (50, 199, 15.95), (200, 499, 14.50), (500, None, 12.95)],  # Queen Fitted Sheet
    3: [(6, 23, 32.50), (24, 99, 29.95), (100, 299, 27.50), (300, None, 24.95)],   # King Duvet Cover
    4: [(24, 47, 85.00), (48, 119, 78.00), (120, 239, 72.00), (240, None, 65.00)], # Bath Towels (per dz)
    5: [(48, 95, 42.00), (96, 239, 38.00), (240, 479, 35.00), (480, None, 31.00)], # Hand Towels (per dz)
    6: [(12, 49, 12.50), (50, 199, 10.95), (200, 499, 9.50), (500, None, 8.25)],   # King Pillow
    7: [(1, 9, 425.00), (10, 49, 395.00), (50, 99, 365.00), (100, None, 335.00)],  # King Mattress
    8: [(1, 9, 375.00), (10, 49, 345.00), (50, 99, 315.00), (100, None, 285.00)],  # Queen Mattress
    9: [(2, 9, 145.00), (10, 49, 132.00), (50, 99, 125.00), (100, None, 115.00)],  # Nightstand
    10: [(1, 9, 165.00), (10, 49, 152.00), (50, 99, 145.00), (100, None, 135.00)], # Desk Chair
    11: [(1, 9, 485.00), (10, 49, 445.00), (50, 99, 415.00), (100, None, 385.00)], # 50" TV
    12: [(1, 9, 125.00), (10, 49, 112.00), (50, 99, 105.00), (100, None, 95.00)]   # Safe
}

effective_date = datetime(2024, 6, 1)
expiration_date = datetime(2025, 5, 31)

for product_id, tiers in product_pricing.items():
    for tier_level, (min_qty, max_qty, price) in enumerate(tiers, 1):
        pricing_data.append((
            price_id,
            product_id,
            tier_level,
            min_qty,
            max_qty if max_qty else 9999,
            price,
            effective_date.strftime('%Y-%m-%d'),
            expiration_date.strftime('%Y-%m-%d')
        ))
        price_id += 1

for pricing in pricing_data:
    conn.execute("""
        INSERT INTO TTH_F_Sourcing_pricing VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, pricing)

# Generate 1000+ Orders
orders = []
order_id = 1
start_date = datetime(2024, 1, 1)

# Property list for random selection
properties = [
    ('WYN-1001', 'Wyndham Garden Downtown'),
    ('WYN-1002', 'Wyndham Hotel City Center'),
    ('WYN-1003', 'Wyndham Resort Beachfront'),
    ('RAM-2001', 'Ramada Airport'),
    ('RAM-2002', 'Ramada Convention Center'),
    ('RAM-2003', 'Ramada Plaza Downtown'),
    ('DI-3001', 'Days Inn Highway 101'),
    ('DI-3002', 'Days Inn Interstate 95'),
    ('DI-3003', 'Days Inn Airport Road'),
    ('SUP-4001', 'Super 8 Main Street'),
    ('SUP-4002', 'Super 8 Downtown'),
    ('SUP-4003', 'Super 8 Highway Exit'),
    ('BAY-5001', 'Baymont Inn & Suites North'),
    ('BAY-5002', 'Baymont Inn & Suites South'),
    ('BAY-5003', 'Baymont Inn & Suites Airport'),
    ('TRV-6001', 'Travelodge Express'),
    ('TRV-6002', 'Travelodge Central'),
    ('MCT-7001', 'Microtel Inn Central'),
    ('MCT-7002', 'Microtel Inn & Suites')
]

statuses = ['Delivered', 'Processing', 'Shipped', 'Pending']
status_weights = [0.6, 0.15, 0.15, 0.1]  # More delivered orders

# Generate 1500 orders over 6 months
for i in range(1500):
    property_id, property_name = random.choice(properties)
    order_date = start_date + timedelta(days=random.randint(0, 180))
    total_amount = round(random.uniform(500, 25000), 2)
    status = random.choices(statuses, weights=status_weights)[0]
    
    orders.append((
        order_id,
        property_id,
        property_name,
        order_date.strftime('%Y-%m-%d'),
        total_amount,
        status
    ))
    order_id += 1

for order in orders:
    conn.execute("INSERT INTO TTH_F_Sourcing_orders VALUES (?, ?, ?, ?, ?, ?)", order)

# Generate Order Items
order_items = []
order_item_id = 1

# Price lookup dictionary based on quantity tiers
def get_unit_price(product_id, quantity):
    tiers = product_pricing.get(product_id, [])
    for min_qty, max_qty, price in tiers:
        if min_qty <= quantity <= (max_qty or 9999):
            return price
    return tiers[-1][2] if tiers else 100.00  # Default to highest tier

# For each order, generate 1-10 line items
for order_id in range(1, 1501):
    num_items = random.randint(1, 8)
    used_products = set()
    
    for _ in range(num_items):
        # Pick a random product not already in this order
        product_id = random.randint(1, 12)
        attempts = 0
        while product_id in used_products and attempts < 10:
            product_id = random.randint(1, 12)
            attempts += 1
        
        if product_id not in used_products:
            used_products.add(product_id)
            
            # Determine quantity based on product type
            if product_id in [7, 8]:  # Mattresses
                quantity = random.choices([5, 10, 20, 30], weights=[0.4, 0.3, 0.2, 0.1])[0]
            elif product_id in [4, 5]:  # Towels (by dozen)
                quantity = random.choice([24, 48, 96, 120, 240])
            elif product_id in [1, 2, 3, 6]:  # Sheets and pillows
                quantity = random.choice([12, 24, 48, 96, 200])
            elif product_id in [9, 10]:  # Furniture
                quantity = random.choices([5, 10, 20, 50], weights=[0.4, 0.3, 0.2, 0.1])[0]
            else:  # Electronics
                quantity = random.choices([2, 5, 10, 20], weights=[0.4, 0.3, 0.2, 0.1])[0]
            
            # Get appropriate price based on quantity
            unit_price = get_unit_price(product_id, quantity)
            line_total = round(quantity * unit_price, 2)
            
            order_items.append((
                order_item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
                line_total
            ))
            order_item_id += 1

# Insert order items
print(f"Inserting {len(order_items)} order items...")
for item in order_items:
    conn.execute("INSERT INTO TTH_F_Sourcing_order_items VALUES (?, ?, ?, ?, ?, ?)", item)

# Create views for common queries
conn.execute("""
    CREATE VIEW product_pricing_current AS
    SELECT 
        p.product_id,
        p.sku,
        p.product_name,
        c.category_name,
        v.vendor_name,
        pr.min_quantity,
        pr.max_quantity,
        pr.unit_price,
        p.unit_of_measure
    FROM TTH_D_Sourcing_products p
    JOIN TTH_D_Sourcing_categories c ON p.category_id = c.category_id
    JOIN TTH_D_Sourcing_vendors v ON p.vendor_id = v.vendor_id
    JOIN TTH_F_Sourcing_pricing pr ON p.product_id = pr.product_id
    WHERE pr.effective_date <= CURRENT_DATE 
    AND pr.expiration_date >= CURRENT_DATE
    ORDER BY p.product_id, pr.min_quantity
""")

conn.execute("""
    CREATE VIEW order_summary AS
    SELECT 
        o.order_id,
        o.property_name,
        o.order_date,
        COUNT(DISTINCT oi.product_id) as item_count,
        SUM(oi.quantity) as total_units,
        o.total_amount,
        o.status
    FROM TTH_F_Sourcing_orders o
    JOIN TTH_F_Sourcing_order_items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.property_name, o.order_date, o.total_amount, o.status
""")

conn.execute("""
    CREATE VIEW monthly_sales AS
    SELECT 
        DATE_TRUNC('month', o.order_date) as month,
        COUNT(DISTINCT o.order_id) as order_count,
        COUNT(DISTINCT o.property_id) as unique_properties,
        SUM(o.total_amount) as total_sales,
        AVG(o.total_amount) as avg_order_value
    FROM TTH_F_Sourcing_orders o
    WHERE o.status = 'Delivered'
    GROUP BY DATE_TRUNC('month', o.order_date)
    ORDER BY month
""")

# Create indexes for better performance
conn.execute("CREATE INDEX idx_products_category ON TTH_D_Sourcing_products(category_id)")
conn.execute("CREATE INDEX idx_products_vendor ON TTH_D_Sourcing_products(vendor_id)")
conn.execute("CREATE INDEX idx_pricing_product ON TTH_F_Sourcing_pricing(product_id)")
conn.execute("CREATE INDEX idx_pricing_dates ON TTH_F_Sourcing_pricing(effective_date, expiration_date)")
conn.execute("CREATE INDEX idx_order_items_order ON TTH_F_Sourcing_order_items(order_id)")
conn.execute("CREATE INDEX idx_order_items_product ON TTH_F_Sourcing_order_items(product_id)")
conn.execute("CREATE INDEX idx_orders_date ON TTH_F_Sourcing_orders(order_date)")
conn.execute("CREATE INDEX idx_orders_property ON TTH_F_Sourcing_orders(property_id)")

# Commit and close
conn.commit()
conn.close()

print(f"Database created successfully at: {DB_PATH}")
print(f"\nGenerated:")
print(f"- {len(orders)} orders")
print(f"- {len(order_items)} order line items")
print("\nTable structure:")
print("Dimension Tables:")
print("- TTH_D_Sourcing_brand_tiers")
print("- TTH_D_Sourcing_vendors")
print("- TTH_D_Sourcing_categories")
print("- TTH_D_Sourcing_products")
print("\nFact Tables:")
print("- TTH_F_Sourcing_pricing")
print("- TTH_F_Sourcing_orders")
print("- TTH_F_Sourcing_order_items")
print("\nViews created:")
print("- product_pricing_current")
print("- order_summary")
print("- monthly_sales")
print("\nSample queries you can run:")
print("- SELECT * FROM product_pricing_current WHERE product_name LIKE '%Sheet%';")
print("- SELECT * FROM order_summary ORDER BY total_amount DESC LIMIT 10;")
print("- SELECT * FROM monthly_sales;")
print("- SELECT property_name, COUNT(*) as order_count, SUM(total_amount) as total_spent")
print("  FROM TTH_F_Sourcing_orders GROUP BY property_name ORDER BY total_spent DESC;")