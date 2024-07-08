import duckdb
import pandas as pd
from datetime import datetime, timedelta

# Connect to your DuckDB file
duckdb_file = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/hotel_reservations.duckdb'
con = duckdb.connect(duckdb_file)

# Generate date range
start_date = datetime(2020, 1, 1)
end_date = datetime(2030, 12, 31)
date_range = pd.date_range(start=start_date, end=end_date)

# Create DataFrame
df = pd.DataFrame({
    'date': date_range,
    'year': date_range.year,
    'month': date_range.month,
    'day': date_range.day,
    'day_of_week': date_range.dayofweek,
    'day_name': date_range.day_name(),
    'week_of_year': date_range.isocalendar().week,
    'quarter': date_range.quarter,
    'is_weekend': date_range.dayofweek.isin([5, 6]),
    'is_leap_year': date_range.is_leap_year,
    'days_in_month': date_range.days_in_month,
    'year_half': (date_range.quarter + 1) // 2
})

# Add fiscal year (assuming it starts in October)
df['fiscal_year'] = df['year'].where(df['month'] < 10, df['year'] + 1)

# Add month name
df['month_name'] = df['date'].dt.strftime('%B')

# Create the table in DuckDB
con.execute("DROP TABLE IF EXISTS calendar_reference")
con.execute("""
CREATE TABLE calendar_reference AS
SELECT
    date,
    year,
    month,
    day,
    day_of_week,
    day_name,
    week_of_year,
    quarter,
    is_weekend,
    is_leap_year,
    days_in_month,
    year_half,
    fiscal_year,
    month_name
FROM df
""")

# Commit changes and close the connection
con.commit()
con.close()

print("Calendar reference table has been created in your DuckDB file.")