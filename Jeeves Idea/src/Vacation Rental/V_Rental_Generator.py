import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

# Define the base path
BASE_PATH = "/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Vacation_Rental"

def generate_sentiment_data(start_date, geo_ids, account_ids=None, table_type='google'):
    data = []
    current_date = datetime.now()
    
    # Generate a date range for the last 25 months with updated frequency parameter
    dates = pd.date_range(start=start_date, end=current_date, freq='ME')
    
    # Define sentiment options based on table type
    if table_type == 'municipal':
        sentiments = ['Good', 'Neutral', 'Negative', None]
        weights = [0.3, 0.3, 0.2, 0.2]  # Including probability for Null values
    else:
        sentiments = ['Good', 'Neutral', 'Negative']
        weights = [0.4, 0.4, 0.2]
    
    # Generate data
    for date in dates:
        if table_type == 'gong':
            ids = account_ids
            id_column = 'Account_ID'
        else:
            ids = geo_ids
            id_column = 'Geo_ID'
            
        for id_value in ids:
            # Add some consistency to the sentiment
            if random.random() < 0.7 and len(data) > 0:
                previous_records = [r for r in data if r[id_column] == id_value]
                if previous_records:
                    sentiment = previous_records[-1]['Sentiment']
                else:
                    sentiment = np.random.choice(sentiments, p=weights)
            else:
                sentiment = np.random.choice(sentiments, p=weights)
            
            row = {
                'Month': date.strftime('%Y-%m'),
                id_column: id_value,
                'Sentiment': sentiment
            }
            data.append(row)
    
    return pd.DataFrame(data)

def generate_all_sentiment_tables():
    # Set start date to 25 months ago
    start_date = datetime.now() - timedelta(days=25*30)
    
    # Generate list of GEO_IDs
    geo_ids = [f"GEO_{i}" for i in range(1, 133)]
    
    # Generate list of Account_IDs
    account_ids = [f"ACCT_{i:02d}" for i in range(1, 101)]
    
    # Generate Google News Sentiment
    google_news_df = generate_sentiment_data(start_date, geo_ids, table_type='google')
    google_news_df.to_csv(os.path.join(BASE_PATH, 'F_Google_News_Sentiment.csv'), index=False)
    
    # Generate Sprinklr Sentiment
    sprinklr_df = generate_sentiment_data(start_date, geo_ids, table_type='sprinklr')
    sprinklr_df.to_csv(os.path.join(BASE_PATH, 'F_Sprinklr_Sentiment.csv'), index=False)
    
    # Generate Gong Sentiment
    gong_df = generate_sentiment_data(start_date, account_ids, account_ids, table_type='gong')
    gong_df.to_csv(os.path.join(BASE_PATH, 'F_Gong_Sentiment.csv'), index=False)
    
    # Generate Municipal Minutes Sentiment
    municipal_df = generate_sentiment_data(start_date, geo_ids, table_type='municipal')
    municipal_df.to_csv(os.path.join(BASE_PATH, 'F_Municipal_Minutes_Sentiment.csv'), index=False)
    
    return {
        'google_news': google_news_df,
        'sprinklr': sprinklr_df,
        'gong': gong_df,
        'municipal': municipal_df
    }

# Ensure the directory exists
os.makedirs(BASE_PATH, exist_ok=True)

# Generate all sentiment tables
sentiment_tables = generate_all_sentiment_tables()