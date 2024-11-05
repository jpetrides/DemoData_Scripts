import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sentiment_data(start_date, geo_ids, account_ids=None, table_type='google'):
    data = []
    current_date = datetime.now()
    
    # Generate a date range for the last 25 months
    dates = pd.date_range(start=start_date, end=current_date, freq='M')
    
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
            # Add some consistency to the sentiment (if an area had negative sentiment,
            # more likely to continue having negative sentiment)
            if random.random() < 0.7 and len(data) > 0:
                # Try to maintain previous sentiment for this ID 70% of the time
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
    
    # Generate list of Account_IDs (assuming 100 accounts as per previous code)
    account_ids = [f"ACCT_{i:02d}" for i in range(1, 101)]
    
    # Generate Google News Sentiment
    google_news_df = generate_sentiment_data(start_date, geo_ids, table_type='google')
    google_news_df.to_csv('Google_News_Sentiment.csv', index=False)
    
    # Generate Sprinklr Sentiment
    sprinklr_df = generate_sentiment_data(start_date, geo_ids, table_type='sprinklr')
    sprinklr_df.to_csv('Sprinklr_Sentiment.csv', index=False)
    
    # Generate Gong Sentiment
    gong_df = generate_sentiment_data(start_date, account_ids, account_ids, table_type='gong')
    gong_df.to_csv('Gong_Sentiment.csv', index=False)
    
    # Generate Municipal Minutes Sentiment
    municipal_df = generate_sentiment_data(start_date, geo_ids, table_type='municipal')
    municipal_df.to_csv('Municipal_Minutes_Sentiment.csv', index=False)
    
    return {
        'google_news': google_news_df,
        'sprinklr': sprinklr_df,
        'gong': gong_df,
        'municipal': municipal_df
    }

# Generate all sentiment tables
sentiment_tables = generate_all_sentiment_tables()