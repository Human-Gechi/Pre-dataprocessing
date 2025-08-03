import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os


def generate_synthetic_data(rows=100, start_date='2023-01-01', null_ratio=0.1):
    """
    Generate synthetic time series data with null values

    Args:
        rows: Number of data points
        start_date: Starting date as string 'YYYY-MM-DD'
        null_ratio: Percentage of values that will be null (0.0-1.0)

    Returns:
        DataFrame with time series data
    """
    fake = Faker()
    
    # Create date range
    dates = pd.date_range(start=start_date, periods=rows, freq='D')
    
    # Generate random data
    stock_price = [round(fake.random.uniform(50, 200), 2) for _ in range(rows)]
    volume = [fake.random_int(min=1000, max=1000000) for _ in range(rows)]
    sentiment_score = [round(fake.random.uniform(-1, 1), 2) for _ in range(rows)]
    volatility = [round(fake.random.uniform(0.01, 0.2), 3) for _ in range(rows)]
    
    # Create DataFrame
    df = pd.DataFrame(
        {'date': dates, 'price': stock_price, 'volume': volume, 'sentiment': sentiment_score, 'volatility': volatility})
    
    # Set date as index
    df.set_index('date', inplace=True)
    
    # Insert null values randomly
    for col in df.columns:
        null_mask = np.random.random(size=len(df)) < null_ratio
        df.loc[null_mask, col] = np.nan
    
    return df


# Create data directory if it doesn't exist
if not os.path.exists('generated_data'):
    os.makedirs('generated_data')

# Generate synthetic data
df = generate_synthetic_data(rows=100, null_ratio=0.15)

# Save in different formats
df.to_csv('generated_data/synthetic_timeseries.csv')
df.to_excel('generated_data/synthetic_timeseries.xlsx')
df.to_json('generated_data/synthetic_timeseries.json')
df.to_parquet('generated_data/synthetic_timeseries.parquet')

