import os
import json
from datetime import datetime
from google.cloud import storage
import requests
from dotenv import load_dotenv

load_dotenv()

def fetch_tiingo_stocks():
    """Fetch stock data from Tiingo API and save to GCS"""
    
    api_key = os.getenv('TIINGO_API_KEY')
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    
    if not api_key or not bucket_name:
        raise ValueError("Missing TIINGO_API_KEY or GCP_BUCKET_NAME in .env")
    
    stocks = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'META']
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    for stock in stocks:
        try:
            url = f"https://api.tiingo.com/tiingo/daily/{stock}/prices"
            params = {'token': api_key}
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            filename = f"tiingo/stocks/{stock}_{timestamp}.json"
            blob = bucket.blob(filename)
            blob.upload_from_string(
                json.dumps(data),
                content_type='application/json'
            )
            print(f"✓ {stock} saved to gs://{bucket_name}/{filename}")
            
        except Exception as e:
            print(f"✗ Error fetching {stock}: {str(e)}")

if __name__ == '__main__':
    fetch_tiingo_stocks()