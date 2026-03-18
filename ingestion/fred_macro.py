import os
import json
from datetime import datetime
from google.cloud import storage
from fredapi import Fred
from dotenv import load_dotenv

#added to get around SSL certificate error on macOS
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

load_dotenv()

def fetch_fred_macro():
    """Fetch macro economic data from FRED API and save to GCS"""
    
    fred_api_key = os.getenv('FRED_API_KEY')
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    
    if not fred_api_key or not bucket_name:
        raise ValueError("Missing FRED_API_KEY or GCP_BUCKET_NAME in .env")
    
    # Macro indicators to track
    indicators = {
        'CPIAUCSL': 'inflation',
        'FEDFUNDS': 'interest_rates',
        'GDP': 'gdp',
        'UNRATE': 'unemployment'
    }
    
    fred = Fred(api_key=fred_api_key)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    for series_id, name in indicators.items():
        try:
            data = fred.get_series(series_id)
            
            # Convert to JSON serialisable format
            data_json = data.reset_index().rename(
                columns={'index': 'date', 0: 'value'}
            ).to_json(orient='records', date_format='iso')
            
            filename = f"fred/{name}_{timestamp}.json"
            blob = bucket.blob(filename)
            blob.upload_from_string(
                data_json,
                content_type='application/json'
            )
            print(f"✓ {name} ({series_id}) saved to gs://{bucket_name}/{filename}")
            
        except Exception as e:
            print(f"✗ Error fetching {name}: {str(e)}")

if __name__ == '__main__':
    fetch_fred_macro()