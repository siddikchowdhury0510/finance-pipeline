import os
import json
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

def load_crypto_to_bigquery():
    """Read crypto JSON files from GCS and load to BigQuery"""
    
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = 'finance_raw'
    table_id = 'raw_crypto'
    
    if not bucket_name or not project_id:
        raise ValueError("Missing GCP_BUCKET_NAME or GCP_PROJECT_ID in .env")
    
    # Initialise clients
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Define BigQuery schema
    schema = [
        bigquery.SchemaField('ticker', 'STRING'),
        bigquery.SchemaField('base_currency', 'STRING'),
        bigquery.SchemaField('quote_currency', 'STRING'),
        bigquery.SchemaField('date', 'TIMESTAMP'),
        bigquery.SchemaField('open', 'FLOAT'),
        bigquery.SchemaField('high', 'FLOAT'),
        bigquery.SchemaField('low', 'FLOAT'),
        bigquery.SchemaField('close', 'FLOAT'),
        bigquery.SchemaField('volume', 'FLOAT'),
        bigquery.SchemaField('ingested_at', 'TIMESTAMP'),
    ]
    
    # Create table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Create table if it doesn't exist
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_ref} already exists")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"Created table {table_ref}")
    
    # List all crypto files in GCS
    blobs = bucket.list_blobs(prefix='tiingo/crypto/')
    
    rows_to_insert = []
    ingested_at = datetime.utcnow().isoformat()
    
    for blob in blobs:
        content = blob.download_as_text()
        data = json.loads(content)
        
        for record in data:
            price_data = record.get('priceData', [])
            for price in price_data:
                rows_to_insert.append({
                    'ticker': record.get('ticker'),
                    'base_currency': record.get('baseCurrency'),
                    'quote_currency': record.get('quoteCurrency'),
                    'date': price.get('date'),
                    'open': price.get('open'),
                    'high': price.get('high'),
                    'low': price.get('low'),
                    'close': price.get('close'),
                    'volume': price.get('volumeNotional'),
                    'ingested_at': ingested_at,
                })
    
    # Load to BigQuery with WRITE_TRUNCATE to avoid duplicates
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema
)
    
    job = bq_client.load_table_from_json(
        rows_to_insert,
        table_ref,
        job_config=job_config
)
    job.result()  # Wait for job to complete

    print(f"✓ {len(rows_to_insert)} rows loaded to {table_ref}")

if __name__ == '__main__':
    load_crypto_to_bigquery()

    

