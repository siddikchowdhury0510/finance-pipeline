import os
import json
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

def load_stocks_to_bigquery():
    """Read stock JSON files from GCS and load to BigQuery"""
    
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = 'finance_raw'
    table_id = 'raw_stocks'
    
    if not bucket_name or not project_id:
        raise ValueError("Missing GCP_BUCKET_NAME or GCP_PROJECT_ID in .env")
    
    # Initialise clients
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Define BigQuery schema
    schema = [
        bigquery.SchemaField('ticker', 'STRING'),
        bigquery.SchemaField('date', 'TIMESTAMP'),
        bigquery.SchemaField('open', 'FLOAT'),
        bigquery.SchemaField('high', 'FLOAT'),
        bigquery.SchemaField('low', 'FLOAT'),
        bigquery.SchemaField('close', 'FLOAT'),
        bigquery.SchemaField('volume', 'INTEGER'),
        bigquery.SchemaField('adj_close', 'FLOAT'),
        bigquery.SchemaField('adj_open', 'FLOAT'),
        bigquery.SchemaField('adj_high', 'FLOAT'),
        bigquery.SchemaField('adj_low', 'FLOAT'),
        bigquery.SchemaField('div_cash', 'FLOAT'),
        bigquery.SchemaField('split_factor', 'FLOAT'),
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
    
    # List all stock files in GCS
    blobs = bucket.list_blobs(prefix='tiingo/stocks/')
    
    rows_to_insert = []
    ingested_at = datetime.utcnow().isoformat()
    
    for blob in blobs:
        # Extract ticker from filename e.g. AAPL_2026-03-18_15-54-24.json
        filename = blob.name.split('/')[-1]
        ticker = filename.split('_')[0]
        
        # Download and parse JSON
        content = blob.download_as_text()
        data = json.loads(content)
        
        for record in data:
            rows_to_insert.append({
                'ticker': ticker,
                'date': record.get('date'),
                'open': record.get('open'),
                'high': record.get('high'),
                'low': record.get('low'),
                'close': record.get('close'),
                'volume': record.get('volume'),
                'adj_close': record.get('adjClose'),
                'adj_open': record.get('adjOpen'),
                'adj_high': record.get('adjHigh'),
                'adj_low': record.get('adjLow'),
                'div_cash': record.get('divCash'),
                'split_factor': record.get('splitFactor'),
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
    load_stocks_to_bigquery()
