import os
import json
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

def load_macro_to_bigquery():
    """Read FRED macro JSON files from GCS and load to BigQuery"""
    
    bucket_name = os.getenv('GCP_BUCKET_NAME')
    project_id = os.getenv('GCP_PROJECT_ID')
    dataset_id = 'finance_raw'
    table_id = 'raw_macro'
    
    if not bucket_name or not project_id:
        raise ValueError("Missing GCP_BUCKET_NAME or GCP_PROJECT_ID in .env")
    
    # Initialise clients
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Define BigQuery schema
    schema = [
        bigquery.SchemaField('indicator', 'STRING'),
        bigquery.SchemaField('date', 'TIMESTAMP'),
        bigquery.SchemaField('value', 'FLOAT'),
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
    
    # List all macro files in GCS
    blobs = bucket.list_blobs(prefix='fred/')
    
    rows_to_insert = []
    ingested_at = datetime.utcnow().isoformat()
    
    for blob in blobs:
        # Extract indicator name from filename
        # e.g. fred/inflation_2026-03-18_16-10-28.json -> inflation
        filename = blob.name.split('/')[-1]
        indicator = filename.split('_')[0]
        
        content = blob.download_as_text()
        data = json.loads(content)
        
        for record in data:
            rows_to_insert.append({
                'indicator': indicator,
                'date': record.get('date'),
                'value': record.get('value'),
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
    load_macro_to_bigquery()
