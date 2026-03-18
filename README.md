# finance-pipeline

## Overview
An end-to-end personal finance data pipeline built as a Data Engineering portfolio project. Pulls live market data from APIs, stores it in GCP, transforms it with dbt, and visualises it in Looker Studio.

## Architecture 
Tiingo/FRED APIs -> GCS (raw layer) -> BigQuery -> dbt -> Looker Studio

## Tech Stack
- **Ingestion:** Python, Tiingo API, FRED API
- **Storage:** GCP Cloud Storage (raw layer) 
- **Warehouse:** GCP BigQuery
- **Orchestration:** Airflow (Cloud Composer) 
- **Visualisation:** Looker Studio
- **Containerisation:** Docker (Phase 7)
- **Version Control:** Github

## Why I built this
Two reasons - portfolio and personal utility.

I wanted a project that mirrors my production stack at work (dbt, BigQuery, Airflow) while also giving me a live dashboard I actually use to track my finances.

## Project Phases
- [x] Phase 1 - GCP setup & repo structure
- [ ] Phase 2 - Tiingo & FRED ingestion scripts
- [ ] Phase 3 - GCS to BigQuery loading
- [ ] Phase 4 - dbt transformations
- [ ] Phase 5 - Airflow orchestration
- [ ] Phase 6 - Looker Studio dashboard
- [ ] Phase 7 - Docker & Cloud Run deployment

## Setup
1. Clone the repo
2. create a virtual enviroment: 'python3 -m venv venv'
3. Activate it: 'source venv/bin/activate'
4. Install dependencies: 'pip install -r requirements.txt'
5. Copy '.env.example' to 'env' and fill in your credentials
6. Authenticate with GCP: 'gcloud auth application-default login'

## Key Technical Decisions 

### Phase 1
- **Tiingo over Alpha Vantage** - Tiingo's free tier offers 500 requests/day vs Alpha Vantage's 25, making it practical for development and testing.
- **FRED API for macro data** - free, unlimited, and run by the Federal Reserve. Best source for inflation, interest rates.
- **Application Default Credentials over JSON keys** - more secure, industry best practise, avoids risk of credentials being comitted to GitHub.
- **GCS as immutable raw layer** - raw API responses are landed in GCS untouched befoere loading to BigQuery. This means data can always be reloaded or reprocessed from source if anything goes wrong downstream.
- **VS Code for editing, terminal for commands** - cleaner workflow, reduces risk of errors when writing file content.

## Learnings & Obstacles

### Phase 1
- **GCP Service Account Key Policy** - GCP's default org policy blocked JSON key creation on new accounts. Switched to Application Default Credentials via gcloud CLI - more secure and avoids credentials ever touching GitHub 
- **GitHub Password Authentication Deprecated** - git push via password no longer works. Resolved using a Personal Access token (PAT) instead 
- **macOS 12 Homebrew Compatibility** - Homebrew flagged macOS 12 as unsupoorted but worked correctly. No action required
- **Terminal vs VS Code for File Editing** - writing file content via terminal commands led to mangled output. Switched to VS Code for all file editing going forward



