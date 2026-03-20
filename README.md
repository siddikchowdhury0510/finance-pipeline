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

## Personal Use Cases
- Track personal stock watchlist performance
- Monitor macro trends affecting savings and mortgage
- Compare crypto vs equity performance
- Visualise inflation impact on purchasing power

## Project Structure
```
finance-pipeline/
├── ingestion/
│   ├── tiingo_stocks.py    # Stock price ingestion
│   ├── tiingo_crypto.py    # Crypto price ingestion
│   ├── fred_macro.py       # Macro indicators ingestion
│   └── run_all.py          # Run all ingestion scripts
├── loading/                # GCS → BigQuery loaders (Phase 3)
├── dbt/                    # Transformation models (Phase 4)
├── airflow/                # DAGs (Phase 5)
├── tests/                  # Unit tests
├── .env.example            # Environment variable template
├── requirements.txt        # Python dependencies
└── README.md
```

## Data Sources
| Source | Data | Frequency |
|--------|------|-----------|
| Tiingo | Stock prices (AAPL, GOOGL, MSFT, TSLA, META) | Daily |
| Tiingo | Crypto prices (BTC, ETH, SOL, XRP) | Daily |
| FRED | Inflation (CPI) | Monthly |
| FRED | Interest rates | Monthly |
| FRED | GDP | Quarterly |
| FRED | Unemployment | Monthly |

## Project Phases
- [x] Phase 1 — GCP setup & repo structure
- [x] Phase 2 — Tiingo & FRED ingestion scripts + tests
- [x] Phase 3 — GCS to BigQuery loading + tests
- [ ] Phase 4 — dbt transformations + authorised views + tests
- [ ] Phase 5 — Airflow orchestration + tests
- [ ] Phase 6 — Looker Studio dashboard
- [ ] Phase 7 — Docker & Cloud Run deployment

## Git Workflow
** Every session star:**
1. 'cd/Users/Siddik/finance-pipeline' - navigate to project
2. 'source/venv/bin/activate' - activate virtual enviroment
3. 'git pull origin main' get latest changes

**Every session end:**
Always follow this order to avoid merge conflicts:
1. Cmd + S - save change
2. `git add .`- stage all changes
3. `git commit -m "your message"` - save a snapshot with commentary
4. `git push origin main` - upload to GitHub

**Golden Rules:**
- Always 'git pull' before making any local changes
- Never edit files directly on GitHub - always edit locally in VS Code and Push. Editing in both places causes diverged branches and merge conflicts
- Always 'cd /Users/siddik/finance-pipeline' at the start of every session
- Always 'source venv/bin/activate' at the start of every session
- Use 'git revert' not 'git reset' on shared branches to avoid rewriting history
- Use 'git log --oneline -5' to see recent commits
- Never commit '.env' - it contains your secrets

## Setup
1. Clone the repo
2. Create a virtual environment: `python3.11 -m venv venv`
3. Activate it: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and fill in your credentials
6. Authenticate with GCP: `gcloud auth application-default login`
7. Run ingestion: `cd ingestion && python run_all.py`


## Key Technical Decisions 

### Testing approach (all phases)
- **Tests built alongside each phase** — rather than treating 
testing as a separate phase, tests are written alongside each 
component. Ingestion tests validate API responses and GCS 
landing. Loading tests validate schema and row counts. dbt 
tests validate transformation logic. Airflow tests validate 
DAG structure. This mirrors professional DE team practices 
where testing is part of the definition of done, not an 
afterthought.

### Phase 1
- **Tiingo over Alpha Vantage** - Tiingo's free tier offers 500 requests/day vs Alpha Vantage's 25, making it practical for development and testing.
- **FRED API for macro data** - free, unlimited, and run by the Federal Reserve. Best source for inflation, interest rates.
- **Application Default Credentials over JSON keys** - more secure, industry best practise, avoids risk of credentials being comitted to GitHub.
- **GCS as immutable raw layer** - raw API responses are landed in GCS untouched befoere loading to BigQuery. This means data can always be reloaded or reprocessed from source if anything goes wrong downstream.
- **VS Code for editing, terminal for commands** - cleaner workflow, reduces risk of errors when writing file content.

### Phase 2
- **Tiingo for both stocks and crypto** - single API key, consistent JSON structure, generous free tier
- **FRED for macro indicators** - offiial Federal Reserve data, free and unlimited, covers inflation, GDP, interest
- **Timestamped filenames** - every file includes the run timestamp so historical runs are preserved and never overwritten. Critical for reprocessing and debugging
- **SSL fix for FRED on macOS** - macOS Python requires an explicit SSL context override for FRED API calls. Added 'ssl.create_unverified_context' as a workaround
- **run_all.py as single entry point** — rather than running three scripts separately, a single orchestration script imports and calls all three ingestion functions in sequence. This mirrors how Airflow will trigger the pipeline in Phase 5 and makes local testing simple with one command: `cd ingestion && python run_all.

### Phase 3
- **Deduplication handled in dbt, not loading layer** — the raw BigQuery tables intentionally preserve all records including duplicates from multiple pipeline runs. Deduplication happens in dbt staging models where the logic is version controlled, testable and rerunnable against the original raw data. This follows the principle of separation of concerns — each layer has one job. Raw layer preserves, staging layer cleans.
- **WRITE_TRUNCATE for raw BigQuery tables** — loading scripts use WRITE_TRUNCATE write disposition which truncates and replaces the table on every run. This prevents duplicate rows accumulating across multiple pipeline runs. GCS is the source of truth so BigQuery raw tables can always be safely reloaded. Incremental logic is handled in the dbt transformation layer.
- **Explicit schemas over auto-detection** — every BigQuery table has an explicitly defined schema with column names and data types. No auto- detection. This prevents silent type errors and makes the pipeline predictable and documented.

### Phase 4 (planned)
- **Authorised views for mart layer** — BigQuery authorised views will be implemented to restrict access to raw tables. Dashboard users and Looker Studio will only have access to mart models, not underlying raw or staging data. This mirrors enterprise data governance patterns and separates presentation layer access from raw data access.

## Learnings & Obstacles

### Phase 1
- **GCP Service Account Key Policy** - GCP's default org policy blocked JSON key creation on new accounts. Switched to Application Default Credentials via gcloud CLI - more secure and avoids credentials ever touching GitHub 
- **GitHub Password Authentication Deprecated** - git push via password no longer works. Resolved using a Personal Access token (PAT) instead 
- **macOS 12 Homebrew Compatibility** - Homebrew flagged macOS 12 as unsupoorted but worked correctly. No action required
- **Terminal vs VS Code for File Editing** - writing file content via terminal commands led to mangled output. Switched to VS Code for all file editing going forward

### Phase 2
- **ModuleNotFoundError for google.cloud** - venv was running Python 3.9 which had package conflicts. Rebuilt venv cleanly with Python 3.11 and reinstalled all dependencies
- **Application Default Credentials lost after venv rebuild** - rebuidling the venv doesn't affect system credentials but a new terminal session needed 'gcloud auth application- default login' to be re-run
- **SSL certificate error on macOS with FRED API** - macOS Python doesn't trust all certificates by default. Fixed with SSL context override in the script
- **Silent script failures** - early runs produced no output due to typos in variable names being caught silently by the except block. Lesson: alwasy test error handling explicitly
- **Verified raw JSON structure before moving to loading** — opened AAPL JSON in GCS to confirm data quality before building the loading layer. Fields include OHLC prices, adjusted prices, volume, date and split factor — everything needed for the dashboard
- **Git merge conflicts** — repeatedly hit merge conflicts on README.md caused by making changes both locally and on GitHub without pulling first. Resolved using `git checkout --theirs` to accept the remote version, then recommitting. Key lesson: always run `git pull origin main` before making any local changes to avoid diverged branches. This is standard practice in team environments.

### Phase 3
- **Duplicate rows from multiple ingestion runs** — running the ingestion script multiple times created duplicate rows in BigQuery raw tables. Deliberately left in the raw layer as this is expected behaviour. Deduplication will be handled in dbt staging models using ROW_NUMBER() or DISTINCT, keeping the raw layer as an immutable audit trail.
- **Understanding WRITE_TRUNCATE vs duplication** — initially confused about whether WRITE_TRUNCATE prevents duplicates in GCS or BigQuery. Clarification: GCS retains all historical files (intentional — immutable raw layer). WRITE_TRUNCATE wipes and reloads the BigQuery table on every run so duplicates never accumulate there. GCS is the filing cabinet that keeps everything. BigQuery raw is the whiteboard that gets erased and rewritten fresh every run.
- **Always cd to project root before running pytest** — pytest must be run from the finance-pipeline root directory otherwise it can't find the tests folder. Always run `cd /Users/siddik/finance-pipeline` first.
