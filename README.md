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
- **Project Management:** Jira

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
├── airflow/
│   └── dags/
│       └── finance_pipeline_dag.py  # Airflow DAG
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── staging.yml
│   │   │   ├── stg_stocks.sql
│   │   │   ├── stg_crypto.sql
│   │   │   └── stg_macro.sql
│   │   ├── intermediate/
│   │   │   ├── intermediate.yml
│   │   │   └── int_market_daily.sql
│   │   └── marts/
│   │       ├── marts.yml
│   │       ├── mart_market_daily.sql
│   │       ├── mart_macro_indicators.sql
│   │       └── mart_macro_market_correlation.sql
├── ingestion/
│   ├── tiingo_stocks.py
│   ├── tiingo_crypto.py
│   ├── fred_macro.py
│   └── run_all.py
├── loading/
│   ├── load_stocks.py
│   ├── load_crypto.py
│   ├── load_macro.py
│   └── run_all.py
├── tests/
├── .env.example
├── requirements.txt
├── start_airflow.sh
└── README.md
```

## Data Sources

|Source|Data                                        |Frequency|
|------|--------------------------------------------|---------|
|Tiingo|Stock prices (AAPL, GOOGL, MSFT, TSLA, META)|Daily    |
|Tiingo|Crypto prices (BTC, ETH, SOL, XRP)          |Daily    |
|FRED  |Inflation (CPI)                             |Monthly  |
|FRED  |Interest rates                              |Monthly  |
|FRED  |GDP                                         |Quarterly|
|FRED  |Unemployment                                |Monthly  |

## Project Phases

- [x] Phase 1 — GCP setup & repo structure
- [x] Phase 2 — Tiingo & FRED ingestion scripts + tests
- [x] Phase 3 — GCS to BigQuery loading + tests
- [x] Phase 4 — dbt transformations + authorised views + tests
- [x] Phase 5 — Airflow orchestration + tests
- [ ] Phase 6 — Looker Studio dashboard
- [ ] Phase 7 — Docker & Cloud Run deployment

## Daily Startup

Every time you open your laptop and want to work on the project:

```bash
cd /Users/Siddik/finance-pipeline
source venv/bin/activate
bash start_airflow.sh
```

Then open http://localhost:8080 in your browser.

The start_airflow.sh script automatically opens 4 terminal tabs and starts all required Airflow processes. Keep a 5th tab free for your own commands.

## Git Workflow

**Every session start:**

1. `cd /Users/Siddik/finance-pipeline` — navigate to project
1. `source venv/bin/activate` — activate virtual environment
1. `git pull origin main` — get latest changes
1. `bash start_airflow.sh` — start all Airflow processes
1. Open http://localhost:8080

**Can’t find your project directory?**
Run `find ~ -name "finance-pipeline" -type d` to locate it

**Every session end:**

1. `Cmd + S` — save changes in VS Code
1. `git add .` — stage all changes
1. `git commit -m "your message"` — save a snapshot with commentary
1. `git push origin main` — upload to GitHub

**After editing the DAG:**
Always copy the updated file to Airflow’s watched folder:

```bash
cp /Users/Siddik/finance-pipeline/airflow/dags/finance_pipeline_dag.py /Users/siddik/airflow/dags/
```

In production (Cloud Composer), Git sync replaces this manual step.

**Golden Rules:**

- Always `git pull` before making any local changes
- Never edit files directly on GitHub — always edit locally in VS Code and push. Editing in both places causes diverged branches and merge conflicts
- Always `cd /Users/Siddik/finance-pipeline` at the start of every session
- Always `source venv/bin/activate` at the start of every session
- Use `git revert` not `git reset` on shared branches to avoid rewriting history
- Use `git log --oneline -5` to see recent commits
- Never commit `.env` — it contains your secrets

## Setup

1. Clone the repo
1. Create a virtual environment: `python3.11 -m venv venv`
1. Activate it: `source venv/bin/activate`
1. Install dependencies: `pip install -r requirements.txt`
1. Copy `.env.example` to `.env` and fill in your credentials
1. Authenticate with GCP: `gcloud auth application-default login`
1. Start Airflow: `bash start_airflow.sh`
1. Open http://localhost:8080

## Key Technical Decisions

### Testing approach (all phases)

- **Tests built alongside each phase** — rather than treating testing as a separate phase, tests are written alongside each component. Ingestion tests validate API responses and GCS landing. Loading tests validate schema and row counts. dbt tests validate transformation logic. Airflow tests validate DAG structure. This mirrors professional DE team practices where testing is part of the definition of done, not an afterthought.

### Phase 1

- **Tiingo over Alpha Vantage** — Tiingo’s free tier offers 500 requests/day vs Alpha Vantage’s 25, making it practical for development and testing.
- **FRED API for macro data** — free, unlimited, and run by the Federal Reserve. Best source for inflation, interest rates.
- **Application Default Credentials over JSON keys** — more secure, industry best practice, avoids risk of credentials being committed to GitHub.
- **GCS as immutable raw layer** — raw API responses are landed in GCS untouched before loading to BigQuery. This means data can always be reloaded or reprocessed from source if anything goes wrong downstream.
- **VS Code for editing, terminal for commands** — cleaner workflow, reduces risk of errors when writing file content.

### Phase 2

- **Tiingo for both stocks and crypto** — single API key, consistent JSON structure, generous free tier.
- **FRED for macro indicators** — official Federal Reserve data, free and unlimited, covers inflation, GDP, interest.
- **Timestamped filenames** — every file includes the run timestamp so historical runs are preserved and never overwritten. Critical for reprocessing and debugging.
- **SSL fix for FRED on macOS** — macOS Python requires an explicit SSL context override for FRED API calls. Added ssl.create_unverified_context as a workaround.
- **run_all.py as single entry point** — rather than running three scripts separately, a single orchestration script imports and calls all three ingestion functions in sequence. This mirrors how Airflow triggers the pipeline in Phase 5 and makes local testing simple with one command: `cd ingestion && python run_all.py`

### Phase 3

- **Deduplication handled in dbt, not loading layer** — the raw BigQuery tables intentionally preserve all records including duplicates from multiple pipeline runs. Deduplication happens in dbt staging models where the logic is version controlled, testable and rerunnable against the original raw data. This follows the principle of separation of concerns — each layer has one job. Raw layer preserves, staging layer cleans.
- **WRITE_TRUNCATE for raw BigQuery tables** — loading scripts use WRITE_TRUNCATE write disposition which truncates and replaces the table on every run. This prevents duplicate rows accumulating across multiple pipeline runs. GCS is the source of truth so BigQuery raw tables can always be safely reloaded. Incremental logic is handled in the dbt transformation layer.
- **Explicit schemas over auto-detection** — every BigQuery table has an explicitly defined schema with column names and data types. No auto-detection. This prevents silent type errors and makes the pipeline predictable and documented.

### Phase 4

- **UNION ALL over JOIN for int_market_daily** — stocks and crypto share the same structure so UNION ALL stacks them vertically into one unified market table. JOIN would create cartesian combinations which is not what we want. Rule: same structure = UNION ALL, different structure = JOIN.
- **Currency column added** — explicit currency column added to int_market_daily to clarify all values are in USD. Stocks hardcoded as USD, crypto uses quote_currency from Tiingo.
- **Full refresh over incremental** — marts use full refresh at current data volumes. Data is small enough that full refresh runs in seconds. Incremental models add complexity that isn’t justified yet. Can be refactored later.
- **dbt location must match BigQuery dataset location** — profiles.yml location must be europe-west2 not EU. EU is a multi-region, europe-west2 is a specific region. Mismatch causes dataset not found errors.
- **Pivot pattern for macro correlation mart** — used CASE WHEN with MAX to pivot indicator rows into columns. Separate latest date CTEs for monthly and quarterly indicators to handle different data frequencies.
- **NULL values filtered at staging layer** — FRED data contains NULL values for certain historical dates. Filtered in stg_macro FINAL CTE so NULLs never propagate to downstream models.
- **dbt Testing Strategy** - Added dbt_utils package to enable 'unique_conbination_of_columns' tests on composite keys (ticker +date for stocks/ crypto, indicator + date for macro). dbt's built-in 'unique' test only works on single columns, so dbt_utils is required for compostite key validation. This is the industry standard approach on professional dbt projects.

Singular tests written as raw SQL in 'dbt/tests/' for non-negative price validation on stocks and crypto. These return zero rows on pass, which is how dbt evaluates signluar tests. Business logic that generic tests cannot express belongs here.

'packages.yml' must live inside the 'dbt/' folder alongside 'dbt_project.yml', and 'dbt deps' must be run from that same directory.


### Phase 5

- **Airflow 3 locally rather than Cloud Composer** — Cloud Composer costs ~£200-400/month minimum — overkill for a portfolio project. Running Airflow locally demonstrates the same orchestration skills at near-zero cost. Cloud Composer is the Phase 6 deployment target.
- **4 separate terminal processes** — Airflow 3 requires four processes running simultaneously: scheduler, api-server, dag-processor, and triggerer. Think of it like a restaurant — the scheduler is the kitchen (executes tasks), the api-server is the front of house (serves the UI), the dag-processor scans the dags folder and registers DAGs in the metadata database, and the triggerer handles deferred tasks. Closing any terminal tab kills that process. In production on Cloud Composer, GCP manages all four as a 24/7 managed service.
- **start_airflow.sh startup script** — rather than typing 4 commands across 4 terminal tabs every session, a single bash script uses osascript to automatically open 4 Terminal tabs and run one Airflow process in each. One command starts the entire local environment.
- **Separate load tasks per data source** — the loading layer uses three parallel tasks (load_stocks, load_crypto, load_macro) rather than a single load_to_bigquery task. If one loader fails, the other two still complete and dbt can still run on the data that landed. More resilient and gives clearer visibility in the Airflow UI into exactly which source failed.
- **DAG copy workflow** — Airflow watches /Users/siddik/airflow/dags/ for DAG files. The DAG lives in the project repo at airflow/dags/ and is copied to Airflow’s watched folder after every edit. In production on Cloud Composer, Git sync replaces this manual step.
- **profiles.yml lives in ~/.dbt not in the project** — dbt’s connection profile is stored at ~/.dbt/profiles.yml rather than inside the project folder. The DAG references this path explicitly with –profiles-dir /Users/siddik/.dbt. This is the default dbt behaviour and keeps credentials outside the repo.
- **Project management in Jira** — decided to manage the project in Jira with one Epic per Phase, Stories and Tasks within each. Using a proper project management tool rather than relying on memory mirrors professional DE team practices and demonstrates workflow discipline in interviews.

## Learnings & Obstacles

### Phase 1

- **GCP Service Account Key Policy** — GCP’s default org policy blocked JSON key creation on new accounts. Switched to Application Default Credentials via gcloud CLI — more secure and avoids credentials ever touching GitHub.
- **GitHub Password Authentication Deprecated** — git push via password no longer works. Resolved using a Personal Access Token (PAT) instead.
- **macOS 12 Homebrew Compatibility** — Homebrew flagged macOS 12 as unsupported but worked correctly. No action required.
- **Terminal vs VS Code for File Editing** — writing file content via terminal commands led to mangled output. Switched to VS Code for all file editing going forward.

### Phase 2

- **ModuleNotFoundError for google.cloud** — venv was running Python 3.9 which had package conflicts. Rebuilt venv cleanly with Python 3.11 and reinstalled all dependencies.
- **Application Default Credentials lost after venv rebuild** — rebuilding the venv doesn’t affect system credentials but a new terminal session needed `gcloud auth application-default login` to be re-run.
- **SSL certificate error on macOS with FRED API** — macOS Python doesn’t trust all certificates by default. Fixed with SSL context override in the script.
- **Silent script failures** — early runs produced no output due to typos in variable names being caught silently by the except block. Lesson: always test error handling explicitly.
- **Verified raw JSON structure before moving to loading** — opened AAPL JSON in GCS to confirm data quality before building the loading layer.
- **Git merge conflicts** — repeatedly hit merge conflicts on README.md caused by making changes both locally and on GitHub without pulling first. Key lesson: always run `git pull origin main` before making any local changes.

### Phase 3

- **Duplicate rows from multiple ingestion runs** — running the ingestion script multiple times created duplicate rows in BigQuery raw tables. Deliberately left in the raw layer as expected behaviour. Deduplication handled in dbt staging models using ROW_NUMBER().
- **Understanding WRITE_TRUNCATE vs duplication** — GCS retains all historical files (immutable raw layer). WRITE_TRUNCATE wipes and reloads the BigQuery table on every run. GCS is the filing cabinet that keeps everything. BigQuery raw is the whiteboard that gets erased and rewritten fresh every run.
- **Always cd to project root before running pytest** — pytest must be run from the finance-pipeline root directory otherwise it can’t find the tests folder.

### Phase 4

- **dbt project initialised in wrong subfolder** — dbt init created files inside dbt/finance_pipeline/ instead of dbt/. Fixed by moving all files up one level with `mv finance_pipeline/* .`
- **BigQuery dataset location mismatch** — profiles.yml had location set to EU but datasets were created in europe-west2. Deleted and recreated datasets in the correct region.
- **YAML files don’t support comments** — added a comment with # in sources.yml which caused a parsing error. Removed the comment to fix.
- **Missing comma caused silent UNION ALL failure** — missing comma after CURRENCY column caused the model to fail. Always check for trailing commas in SELECT statements.
- **Always run dbt from the dbt/ folder** — dbt must be run from the folder containing dbt_project.yml.
- **BigQuery doesn’t support subqueries in JOIN predicates** — fixed by pre-calculating the latest macro date in a separate CTE first.
- **Invisible characters from copy/paste** — copying SQL from messaging apps introduced invisible unicode characters causing syntax errors. Always retype code in VS Code rather than pasting from phone.
- **dbt test: REF macro is case sensitive** Singluar test failed because I forgot to turn off caps lock. Regardless, dbt's Jinja macros are case- sensitive.

### Phase 5

- **airflow db init removed in Airflow 3** — replaced by `airflow db migrate`. Running the old command returns an error listing valid subcommands.
- **airflow webserver removed in Airflow 3** — replaced by `airflow api-server`. The UI is now served via a proper API server.
- **dag-processor is now a separate process** — in Airflow 2, the scheduler handled DAG file parsing. In Airflow 3 this was split into a dedicated dag-processor. Without it running, DAG files in the watched folder are never picked up or registered.
- **Dependency conflict between dbt-bigquery and gcsfs** — dbt-bigquery requires google-cloud-storage < 3.2 while gcsfs requires >= 3.9. Pinned to 3.1.1 which satisfies dbt-bigquery. The gcsfs warning is safely ignored as gcsfs is not directly used in this pipeline.
- **Function names in the DAG must match exactly** — the function names assumed when writing the DAG did not match the actual exported function names in the scripts. Always verify with `grep "^def " <script>.py` before writing DAG wrappers.
- **Unicode characters cause Python parse errors** — generated code containing Unicode characters (em dashes, curly quotes, box-drawing characters) causes SyntaxError when Airflow parses the DAG file. Fix: disable macOS smart quotes in System Settings -> Keyboard -> Text Input before pasting code into VS Code.
- **Airflow auto-detects DAG version changes** — when a DAG file is updated and re-parsed, Airflow 3 automatically increments the DAG version (v1, v2 etc.) visible in the UI.
- **venv must be activated in every terminal tab** — each of the 4 Airflow terminal tabs requires the venv to be activated separately. The start_airflow.sh script handles this automatically.