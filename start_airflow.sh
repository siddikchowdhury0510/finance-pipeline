#!/bin/bash
# start_airflow.sh
# Run from project root to start all Airflow processes
# Usage: bash start_airflow.sh

cd /Users/Siddik/finance-pipeline
source venv/bin/activate

echo "Starting all Airflow processes..."

osascript -e 'tell app "Terminal" to do script "cd /Users/Siddik/finance-pipeline && source venv/bin/activate && airflow scheduler"'

osascript -e 'tell app "Terminal" to do script "cd /Users/Siddik/finance-pipeline && source venv/bin/activate && airflow api-server --port 8080"'

osascript -e 'tell app "Terminal" to do script "cd /Users/Siddik/finance-pipeline && source venv/bin/activate && airflow dag-processor"'

osascript -e 'tell app "Terminal" to do script "cd /Users/Siddik/finance-pipeline && source venv/bin/activate && airflow triggerer"'

echo "Done. Open http://localhost:8080"
