#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ formsdb - Export combined CSV
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.bash import BashOperator

# CONDA_ENV_PATH = "/home/pnl/dev/ampscz-formsdb/.venv"
# PYTHON_PATH = f"{CONDA_ENV_PATH}/bin/python"
PYTHON_PATH = "uv run python"
REPO_ROOT = "/data/predict1/home/dm1447/dev/ampscz-formsdb"

postgresdb = Asset(
    uri="x-db://ampscz_formsdb:imported",
)
dpdash_csvs = Asset(
    uri="x-files://ampscz_formsdb/dpdash_csvs",
)
individual_csvs = Asset(
    uri="x-files://ampscz_formsdb/individual_csvs",
)


# Define variables
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 23),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "max_active_runs": 1,
}


dag = DAG(
    dag_id="ampscz_forms_db_export_individual_csv",
    default_args=default_args,
    description="DAG for AMPSCZ formsdb - Export Individual CSV",
    schedule=[postgresdb],
)

# Define variables
info = BashOperator(
    task_id="print_info",
    bash_command='''echo "$(date) - Hostname: $(hostname)"
echo "$(date) - User: $(whoami)"
echo ""
echo "$(date) - Current directory: $(pwd)"
echo "$(date) - Git branch: $(git rev-parse --abbrev-ref HEAD)"
echo "$(date) - Git commit: $(git rev-parse HEAD)"
echo "$(date) - Git status: "
echo "$(git status --porcelain)"
echo ""
echo "$(date) - Uptime: $(uptime)"''',
    dag=dag,
    cwd=REPO_ROOT,
)

# Generate individual CSV
generate_individual_csv = BashOperator(
    task_id="generate_individual_csv",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_individual_csvs.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[individual_csvs],
    pool_slots=8,
)


# Generate DPDash CSV
generate_dpdash_csv = BashOperator(
    task_id="generate_dpdash_csv",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/dpdash_charts.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[dpdash_csvs],
)

# Define DAG
info.set_downstream(generate_individual_csv)
info.set_downstream(generate_dpdash_csv)
# End of DAG
