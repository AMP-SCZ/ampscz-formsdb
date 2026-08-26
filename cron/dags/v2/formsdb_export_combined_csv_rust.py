#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ formsdb - Export combined CSV
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# CONDA_ENV_PATH = "/home/pnl/miniforge3/envs/jupyter/bin"
# PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
PYTHON_PATH = "uv run python"
REPO_ROOT = "/data/predict2/home/dm1447/c-dev/ampscz-formsdb"
RUST_REPO_ROOT = "/data/predict2/home/dm1447/c-dev/formsdb-rust"

postgresdb_computed = Asset(
    uri="x-db://ampscz_formsdb:computed",
)
combined_csvs = Asset(
    uri="x-files://ampscz_formsdb/combined_csvs",
)
filters_csv = Asset(
    uri="x-files://ampscz_formsdb/filters_csv",
)


# Define variables
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 15),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "max_active_runs": 1,
}

AMPSCZ_DB_URI = Variable.get("AMPSCZ_DB_URI")

dag = DAG(
    "ampscz_forms_db_export_combined_csv",
    default_args=default_args,
    description="DAG for AMPSCZ formsdb - Export combined CSV",
    schedule=[postgresdb_computed],
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

# Generate combined CSV
generate_combined_csv = BashOperator(
    task_id="generate_combined_csv",
    bash_command=RUST_REPO_ROOT
    + "/target/release/export_combined_csv"
    + " --output-dir /data/predict1/data_from_nda/formsdb/generated_outputs/combined/PROTECTED"
    + " --network ProNET PRESCIENT",
    dag=dag,
    cwd=RUST_REPO_ROOT,
    env={"DB_URI": AMPSCZ_DB_URI},
)

export_filters = BashOperator(
    task_id="export_filters",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_filters.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[filters_csv],
)

# Generate date-shifted combined CSV
generate_date_shifted_combined_csv = BashOperator(
    task_id="generate_date_shifted_combined_csv",
    bash_command=RUST_REPO_ROOT
    + "/target/release/export_date_offset_csv"
    + " --input-dir /data/predict1/data_from_nda/formsdb/generated_outputs/combined/PROTECTED"
    + " --output-dir /data/predict1/data_from_nda/formsdb/generated_outputs/combined/GENERAL"
    + " --date-offset"
    + " /data/predict1/data_from_nda/Pronet/PHOENIX/PROTECTED/date_offset.csv"
    + " /data/predict1/data_from_nda/Prescient/PHOENIX/PROTECTED/date_offset.csv",
    dag=dag,
    cwd=RUST_REPO_ROOT,
    env={"DB_URI": AMPSCZ_DB_URI},
)

# Start DAG construction

info.set_downstream(generate_combined_csv)
info.set_downstream(export_filters)
generate_combined_csv.set_downstream(generate_date_shifted_combined_csv)

csvs_generated = EmptyOperator(
    task_id="combined_csvs_generated",
    dag=dag,
)

generate_date_shifted_combined_csv.set_downstream(csvs_generated)

# End DAG construction
