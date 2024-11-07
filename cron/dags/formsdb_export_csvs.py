#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ formsdb - Export combined CSV
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"


# Define variables
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 23),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

# Outputs
dpdash_csvs = Dataset(
    uri="file:///data/predict1/data_from_nda/formqc/??-*-form_dpdash_charts-*.csv"
)

dag = DAG(
    "ampscz_forms_db_export_individual_csv",
    default_args=default_args,
    description="DAG for AMPSCZ formsdb - Export Individual CSV",
    schedule_interval=None,
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
    pool_slots=16,
)


# Generate DPDash CSV
generate_dpdash_csv = BashOperator(
    task_id="generate_dpdash_csv",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/dpdash/merge_metrics.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[dpdash_csvs],
    pool_slots=16,
)

# Define DAG
info.set_downstream(generate_individual_csv)
generate_individual_csv.set_downstream(generate_dpdash_csv)
# End of DAG
