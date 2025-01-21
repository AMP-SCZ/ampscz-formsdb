#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ formsdb - Export combined CSV
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apprise.notifications import apprise
from apprise import NotifyType

CONDA_ENV_PATH = "/home/pnl/miniforge3/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/data/predict1/data_from_nda/formsdb/ampscz-formsdb"

dpdash_csvs = Dataset(
    uri="file:///PHOENIX/PROTECTED/_NETWORK_/processed/_SUBJECT_/surveys/_FORMS_.csv",
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
}

dag = DAG(
    "ampscz_forms_db_export_combined_csv",
    default_args=default_args,
    description="DAG for AMPSCZ formsdb - Export combined CSV",
    schedule=[dpdash_csvs],
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
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_combined_csv.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=16,
)

# Generate date-shifted combined CSV
generate_date_shifted_combined_csv = BashOperator(
    task_id="generate_date_shifted_combined_csv",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_date_offset_combined_csv.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=16,
)

# Start DAG construction

info.set_downstream(generate_combined_csv)
generate_combined_csv.set_downstream(generate_date_shifted_combined_csv)

csvs_generated = EmptyOperator(
    task_id="comvined_csvs_generated",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="Combined CSVs generated successfully",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

generate_date_shifted_combined_csv.set_downstream(csvs_generated)

# End DAG construction
