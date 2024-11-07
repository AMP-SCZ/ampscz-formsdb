#!/usr/bin/env python
"""
Legacy Forms QC DAG
Uses Grace Jacob's scripts
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"

# Define variables
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 21),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "forms_qc_legacy",
    default_args=default_args,
    description="DAG for running legacy forms QC scripts",
    schedule="0 2 * * *",  # Run daily at 2am
)

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

process_prescient = BashOperator(
    task_id="process_prescient",
    bash_command="set -x; /bin/bash /data/pnl/home/gj936/U24/formqc/run_forms_qc_prescient.sh ",
    dag=dag,
)

process_pronet = BashOperator(
    task_id="process_pronet",
    bash_command="set -x; /bin/bash /data/pnl/home/gj936/U24/formqc/run_forms_qc_pronet.sh ",
    dag=dag,
)


info.set_downstream(process_prescient)
process_prescient.set_downstream(process_pronet)

all_processed = EmptyOperator(task_id="all_processed", dag=dag)

process_pronet.set_downstream(all_processed)
