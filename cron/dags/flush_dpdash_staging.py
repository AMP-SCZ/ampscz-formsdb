#!/usr/bin/env python
"""
Airflow DAG for flushing DPDash data
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apprise.notifications import apprise
from apprise import NotifyType

CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"

STAGING_DPIMPORT_REFRESH_METADATA_SCRIPT = "/PHShome/dm1447/dev/dpimport/scripts/refresh_metadata.py"
STAGING_DPDASH_CONFIG = "/PHShome/dm1447/.keys/staging_dpdash.yaml"

# Define variables
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 10),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "flush_dpdash_staging",
    default_args=default_args,
    description="DAG for flushing DPDash data",
    schedule="0 0 * * 6",  # Run only on Saturdays at midnight
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

flush_assesments = BashOperator(
    task_id="flush_day_assesments",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/scripts/wipe_dpdash.py"
    + " -c "
    + STAGING_DPDASH_CONFIG,
    dag=dag
)

flush_metadata = BashOperator(
    task_id="flush_metadata",
    bash_command=PYTHON_PATH
    + " "
    + STAGING_DPIMPORT_REFRESH_METADATA_SCRIPT
    + " -c "
    + STAGING_DPDASH_CONFIG,
    dag=dag
)

trigger_reimport_data = TriggerDagRunOperator(
    task_id="trigger_reimport_data",
    trigger_dag_id="ampscz_forms_db_import",
    dag=dag,
)
trigger_dpdash_reimport = TriggerDagRunOperator(
    task_id="trigger_dpdash_reimport",
    trigger_dag_id="ampscz_forms_db_dpimport",
    dag=dag,
)

info.set_downstream(flush_assesments)
info.set_downstream(flush_metadata)

all_flushed = EmptyOperator(
    task_id="all_flushed",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="Flushed Staging DPDash Data",
        body="All DPDash data successfully flushed",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

flush_assesments.set_downstream(all_flushed)
flush_metadata.set_downstream(all_flushed)

all_flushed.set_downstream(trigger_reimport_data)
all_flushed.set_downstream(trigger_dpdash_reimport)
