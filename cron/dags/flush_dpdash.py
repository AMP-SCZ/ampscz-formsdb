#!/usr/bin/env python
"""
Airflow DAG for flushing DPDash data
"""

from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.apprise.notifications import apprise
from apprise import NotifyType

# CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
CONDA_ENV_PATH = Variable.get("CONDA_ENV_PATH")
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/data/predict1/home/dm1447/ampscz-formsdb"

MONGO_BIN = "/data/predict1/mongodb-linux-x86_64-rhel70-4.4.20/bin/mongo"

mongo_env: Dict[str, str] = {
    "state": Variable.get("DEV_STATE"),
    "HOST": Variable.get("DEV_HOST"),
    "PORT": Variable.get("MONGODB_PORT"),
    "MONGO_PASS": Variable.get("DEV_MONGO_PASS"),
}


def construct_wipe_command(env: Dict[str, str], assesment: str) -> str:
    """
    Construct the bash command to wipe the given assessment from the DPDash database

    Args:
        env: Dict[str, str] - environment variables
        assessment: str - the assessment to wipe

    Returns:
        str - the bash command to wipe the given assessment from the DPDash database
    """
    bash_command = f"{MONGO_BIN} --tls --tlsCAFile {env['state']}/ssl/ca/cacert.pem \
        --tlsCertificateKeyFile {env['state']}/ssl/mongo_client.pem \
        mongodb://dpdash:{env['MONGO_PASS']}@{env['HOST']}:{env['PORT']}/dpdata?authSource=admin \
        --eval \"assess='{assesment}'\" \
        /data/predict1/utility/remove_assess.js"

    return bash_command


# Define variables
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 16),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "flush_dpdash",
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

flush_informed_consent = BashOperator(
    task_id="flush_informed_consent",
    bash_command=construct_wipe_command(
        env=mongo_env, assesment="form_informed_consent_run_sheet"
    ),
    dag=dag,
    env=mongo_env,
)

flush_inclusionexclusion = BashOperator(
    task_id="flush_inclusionexclusion",
    bash_command=construct_wipe_command(
        env=mongo_env, assesment="form_inclusionexclusion_criteria_review"
    ),
    dag=dag,
    env=mongo_env,
)

flush_sociodemographics = BashOperator(
    task_id="flush_sociodemographics",
    bash_command=construct_wipe_command(
        env=mongo_env, assesment="form_sociodemographics"
    ),
    dag=dag,
    env=mongo_env,
)

flush_recruitment_status = BashOperator(
    task_id="flush_recruitment_status",
    bash_command=construct_wipe_command(
        env=mongo_env, assesment="form_recruitment_status"
    ),
    dag=dag,
    env=mongo_env,
)

flush_dpdash_charts = BashOperator(
    task_id="flush_dpdash_charts",
    bash_command=construct_wipe_command(env=mongo_env, assesment="form_dpdash_charts"),
    dag=dag,
    env=mongo_env,
)

trigger_reimport = TriggerDagRunOperator(
    task_id="trigger_import",
    trigger_dag_id="ampscz_forms_db",
    dag=dag,
)

info.set_downstream(flush_informed_consent)
info.set_downstream(flush_inclusionexclusion)
info.set_downstream(flush_sociodemographics)
info.set_downstream(flush_recruitment_status)
info.set_downstream(flush_dpdash_charts)

all_flushed = EmptyOperator(
    task_id="all_flushed",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="Flushed DPDash Data",
        body="All DPDash data successfully flushed",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)
flush_informed_consent.set_downstream(all_flushed)
flush_inclusionexclusion.set_downstream(all_flushed)
flush_sociodemographics.set_downstream(all_flushed)
flush_recruitment_status.set_downstream(all_flushed)
flush_dpdash_charts.set_downstream(all_flushed)

all_flushed.set_downstream(trigger_reimport)
