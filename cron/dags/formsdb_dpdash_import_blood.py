#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database - import
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apprise.notifications import apprise
from apprise import NotifyType

CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"

blood_metrics_csvs = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/blood_metrics/??-*-form_bloodMetrics-*.csv"
)

# Define variables
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 4),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "ampscz_forms_dpimport_blood",
    default_args=default_args,
    description="DAG for AMPSCZ forms database Import: Blood Metrics",
    schedule=[
        blood_metrics_csvs,
    ],
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

# Import to DPDash Development instance
DPIMPORT_SCRIPT = "/data/predict1/miniconda3/bin/import.py"
STAGING_DPIMPORT_SCRIPT = "/PHShome/dm1447/dev/dpimport/scripts/import.py"
STAGING_DPDASH_CONFIG = "/PHShome/dm1447/.keys/staging_dpdash.yaml"

# Read variables from Airflow's Variables
dpimport_dev_env = {
    "state": Variable.get("DEV_STATE"),
    "HOST": Variable.get("DEV_HOST"),
    "PORT": Variable.get("MONGODB_PORT"),
    "MONGO_PASS": Variable.get("DEV_MONGO_PASS"),
    "CONFIG": Variable.get("DPIMPORT_CONFIG"),
}
dpimport_prod_env = {
    "state": Variable.get("DPDASH_PROD_STATE"),
    "HOST": Variable.get("DPDASH_PROD_HOST"),
    "PORT": Variable.get("MONGODB_PORT"),
    "MONGO_PASS": Variable.get("DPDASH_PROD_MONGO_PASS"),
    "CONFIG": Variable.get("DPIMPORT_PROD_CONFIG"),
}
NUM_PARALLEL_IMPORT = 4


dpimport_blood_metrics = BashOperator(
    task_id="dpimport_blood_metrics",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_dev_env["CONFIG"]} \
"/PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/blood_metrics/??-*-form_bloodMetrics-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_dev_env,
    dag=dag,
)

dpimport_blood_metrics_prod = BashOperator(
    task_id="dpimport_blood_metrics_prod",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_prod_env["CONFIG"]} \
"/PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/blood_metrics/??-*-form_bloodMetrics-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_prod_env,
    dag=dag,
)

# Staging DPDash
dpimport_blood_metrics_staging = BashOperator(
    task_id="dpimport_blood_metrics_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"/PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/blood_metrics/??-*-form_bloodMetrics-*.csv"',
    dag=dag,
)
# Done Task Definitions

info.set_downstream(dpimport_blood_metrics)
info.set_downstream(dpimport_blood_metrics_prod)
info.set_downstream(dpimport_blood_metrics_staging)

blood_dpimports_dome = EmptyOperator(
    task_id="blood_dpimports_done",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="Blood metrics import to DPDash done",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

dpimport_blood_metrics.set_downstream(blood_dpimports_dome)
dpimport_blood_metrics_prod.set_downstream(blood_dpimports_dome)
dpimport_blood_metrics_staging.set_downstream(blood_dpimports_dome)

# Done Task Definitions
