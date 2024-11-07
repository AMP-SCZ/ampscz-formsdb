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

dpdash_csvs = Dataset(
    uri="file:///data/predict1/data_from_nda/formqc/??-*-form_dpdash_charts-*.csv"
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
    "ampscz_forms_dpimport_charts",
    default_args=default_args,
    description="DAG for AMPSCZ forms database Import - DPDash Charts",
    schedule=[dpdash_csvs],
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

dpimport_dpdash_charts = BashOperator(
    task_id="dpimport_dpdash_charts",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_dev_env["CONFIG"]} \
"/data/predict1/data_from_nda/Pr*/PHOENIX/PROTECTED/Pr*/processed/*/surveys/??-*-form_dpdash_charts-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_dev_env,
    dag=dag,
)

# Staging DPDash
dpimport_dpdash_charts_staging = BashOperator(
    task_id="dpimport_dpdash_charts_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"/data/predict1/data_from_nda/Pr*/PHOENIX/PROTECTED/Pr*/processed/*/surveys/??-*-form_dpdash_charts-*.csv"',
    dag=dag,
)
# Done Task Definitions

# Define Task Dependencies
info.set_downstream(dpimport_dpdash_charts)
info.set_downstream(dpimport_dpdash_charts_staging)

dpdash_charts_dpimport_done = EmptyOperator(
    task_id="dpdash_charts_dpimport_done",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="DPDash Charts Import Done",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

dpimport_dpdash_charts.set_downstream(dpdash_charts_dpimport_done)
dpimport_dpdash_charts_staging.set_downstream(dpdash_charts_dpimport_done)
