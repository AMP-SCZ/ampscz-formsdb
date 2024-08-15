#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database - import
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apprise.notifications import apprise
from airflow.utils.task_group import TaskGroup
from apprise import NotifyType

CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"

METADATA_PATTERN = (
    "/data/predict1/data_from_nda/Pr*/PHOENIX/PROTECTED/P*/P*_metadata.csv"
)
METADATA_TEMP = "/data/predict1/home/dm1447/data"

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
    "ampscz_forms_db_dpimport",
    default_args=default_args,
    description="DAG for AMPSCZ forms database Import",
    schedule="@daily",
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

# Task Groups
staging_tg = TaskGroup("staging", dag=dag)
production_tg = TaskGroup("production", dag=dag)
predict2_tg = TaskGroup("predict2", dag=dag)

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
NUM_PARALLEL_IMPORT = 4


dpimport_informed_consent_run_sheet = BashOperator(
    task_id="dpimport_informed_consent_run_sheet",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_dev_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_informed_consent_run_sheet-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_dev_env,
    dag=dag,
    task_group=predict2_tg,
)

dpimport_inclusionexclusion_criteria_review = BashOperator(
    task_id="dpimport_inclusionexclusion_criteria_review",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_dev_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_inclusionexclusion_criteria_review-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_dev_env,
    dag=dag,
    task_group=predict2_tg,
)

dpimport_form_sociodemographics = BashOperator(
    task_id="dpimport_form_sociodemographics",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_dev_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_sociodemographics-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_dev_env,
    dag=dag,
    task_group=predict2_tg,
)

# Staging DPDash
dpimport_prepate_metadata_staging = BashOperator(
    task_id="dpimport_prepare_metadata_staging",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/scripts/prepare_dpdash_metadata.py"
    + f" -m '{METADATA_PATTERN}'"
    + f" -o '{METADATA_TEMP}'",
    dag=dag,
    task_group=staging_tg,
)

dpimport_metadata_staging = BashOperator(
    task_id="dpimport_metadata_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"{METADATA_TEMP}/*_metadata.csv"',
    dag=dag,
    task_group=staging_tg,
)

dpimport_informed_consent_run_sheet_staging = BashOperator(
    task_id="dpimport_informed_consent_run_sheet_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"/data/predict1/data_from_nda/formqc/??-*-form_informed_consent_run_sheet-*.csv"',
    dag=dag,
    task_group=staging_tg,
)

dpimport_inclusionexclusion_criteria_review_staging = BashOperator(
    task_id="dpimport_inclusionexclusion_criteria_review_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"/data/predict1/data_from_nda/formqc/??-*-form_inclusionexclusion_criteria_review-*.csv"',
    dag=dag,
    task_group=staging_tg,
)

dpimport_form_sociodemographics_staging = BashOperator(
    task_id="dpimport_form_sociodemographics_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"/data/predict1/data_from_nda/formqc/??-*-form_sociodemographics-*.csv"',
    dag=dag,
    task_group=staging_tg,
)

dpimport_form_filters_staging = BashOperator(
    task_id="dpimport_form_filters_staging",
    bash_command=f'{PYTHON_PATH} \
{STAGING_DPIMPORT_SCRIPT} \
-c {STAGING_DPDASH_CONFIG} \
"/PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/filters/??-*-form_filters-*.csv"',
    dag=dag,
    task_group=staging_tg,
)
# Done Task Definitions

info.set_downstream(dpimport_informed_consent_run_sheet)
info.set_downstream(dpimport_inclusionexclusion_criteria_review)
info.set_downstream(dpimport_form_sociodemographics)
info.set_downstream(dpimport_prepate_metadata_staging)
info.set_downstream(dpimport_informed_consent_run_sheet_staging)
info.set_downstream(dpimport_inclusionexclusion_criteria_review_staging)
info.set_downstream(dpimport_form_sociodemographics_staging)
info.set_downstream(dpimport_form_filters_staging)

dpimport_prepate_metadata_staging.set_downstream(dpimport_metadata_staging)

all_dpimport_done_staging = EmptyOperator(
    task_id="all_dpimport_done_staging_v2",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="All DPImport tasks successfully completed - Staging",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
    on_failure_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="One or more FormsDB tasks failed - Staging",
        notify_type=NotifyType.FAILURE,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

dpimport_metadata_staging.set_downstream(all_dpimport_done_staging)
dpimport_informed_consent_run_sheet_staging.set_downstream(all_dpimport_done_staging)
dpimport_inclusionexclusion_criteria_review_staging.set_downstream(
    all_dpimport_done_staging
)
dpimport_form_sociodemographics_staging.set_downstream(all_dpimport_done_staging)
dpimport_form_filters_staging.set_downstream(all_dpimport_done_staging)

all_dpimport_done_predict2 = EmptyOperator(
    task_id="all_dpimport_done_staging",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="All DPImport tasks successfully completed - predict2",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
    on_failure_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="One or more FormsDB tasks failed - predict2",
        notify_type=NotifyType.FAILURE,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

dpimport_informed_consent_run_sheet.set_downstream(all_dpimport_done_predict2)
dpimport_inclusionexclusion_criteria_review.set_downstream(all_dpimport_done_predict2)
dpimport_form_sociodemographics.set_downstream(all_dpimport_done_predict2)

all_dpimport_done = EmptyOperator(
    task_id="all_dpimport_done",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="All DPImport tasks successfully completed - Production",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
    on_failure_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="One or more FormsDB tasks failed - Production",
        notify_type=NotifyType.FAILURE,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)

all_dpimport_done_staging.set_downstream(all_dpimport_done)
all_dpimport_done_predict2.set_downstream(all_dpimport_done)

# Done Task Definitions
