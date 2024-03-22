#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.apprise.notifications import apprise
from apprise import NotifyType

CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"

# Define variables
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 15),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False
}

dag = DAG(
    "ampscz_forms_db",
    default_args=default_args,
    description="DAG for AMPSCZ forms database",
    schedule="0 0 * * 0-5",  # All days at midnight, except Saturdays
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

# Ignore exit code
start_mongo = BashOperator(
    task_id="start_mongo",
    bash_command=CONDA_ENV_PATH
    + "/mongod --config "
    + REPO_ROOT
    + "/data/mongod.conf || true",
    dag=dag,
)

# Import
import_upenn_jsons = BashOperator(
    task_id="import_upenn_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_upenn_jsons.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_upenn_json = BashOperator(
    task_id="export_upenn_json",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/export_upenn_mongo_to_psql.py",
    dag=dag,
    cwd=REPO_ROOT,
)

import_harmonized_jsons = BashOperator(
    task_id="import_harmonized_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_jsons.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_harmonized_jsons = BashOperator(
    task_id="export_harmonized_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/export_mongo_to_psql.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# Compute
compute_cognition = BashOperator(
    task_id="compute_cognition",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_cogntion.py",
    dag=dag,
    cwd=REPO_ROOT,
)

compute_converted = BashOperator(
    task_id="compute_converted",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_converted.py",
    dag=dag,
    cwd=REPO_ROOT,
)

compute_recruitment_status = BashOperator(
    task_id="compute_recruitment_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_recruitment_status.py",
    dag=dag,
    cwd=REPO_ROOT,
)

compute_removed = BashOperator(
    task_id="compute_removed",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_removed.py",
    dag=dag,
    cwd=REPO_ROOT,
)

compute_visit_status = BashOperator(
    task_id="compute_visit_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_visit_status.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# export
export_cognitive_summary = BashOperator(
    task_id="export_cognitive_summary",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_cognitive_summary.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_combined_cognitive = BashOperator(
    task_id="export_combined_cognitive",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_combined_cognitive.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_consolidated_combined_cognitive = BashOperator(
    task_id="export_consolidated_combined_cognitive",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_consolidated_combined_cognitive.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_recruitment_status = BashOperator(
    task_id="export_recruitment_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_recruitment_status.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_visit_status = BashOperator(
    task_id="export_visit_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_visit_status.py",
    dag=dag,
    cwd=REPO_ROOT,
)

export_converted = BashOperator(
    task_id="export_converted",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_converted.py",
    dag=dag,
    cwd=REPO_ROOT,
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
)

# Import to DPDash Development instance
DPIMPORT_SCRIPT = "/data/predict1/miniconda3/bin/import.py"

# Read variables from Airflow's Variables
dpimport_env = {
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
-c {dpimport_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_informed_consent_run_sheet-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_env,
    dag=dag,
)

dpimport_inclusionexclusion_criteria_review = BashOperator(
    task_id="dpimport_inclusionexclusion_criteria_review",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_inclusionexclusion_criteria_review-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_env,
    dag=dag,
)

dpimport_form_sociodemographics = BashOperator(
    task_id="dpimport_form_sociodemographics",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_sociodemographics-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_env,
    dag=dag,
)

dpimport_dpdash_charts = BashOperator(
    task_id="dpimport_dpdash_charts",
    bash_command=f'{DPIMPORT_SCRIPT} \
-c {dpimport_env["CONFIG"]} \
"/data/predict1/data_from_nda/formqc/??-*-form_dpdash_charts-*.csv" \
-n {NUM_PARALLEL_IMPORT}',
    env=dpimport_env,
    dag=dag,
)


# Done
info.set_downstream(start_mongo)
info.set_downstream(dpimport_informed_consent_run_sheet)
info.set_downstream(dpimport_inclusionexclusion_criteria_review)
info.set_downstream(dpimport_form_sociodemographics)

start_mongo.set_downstream(import_upenn_jsons)
start_mongo.set_downstream(import_harmonized_jsons)

import_upenn_jsons.set_downstream(export_upenn_json)
import_harmonized_jsons.set_downstream(export_harmonized_jsons)

all_imports_done = EmptyOperator(task_id="all_imports_done", dag=dag)
export_upenn_json.set_downstream(all_imports_done)
export_harmonized_jsons.set_downstream(all_imports_done)

all_imports_done.set_downstream(compute_cognition)
all_imports_done.set_downstream(compute_converted)
all_imports_done.set_downstream(compute_recruitment_status)
all_imports_done.set_downstream(compute_removed)
all_imports_done.set_downstream(compute_visit_status)

compute_cognition.set_downstream(export_cognitive_summary)
compute_cognition.set_downstream(export_combined_cognitive)
compute_visit_status.set_downstream(export_visit_status)

export_combined_cognitive.set_downstream(export_consolidated_combined_cognitive)

compute_recruitment_status.set_downstream(export_recruitment_status)
compute_converted.set_downstream(export_converted)

dpdash_merge_ready = EmptyOperator(task_id="dpdash_merge_ready", dag=dag)
export_cognitive_summary.set_downstream(dpdash_merge_ready)
export_recruitment_status.set_downstream(dpdash_merge_ready)
export_visit_status.set_downstream(dpdash_merge_ready)
export_converted.set_downstream(dpdash_merge_ready)

dpdash_merge_ready.set_downstream(generate_dpdash_csv)
generate_dpdash_csv.set_downstream(dpimport_dpdash_charts)


all_dpimport_done = EmptyOperator(
    task_id="all_dpimport_done",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="All DPImport tasks successfully completed",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
    on_failure_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="One or more FormsDB tasks failed",
        notify_type=NotifyType.FAILURE,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)
dpimport_informed_consent_run_sheet.set_downstream(all_dpimport_done)
dpimport_inclusionexclusion_criteria_review.set_downstream(all_dpimport_done)
dpimport_form_sociodemographics.set_downstream(all_dpimport_done)
dpimport_dpdash_charts.set_downstream(all_dpimport_done)
# dpimport_recruitment_status.set_downstream(all_dpimport_done)
