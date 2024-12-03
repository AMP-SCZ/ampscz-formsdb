#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database - import
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apprise.notifications import apprise
from airflow.utils.task_group import TaskGroup
from apprise import NotifyType

CONDA_ENV_PATH = "/PHShome/dm1447/mambaforge/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/PHShome/dm1447/dev/ampscz-formsdb"

postgresdb = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf"
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
    "ampscz_forms_db_import",
    default_args=default_args,
    description="DAG for AMPSCZ forms database Import",
    schedule="0 18 * * *",
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
# Data Dictionary
import_data_dictionary = BashOperator(
    task_id="import_data_dictionary",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_data_dictionary.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# UPENN JSONs
upenn_task_group = TaskGroup("upenn_jsons", dag=dag)
import_upenn_jsons = BashOperator(
    task_id="import_upenn_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_upenn_jsons.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=upenn_task_group,
    skip_on_exit_code=1,
)

export_upenn_json = BashOperator(
    task_id="export_upenn_json",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/export_upenn_mongo_to_psql.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=upenn_task_group,
)

# Harmonized JSONs
harmonized_task_group = TaskGroup("harmonized_jsons", dag=dag)
import_harmonized_jsons = BashOperator(
    task_id="import_harmonized_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_jsons.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=harmonized_task_group,
)

export_harmonized_jsons = BashOperator(
    task_id="export_harmonized_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/export_mongo_to_psql.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=harmonized_task_group,
)

# RPMS SPecific Imports
rpms_imports_task_group = TaskGroup("rpms_csvs", dag=dag)

import_rpms_csvs = BashOperator(
    task_id="import_rpms_csvs",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_rpms_csvs.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=rpms_imports_task_group,
)

import_rpms_entry_status = BashOperator(
    task_id="import_rpms_entry_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_rpms_entry_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=rpms_imports_task_group,
)

import_client_status = BashOperator(
    task_id="import_client_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_client_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=rpms_imports_task_group,
)
# Done Task Definitions

# Start DAG construction
info.set_downstream(start_mongo)

start_mongo.set_downstream(import_data_dictionary)

info.set_downstream(import_rpms_csvs)
info.set_downstream(import_rpms_entry_status)
info.set_downstream(import_client_status)

start_mongo.set_downstream(import_upenn_jsons)
start_mongo.set_downstream(import_harmonized_jsons)

import_upenn_jsons.set_downstream(export_upenn_json)
import_harmonized_jsons.set_downstream(export_harmonized_jsons)

all_imports_done = EmptyOperator(
    task_id="all_imports_done",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="All import tasks successfully completed",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
    on_failure_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="One or more FormsDB import tasks failed",
        notify_type=NotifyType.FAILURE,
        apprise_conn_id="teams",
        tag="alerts",
    ),
    outlets=[postgresdb],
    trigger_rule="none_failed",
)
export_upenn_json.set_downstream(all_imports_done)
export_harmonized_jsons.set_downstream(all_imports_done)
import_rpms_csvs.set_downstream(all_imports_done)
import_rpms_entry_status.set_downstream(all_imports_done)
import_client_status.set_downstream(all_imports_done)

# Done DAG construction
