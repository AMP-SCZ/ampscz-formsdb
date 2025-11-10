#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database - import
"""
import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "ampscz-formsdb":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, Variable
from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from formsdb.helpers import airflow as airflow_helpers

# CONDA_ENV_PATH = "/home/pnl/dev/ampscz-formsdb/.venv"
# PYTHON_PATH = f"{CONDA_ENV_PATH}/bin/python"
PYTHON_PATH = "uv run python"
REPO_ROOT = "/data/predict2/home/dm1447/c-dev/ampscz-formsdb"

postgresdb = Asset(
    uri="x-db://ampscz_formsdb:imported",
)

# Define variables
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 23),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "max_active_runs": 1,
}

PNL_HOSTNAME = Variable.get("PNL_HOSTNAME")
PNL_PASSWORD = Variable.get("PNL_PASSWORD")
PNL_IMPORTED_ASSET_ID = Variable.get("PNL_IMPORTED_ASSET_ID")


dag = DAG(
    "ampscz_forms_db_import",
    default_args=default_args,
    description="DAG for AMPSCZ forms database Import",
    schedule="0 20 * * *",
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
import_upenn_jsons = BashOperator(
    task_id="import_upenn_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_upenn_jsons.py",
    dag=dag,
    cwd=REPO_ROOT,
    skip_on_exit_code=1,
    pool_slots=8,
)

# Harmonized JSONs
import_harmonized_jsons = BashOperator(
    task_id="import_harmonized_jsons",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_jsons.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=8,
)

# RPMS Specific Imports
@task_group("rpms_imports", dag=dag)
def rpms_imports():
    import_rpms_entry_status = BashOperator(
        task_id="import_rpms_entry_status",
        bash_command=PYTHON_PATH
        + " "
        + REPO_ROOT
        + "/formsdb/runners/imports/import_rpms_entry_status.py",
        dag=dag,
        cwd=REPO_ROOT,
    )

    import_rpms_csvs = BashOperator(
        task_id="import_rpms_csvs",
        bash_command=PYTHON_PATH
        + " "
        + REPO_ROOT
        + "/formsdb/runners/imports/import_rpms_csvs.py",
        dag=dag,
        cwd=REPO_ROOT,
        pool_slots=8,
    )

    import_client_status = BashOperator(
        task_id="import_client_status",
        bash_command=PYTHON_PATH
        + " "
        + REPO_ROOT
        + "/formsdb/runners/imports/import_rpms_client_status.py",
        dag=dag,
        cwd=REPO_ROOT,
    )

    import_client_status_raw = BashOperator(
        task_id="import_client_status_raw",
        bash_command=PYTHON_PATH
        + " "
        + REPO_ROOT
        + "/formsdb/runners/imports/import_rpms_client_status_raw.py",
        dag=dag,
        cwd=REPO_ROOT,
    )

    import_rpms_entry_status.set_downstream(import_rpms_csvs)
    import_client_status.set_downstream(import_client_status_raw)

rpms_imports_task_group = rpms_imports()

# Form QC
import_form_qc = BashOperator(
    task_id="import_form_qc",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_form_qc.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=2,
)

# Calculated Outcomes
import_calculated_outcomes = BashOperator(
    task_id="import_calculated_outcomes",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/imports/import_calculated_outcomes.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=8,
)

def trigger_pnl_airflow_event():
    airflow_helpers.trigger_airflow_imported_asset_event(
        hostname=PNL_HOSTNAME,
        username="pnl",
        password=PNL_PASSWORD,
        imported_asset_id=int(PNL_IMPORTED_ASSET_ID),
    )

trigger_pnl_event = PythonOperator(
    task_id="trigger_pnl_airflow_event",
    python_callable=trigger_pnl_airflow_event,
    dag=dag,
)

# Done Task Definitions

# Start DAG construction
info.set_downstream(import_data_dictionary)

info.set_downstream(rpms_imports_task_group)

import_data_dictionary.set_downstream(import_upenn_jsons)
import_data_dictionary.set_downstream(import_harmonized_jsons)

rpms_imports_task_group.set_downstream(import_form_qc)
rpms_imports_task_group.set_downstream(import_calculated_outcomes)
import_harmonized_jsons.set_downstream(import_form_qc)
import_harmonized_jsons.set_downstream(import_calculated_outcomes)

all_imports_done = EmptyOperator(
    task_id="all_imports_done",
    dag=dag,
    outlets=[postgresdb],
    trigger_rule="none_failed",
)

import_form_qc.set_downstream(all_imports_done)
import_calculated_outcomes.set_downstream(all_imports_done)
import_harmonized_jsons.set_downstream(all_imports_done)
import_upenn_jsons.set_downstream(all_imports_done)

all_imports_done.set_downstream(trigger_pnl_event)

# Done DAG construction
