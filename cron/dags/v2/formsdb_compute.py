#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database - compute
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
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

from formsdb.helpers import airflow as airflow_helpers

# CONDA_ENV_PATH = "/home/pnl/dev/ampscz-formsdb/.venv"
# PYTHON_PATH = f"{CONDA_ENV_PATH}/bin/python"
PYTHON_PATH = "uv run python"
REPO_ROOT = "/scratch/home/dm1447/dev/ampscz-formsdb"

postgresdb = Asset(
    uri="x-db://ampscz_formsdb:imported",
)
postgresdb_computed = Asset(
    uri="x-db://ampscz_formsdb:computed",
)

ERIS_HOSTNAME = Variable.get("ERIS_HOSTNAME")
ERIS_PASSWORD = Variable.get("ERIS_PASSWORD")
ERIS_COMPUTED_ASSET_ID = Variable.get("ERIS_COMPUTED_ASSET_ID")


# Define variables
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 15),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "max_active_runs": 1,
}

dag = DAG(
    "ampscz_forms_db_compute",
    default_args=default_args,
    description="DAG for AMPSCZ forms database Compute",
    schedule=[postgresdb],
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

# Tash Groups

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

compute_medication = BashOperator(
    task_id="compute_medication",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_medication.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=4,
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
    pool_slots=8,
)

compute_removed = BashOperator(
    task_id="compute_removed",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_removed.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=4,
)

compute_visit_status = BashOperator(
    task_id="compute_visit_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_visit_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=4,
)

compute_visit_completed = BashOperator(
    task_id="compute_visit_completed",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_visit_completed.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=8,
)

compute_blood_metrics = BashOperator(
    task_id="compute_blood_metrics",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_blood_metrics.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=4,
)

compute_nda_eligibility = BashOperator(
    task_id="compute_nda_eligibility",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_nda_upload_eligibility.py",
    dag=dag,
    cwd=REPO_ROOT,
    pool_slots=4,
)

def trigger_eris_airflow_event():
    airflow_helpers.trigger_airflow_imported_asset_event(
        hostname=ERIS_HOSTNAME,
        username="pnl",
        password=ERIS_PASSWORD,
        imported_asset_id=int(ERIS_COMPUTED_ASSET_ID),
    )

trigger_eris_event = PythonOperator(
    task_id="trigger_eris_airflow_event",
    python_callable=trigger_eris_airflow_event,
    dag=dag,
)

# Done Task Definitions

# Start DAG construction
info.set_downstream(compute_cognition)
info.set_downstream(compute_medication)
info.set_downstream(compute_converted)
info.set_downstream(compute_removed)
info.set_downstream(compute_visit_status)
info.set_downstream(compute_blood_metrics)
info.set_downstream(compute_visit_completed)

compute_converted.set_downstream(compute_recruitment_status)
compute_removed.set_downstream(compute_recruitment_status)
compute_visit_status.set_downstream(compute_recruitment_status)

compute_recruitment_status.set_downstream(compute_nda_eligibility)
all_done = EmptyOperator(
    task_id="all_done",
    dag=dag,
    outlets=[postgresdb_computed],
)

compute_cognition.set_downstream(all_done)
compute_medication.set_downstream(all_done)
compute_recruitment_status.set_downstream(all_done)
compute_blood_metrics.set_downstream(all_done)
compute_visit_completed.set_downstream(all_done)
compute_nda_eligibility.set_downstream(all_done)

all_done.set_downstream(trigger_eris_event)

# End DAG construction
