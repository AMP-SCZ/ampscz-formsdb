#!/usr/bin/env python
"""
Airflow DAG for AMPSCZ forms database - compute
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    "start_date": datetime(2024, 3, 15),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
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

# Outputs
# cognition
cognitive_data_availability = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:cognitive_data_availability"
)
cognitive_summary = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:cognitive_summary"
)

subject_removed = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:subject_removed"
)

conversion_status = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:conversion_status"
)

recruitment_status = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:recruitment_status"
)
filters = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:filters"
)

subject_visit_status = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:subject_visit_status"
)

subject_visit_completed = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:subject_visit_completed"
)

blood_metrics = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/postgresql/postgresql.conf:blood_metrics"
)
blood_metrics_csvs = Dataset(
    uri="file:///PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/blood_metrics/??-*-form_bloodMetrics-*.csv"
)

dpdash_csvs = Dataset(
    uri="file:///data/predict1/data_from_nda/formqc/??-*-form_dpdash_charts-*.csv"
)

# Tash Groups
cognition_tg = TaskGroup("cognition", dag=dag)
conversion_tg = TaskGroup("conversion", dag=dag)
visit_status_tg = TaskGroup("visit_status", dag=dag)
visit_competed_tg = TaskGroup("visit_completed", dag=dag)
recruitment_status_tg = TaskGroup("recruitment_status", dag=dag)
withdrawal_tg = TaskGroup("withdrawal", dag=dag)
blood_metrics_tg = TaskGroup("blood_metrics", dag=dag)

# Compute
compute_cognition = BashOperator(
    task_id="compute_cognition",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_cogntion.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[cognitive_data_availability, cognitive_summary],
    task_group=cognition_tg,
)

compute_converted = BashOperator(
    task_id="compute_converted",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_converted.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[conversion_status],
    task_group=conversion_tg,
)

compute_recruitment_status = BashOperator(
    task_id="compute_recruitment_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_recruitment_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[filters, recruitment_status],
    task_group=recruitment_status_tg,
)

compute_removed = BashOperator(
    task_id="compute_removed",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_removed.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[subject_removed],
    task_group=withdrawal_tg,
)

compute_visit_status = BashOperator(
    task_id="compute_visit_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_visit_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[subject_visit_status],
    task_group=visit_status_tg,
)

compute_visit_completed = BashOperator(
    task_id="compute_visit_completed",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_visit_completed.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[subject_visit_completed],
    task_group=visit_competed_tg,
)

compute_blood_metrics = BashOperator(
    task_id="compute_blood_metrics",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/compute/compute_blood_metrics.py",
    dag=dag,
    cwd=REPO_ROOT,
    outlets=[blood_metrics],
    task_group=blood_metrics_tg,
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
    task_group=cognition_tg,
)

export_combined_cognitive = BashOperator(
    task_id="export_combined_cognitive",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_combined_cognitive.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=cognition_tg,
)

export_consolidated_combined_cognitive = BashOperator(
    task_id="export_consolidated_combined_cognitive",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_consolidated_combined_cognitive.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=cognition_tg,
)

export_recruitment_status = BashOperator(
    task_id="export_recruitment_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_recruitment_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=recruitment_status_tg,
)

export_filters = BashOperator(
    task_id="export_filters",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_filters.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=recruitment_status_tg,
)

export_visit_status = BashOperator(
    task_id="export_visit_status",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_visit_status.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=visit_status_tg,
)

export_converted = BashOperator(
    task_id="export_converted",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_converted.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=conversion_tg,
)

export_withdrawal = BashOperator(
    task_id="export_withdrawal",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_withdrawal.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=withdrawal_tg,
)

export_blood_metrics = BashOperator(
    task_id="export_blood_metrics",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/formsdb/runners/export/export_blood_metrics.py",
    dag=dag,
    cwd=REPO_ROOT,
    task_group=blood_metrics_tg,
    outlets=[blood_metrics_csvs],
)

# Trigger CSV generation
trigger_combined_csv_generation = TriggerDagRunOperator(
    task_id="trigger_export_csvs",
    trigger_dag_id="ampscz_forms_db_export_individual_csv",
    dag=dag,
)
# Done Task Definitions

# Start DAG construction
info.set_downstream(compute_cognition)
info.set_downstream(compute_converted)
info.set_downstream(compute_removed)
info.set_downstream(compute_visit_status)
info.set_downstream(compute_blood_metrics)
info.set_downstream(compute_visit_completed)

compute_converted.set_downstream(compute_recruitment_status)
compute_removed.set_downstream(compute_recruitment_status)
compute_visit_status.set_downstream(compute_recruitment_status)

compute_cognition.set_downstream(export_cognitive_summary)
compute_cognition.set_downstream(export_combined_cognitive)
compute_visit_status.set_downstream(export_visit_status)

export_combined_cognitive.set_downstream(export_consolidated_combined_cognitive)

compute_recruitment_status.set_downstream(export_recruitment_status)
compute_recruitment_status.set_downstream(export_filters)
compute_converted.set_downstream(export_converted)
compute_removed.set_downstream(export_withdrawal)
compute_blood_metrics.set_downstream(export_blood_metrics)

dpdash_merge_ready = EmptyOperator(
    task_id="dpdash_merge_ready",
    dag=dag,
    on_success_callback=apprise.send_apprise_notification(
        title="AMPSCZ Forms DB",
        body="All forms measures are computed",
        notify_type=NotifyType.SUCCESS,
        apprise_conn_id="teams",
        tag="alerts",
    ),
)
export_cognitive_summary.set_downstream(dpdash_merge_ready)
export_recruitment_status.set_downstream(dpdash_merge_ready)
export_visit_status.set_downstream(dpdash_merge_ready)
export_converted.set_downstream(dpdash_merge_ready)
export_withdrawal.set_downstream(dpdash_merge_ready)
export_blood_metrics.set_downstream(dpdash_merge_ready)

dpdash_merge_ready.set_downstream(trigger_combined_csv_generation)

all_done = EmptyOperator(task_id="all_done", dag=dag)

trigger_combined_csv_generation.set_downstream(all_done)
compute_visit_completed.set_downstream(all_done)

# End DAG construction
