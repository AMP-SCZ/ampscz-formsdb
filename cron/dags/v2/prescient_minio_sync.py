"""
Prescient MinIO Data Sync DAG (ERIS)

This DAG establishes a secure tunnel using Ghostunnel and then mirrors data
from a remote MinIO bucket to a local directory.

The DAG is designed to be triggered manually (on-demand) only.
"""

import logging
import os
import signal
import subprocess
import time
from datetime import timedelta

from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}

# Define the DAG
dag = DAG(
    "prescient_minio_sync",
    default_args=default_args,
    description="Sync data from Prescient MinIO bucket via secure tunnel",
    tags=["prescient", "minio", "sync", "manual"],
    catchup=False,
    params=ParamsDict(
        {
            "ghostunnel_path": Param(
                "/PHShome/dm1447/build/ghostunnel/ghostunnel",
                type="string",
                title="Ghostunnel Binary Path",
                description="Absolute path to the ghostunnel executable",
                section="Tunnel Configuration",
            ),
            "mc_path": Param(
                "/PHShome/dm1447/bin/mc",
                type="string",
                title="MinIO Client Path",
                description="Absolute path to the mc (MinIO client) executable",
                section="Tunnel Configuration",
            ),
            "cert_path": Param(
                "/data/predict2/credentials/prescient_minio/signed_prescient_minio_v2.cert",
                type="string",
                title="Certificate Path",
                description="Absolute path to the SSL certificate file",
                section="Tunnel Configuration",
            ),
            "key_path": Param(
                "/data/predict2/credentials/prescient_minio/prescient_minio.key",
                type="string",
                title="Private Key Path",
                description="Absolute path to the SSL private key file",
                section="Tunnel Configuration",
            ),
            "local_tunnel_port": Param(
                "34567",
                type="string",
                title="Local Tunnel Port",
                description="Port number for the local tunnel endpoint",
                section="Tunnel Configuration",
            ),
            "remote_target": Param(
                "data.prescient.endo-vl.cloud.edu.au:443",
                type="string",
                title="Remote Target",
                description="Remote MinIO server address and port",
                section="Tunnel Configuration",
            ),
            "minio_alias": Param(
                "ycminio",
                type="string",
                title="MinIO Alias",
                description="Alias name for the MinIO server configuration",
                section="MinIO Configuration",
            ),
            "source_bucket": Param(
                "ycminio/export-buffer",
                type="string",
                title="Source Bucket",
                description="Source bucket path to mirror from (format: alias/bucket-name)",
                section="MinIO Configuration",
            ),
            "destination_path": Param(
                "/data/predict2/data_from_nda/Prescient/PHOENIX/PROTECTED/MINDLAMP/minio_bucket/export-buffer",
                type="string",
                title="Destination Path",
                description="Local directory path where data will be mirrored to",
                section="MinIO Configuration",
            ),
            "tunnel_startup_wait_time": Param(
                5,
                type="integer",
                title="Tunnel Startup Wait Time (seconds)",
                description="Time to wait for tunnel to establish before proceeding",
                minimum=1,
                section="Tunnel Configuration",
            ),
            "log_file_path": Param(
                "/tmp/ghostunnel_minio_sync.log",
                type="string",
                title="Log File Path",
                description="Path to the log file for ghostunnel output",
                section="Tunnel Configuration",
            ),
            "enable_overwrite": Param(
                False,
                type="boolean",
                title="Enable Overwrite",
                description="If enabled, adds --overwrite flag to mirror command to overwrite existing files",
                section="MinIO Configuration",
            ),
        }
    ),
)


# Python function to start the Ghostunnel tunnel
def start_tunnel(**context):
    """Start Ghostunnel tunnel and store the process ID in XCom"""
    params = context["params"]

    cmd = [
        params["ghostunnel_path"],
        "client",
        "--listen",
        f"localhost:{params['local_tunnel_port']}",
        "--target",
        params["remote_target"],
        "--cert",
        params["cert_path"],
        "--key",
        params["key_path"],
    ]

    logging.info(f"Starting Ghostunnel tunnel with command: {' '.join(cmd)}")

    # Start the tunnel process with nohup-like behavior to prevent it from being killed
    # Redirect stdout and stderr to log file
    log_file = params["log_file_path"]
    logging.info(f"Ghostunnel output will be logged to: {log_file}")

    with open(log_file, "a", encoding="utf-8") as log:
        process = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,  # Redirect stderr to stdout (both go to log file)
            start_new_session=True,  # Detach from parent process group
        )
    pid = process.pid

    logging.info(f"Ghostunnel tunnel started with PID: {pid}")

    # Wait for tunnel to establish
    wait_time = params["tunnel_startup_wait_time"]
    logging.info(f"Waiting {wait_time} seconds for tunnel to establish...")
    time.sleep(wait_time)

    # Return the PID so it can be used to terminate the process later
    return pid


# Python function to stop the Ghostunnel tunnel
def stop_tunnel(**context):
    """Stop the Ghostunnel tunnel using the PID from XCom"""
    ti = context["ti"]
    pid = ti.xcom_pull(task_ids="start_ghostunnel_tunnel")

    if pid:
        logging.info(f"Stopping Ghostunnel tunnel with PID: {pid}")
        try:
            os.kill(pid, signal.SIGTERM)
            logging.info("Ghostunnel tunnel stopped successfully")
        except OSError as e:
            logging.error(f"Error stopping Ghostunnel tunnel: {e}")
    else:
        logging.warning("No PID found for Ghostunnel tunnel")


# Task 1: Start the Ghostunnel tunnel
start_ghostunnel_tunnel = PythonOperator(
    task_id="start_ghostunnel_tunnel",
    python_callable=start_tunnel,
    dag=dag,
)

# Task 2: Check if the tunnel is active
check_tunnel = BashOperator(
    task_id="check_tunnel",
    bash_command="""
        export HTTP_PROXY=http://localhost:{{ params.local_tunnel_port }}
        export HTTPS_PROXY=http://localhost:{{ params.local_tunnel_port }}
        
        echo "Checking if tunnel is active..."
        {{ params.mc_path }} ping --count 5 --interval 1 {{ params.minio_alias }}
        
        if [ $? -ne 0 ]; then
            echo "Tunnel check failed. Exiting."
            exit 1
        fi
        
        echo "Tunnel is active and connection to MinIO server is successful."
    """,
    dag=dag,
)

# Task 3: Mirror data from MinIO bucket to local directory
mirror_data = BashOperator(
    task_id="mirror_data",
    bash_command="""
        export HTTP_PROXY=http://localhost:{{ params.local_tunnel_port }}
        export HTTPS_PROXY=http://localhost:{{ params.local_tunnel_port }}
        
        echo "Starting data mirroring from {{ params.source_bucket }} to {{ params.destination_path }}..."
        {{ params.mc_path }} mirror {{ params.source_bucket }} {{ params.destination_path }} --skip-errors --summary {% if params.enable_overwrite %}--overwrite{% endif %}
        
        echo "Data mirroring completed successfully."
    """,
    dag=dag,
)

# Task 4: Stop the Ghostunnel tunnel
stop_ghostunnel_tunnel = PythonOperator(
    task_id="stop_ghostunnel_tunnel",
    python_callable=stop_tunnel,
    trigger_rule="all_done",  # Ensure this runs even if previous tasks fail
    dag=dag,
)

# Set task dependencies
start_ghostunnel_tunnel >> check_tunnel >> mirror_data >> stop_ghostunnel_tunnel
