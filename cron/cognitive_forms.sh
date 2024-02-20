#!/usr/bin/env bash

source ~/.bash_profile

export SEPARATOR="========================================================================================================================"

export REPO_ROOT=/PHShome/dm1447/dev/ampscz-formsdb
cd $REPO_ROOT

LOG_FILE=$REPO_ROOT/data/logs/crons/cron-$(date +%Y%m%d%H%M%S).log
exec > >(tee -ia "$LOG_FILE")
exec 2> >(tee -ia "$LOG_FILE" >&2)

echo "$(date) - Starting cron job"
echo "$(date) - Log file: $LOG_FILE"
echo $SEPARATOR
echo "$(date) - Hostname: $(hostname)"
echo "$(date) - User: $(whoami)"
echo ""
echo "$(date) - Current directory: $(pwd)"
echo "$(date) - Git branch: $(git rev-parse --abbrev-ref HEAD)"
echo "$(date) - Git commit: $(git rev-parse HEAD)"
echo "$(date) - Git status: "
echo "$(git status --porcelain)"
echo ""
echo "$(date) - Uptime: $(uptime)"
echo $SEPARATOR

echo "$(date) - Activating conda environment: jupyter"
source /PHShome/dm1447/mambaforge/etc/profile.d/conda.sh
conda activate jupyter

echo $SEPARATOR
echo "$(date) - Killing any running MongoDB instances"
pkill -U $(whoami) mongod

echo "$(date) - Waiting for MongoDB to stop..."
while pgrep -u $(whoami) mongod >/dev/null; do sleep 1; done

echo "$(date) - Starting MongoDB..."
mongod --config $REPO_ROOT/data/mongod.conf

if [ $? -ne 0 ]; then
    echo "ERROR: Could not start MongoDB"
    exit 1
fi

echo $SEPARATOR
echo "$(date) - Starting PostgreSQL"
pg_ctl -D $REPO_ROOT/data/postgresql -l $REPO_ROOT/data/postgresql/logfile -o "-p 5433" restart

if [ $? -ne 0 ]; then
    echo "ERROR: Could not start PostgreSQL"
    exit 1
fi

echo $SEPARATOR
echo "Running import.py at $(date)"
$REPO_ROOT/formsdb/scripts/import.py >/dev/null

echo $SEPARATOR
echo "Running compute.py at $(date)"
$REPO_ROOT/formsdb/scripts/compute.py >/dev/null

echo $SEPARATOR
echo "Running export.py at $(date)"
$REPO_ROOT/formsdb/scripts/export.py >/dev/null

echo $SEPARATOR
echo "Running DPDash Merger at $(date)"
$REPO_ROOT/formsdb/runners/dpdash/merge_metrics.py >/dev/null

echo $SEPARATOR
echo "Importing DPDash data to predict2 (dev instance) at $(date)"
source /data/predict1/utility/.vault/.env.rc-predict-dev

export PATH=/data/predict1/miniconda3/bin/:$PATH
import.py -c $CONFIG "/data/predict1/data_from_nda/formqc/??-*-form_dpdash_charts-*.csv" >/dev/null

echo $SEPARATOR
echo "Done at $(date)"

echo $SEPARATOR

echo "$(date) - Killing MongoDB"
pkill -U $(whoami) mongod

echo "$(date) - Waiting for MongoDB to stop..."
while pgrep -u $(whoami) mongod >/dev/null; do sleep 1; done

echo "$(date) - Killing PostgreSQL"
pg_ctl -D $REPO_ROOT/data/postgresql -l $REPO_ROOT/data/postgresql/logfile -o "-p 5433" stop

echo "$(date) - Done"
