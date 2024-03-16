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

# # Open SSH tunnel to PostgreSQL
# # Reference: https://stackoverflow.com/questions/2241063/bash-script-to-set-up-a-temporary-ssh-tunnel
# echo $SEPARATOR
# echo "$(date) - Opening SSH tunnel to PostgreSQL at pnl-x80-3.partners.org"
# ssh -M -S formsdb-ctrl-socket -fNT -L 5433:127.0.0.1:5432 pnl-x80-3.partners.org 2>/dev/null

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

# Remove old data from MongoDB
# export PATH=/data/predict1/mongodb-linux-x86_64-rhel70-4.4.20/bin:$PATH
# mongo --tls --tlsCAFile $state/ssl/ca/cacert.pem --tlsCertificateKeyFile $state/ssl/mongo_client.pem mongodb://dpdash:$MONGO_PASS@$HOST:$PORT/dpdata?authSource=admin --eval "assess=\"form_dpdash_charts\"" /data/predict1/utility/remove_assess.js

export PATH=/data/predict1/miniconda3/bin/:$PATH
echo "$(date) - Importing 'form_informed_consent_run_sheet' data"
import.py -c $CONFIG "/data/predict1/data_from_nda/formqc/??-*-form_informed_consent_run_sheet-*.csv" -n 8 >/dev/null 2>/dev/null
echo "$(date) - Importing 'form_inclusionexclusion_criteria_review' data"
import.py -c $CONFIG "/data/predict1/data_from_nda/formqc/??-*-form_inclusionexclusion_criteria_review-*.csv" -n 8 >/dev/null 2>/dev/null
echo "$(date) - Importing 'form_sociodemographics' data"
import.py -c $CONFIG "/data/predict1/data_from_nda/formqc/??-*-form_sociodemographics-*.csv" -n 8 >/dev/null 2>/dev/null
echo "$(date) - Importing Chart Variables"
import.py -c $CONFIG "/data/predict1/data_from_nda/formqc/??-*-form_dpdash_charts-*.csv" -n 8 >/dev/null 2>/dev/null
echo "$(date) - Importing Recruitment Status"
import.py -c $CONFIG "/PHShome/dm1447/dev/ampscz-formsdb/data/generated_outputs/recruitment/??-*-form_recruitment_status-*.csv" -n 8 >/dev/null 2>/dev/null

echo $SEPARATOR
echo "Done at $(date)"

echo $SEPARATOR

echo "$(date) - Killing MongoDB"
pkill -U $(whoami) mongod

echo "$(date) - Waiting for MongoDB to stop..."
while pgrep -u $(whoami) mongod >/dev/null; do sleep 1; done

# echo "$(date) - Killing PostgreSQL"
# pg_ctl -D $REPO_ROOT/data/postgresql -l $REPO_ROOT/data/postgresql/logfile -o "-p 5433" stop

# # Stop SSH tunnel to PostgreSQL
# echo "$(date) - Closing SSH tunnel to PostgreSQL"
# ssh -S formsdb-ctrl-socket -O exit pnl-x80-3.partners.org

echo "$(date) - Done"
