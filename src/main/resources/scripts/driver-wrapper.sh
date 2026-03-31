#!/bin/bash
set -e

ORIGINAL_CMD="$1"

echo "[Ignis Wrapper] ================================================"
echo "[Ignis Wrapper] Starting wrapper for job ${IGNIS_JOB_ID:-unknown}"
echo "[Ignis Wrapper] Original command: ${ORIGINAL_CMD}"

echo "[Ignis Wrapper] Waiting for cluster to be ready..."

# Esperar la señal publicada por createCluster
MAX_WAIT=360  # 6 minutos máximo
for ((i=0; i<MAX_WAIT; i++)); do
    if aws s3 ls "s3://${IGNIS_BUCKET}/jobs/${IGNIS_JOB_ID}/cluster-ready" >/dev/null 2>&1; then
        echo "[Ignis Wrapper] Cluster is READY! Launching real driver..."
        break
    fi
    sleep 2
done

if ! aws s3 ls "s3://${IGNIS_BUCKET}/jobs/${IGNIS_JOB_ID}/cluster-ready" >/dev/null 2>&1; then
    echo "[Ignis Wrapper] ERROR: Timeout waiting for cluster ready signal"
    exit 1
fi

echo "[Ignis Wrapper] Executing user driver now..."
exec $ORIGINAL_CMD