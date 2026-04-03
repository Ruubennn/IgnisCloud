#!/bin/bash
set -euo pipefail

# Redirigir salida a log para debug
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "[user-data] starting..."
export IMAGE='{{IMAGE}}'

# --- 1. GESTIÓN DE DEPENDENCIAS (EL "HORNEADO") ---
if [ -f "/etc/ignis-baked" ]; then
  echo "[user-data] AMI optimizada detectada. Saltando instalaciones de paquetes."
  systemctl start docker
else
  echo "[user-data] AMI base detectada. Ejecutando instalación completa..."

  if grep -qi "Amazon Linux" /etc/os-release; then
    dnf -y install tar gzip docker awscli-2
    systemctl enable --now docker
  else
    apt-get update -y
    apt-get install -y docker.io awscli tar gzip curl
    systemctl enable --now docker
  fi
  # NO hacemos el touch aquí todavía, lo hacemos tras validar
fi

# --- 2. VALIDACIÓN Y PREPARACIÓN DE DOCKER ---
command -v aws    >/dev/null 2>&1 || { echo "[user-data] ERROR: aws not found";    exit 1; }
command -v docker >/dev/null 2>&1 || { echo "[user-data] ERROR: docker not found"; exit 1; }

# El PULL siempre fuera: garantiza que la imagen esté lista, sea en AMI base o optimizada
echo "[user-data] pulling image $IMAGE"
docker pull "$IMAGE"

# Si no existía la señal, la creamos ahora que sabemos que todo (incluido el pull) está OK
if [ ! -f "/etc/ignis-baked" ]; then
    touch /etc/ignis-baked
    echo "[user-data] Señal de optimización creada para futura AMI."
fi


# Env variables
export REGION='{{REGION}}'
export BUCKET='{{BUCKET}}'
export JOB_ID='{{JOB_ID}}'
export JOB_NAME='{{JOB_NAME}}'
export BUNDLE_KEY='{{BUNDLE_KEY}}'
export CMD='{{CMD}}'
export IGNIS_SCHEDULER_ENV_JOB="$JOB_ID"
export IGNIS_JOB_ID="$JOB_ID"

# Instance ID from metadata
IID="unknown"
TOKEN=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)

if [ -n "$TOKEN" ]; then
  IID=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
else
  IID=$(curl -fsS http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
fi

export IGNIS_SCHEDULER_ENV_CONTAINER="$IID"
echo "[user-data] instance-id=$IID"

# Bundle and payload download
echo "[user-data] downloading bundle s3://$BUCKET/$BUNDLE_KEY"
aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

echo "[user-data] downloading large payload files from S3..."
aws s3 sync "s3://${BUCKET}/jobs/${JOB_ID}/payload/large/" "/ignis/dfs/payload/" --quiet || true
echo "[user-data] large files ready."

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")


cleanup_and_finish() {
  local rc=$1
  set +e

  END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")
  local state="FAILED"
  [ "$rc" -eq 0 ] && state="FINISHED"

  echo "[user-data] job finished with rc=$rc state=$state"

  # Upload execution logs
  if [ -f /tmp/out.txt ]; then
    aws --region "$REGION" s3 cp /tmp/out.txt \
      "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true
  fi

  # Upload job results
  if [ -d "/ignis/dfs/output" ]; then
    aws --region "$REGION" s3 sync "/ignis/dfs/output" \
      "s3://$BUCKET/jobs/$JOB_ID/results/" --quiet || true
  fi

  # Upload payload directories
  find /ignis/dfs/payload/ -mindepth 1 -maxdepth 1 -type d | while read dir; do
    dirname=$(basename "$dir")
    aws --region "$REGION" s3 sync "$dir/" \
      "s3://$BUCKET/jobs/$JOB_ID/results/$dirname/" --quiet || true
  done

  # Upload state
  printf '{"state":"%s","rc":%s,"start":"%s","end":"%s"}\n' \
    "$state" "$rc" "$START_TS" "$END_TS" > /tmp/status.json

  aws --region "$REGION" s3 cp /tmp/status.json \
    "s3://$BUCKET/jobs/$JOB_ID/status.json" || true

  # Shutdown
  echo "[user-data] shutting down instance"
  shutdown -h now

  exit "$rc"
}

# Restaure job-meta from S3
echo "[user-data] restoring job meta from S3"
mkdir -p /var/tmp/ignis-cloud/jobs

aws --region "$REGION" s3 cp \
  "s3://$BUCKET/jobs/$JOB_ID/job-meta.json" \
  "/var/tmp/ignis-cloud/jobs/$JOB_ID.json"

echo "[user-data] restored meta:"
cat "/var/tmp/ignis-cloud/jobs/$JOB_ID.json" || true

# Create sockets directories for backend
mkdir -p "/opt/ignis/jobs/$JOB_ID/sockets"
chmod 777 "/opt/ignis/jobs/$JOB_ID/sockets"

# Execute container
echo "[user-data] launching Ignis backend + driver in container"
echo "[user-data] CMD=$CMD"

set +e

docker run --rm \
  --network host \
  -e IGNIS_SCHEDULER_NAME=Cloud \
  -e IGNIS_SCHEDULER_URL=cloud://aws \
  -e IGNIS_JOB_ID="$JOB_ID" \
  -e IGNIS_SCHEDULER_ENV_JOB="$JOB_ID" \
  -e IGNIS_JOB_DIR="/opt/ignis/jobs/$JOB_ID" \
  -e IGNIS_SCHEDULER_ENV_CONTAINER="$IID" \
  -e IGNIS_HOME=/opt/ignis \
  -e IGNIS_JOB_SOCKETS="/opt/ignis/jobs/$JOB_ID/sockets" \
  -e IGNIS_WDIR="/ignis/dfs/payload" \
  -e IGNIS_JOBS_BUCKET="$BUCKET" \
  -v /ignis/dfs:/ignis/dfs \
  -v /var/tmp/ignis-cloud:/var/tmp/ignis-cloud \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "/opt/ignis/jobs/$JOB_ID/sockets:/opt/ignis/jobs/$JOB_ID/sockets" \
  -v /usr/bin/docker:/usr/bin/docker \
  "$IMAGE" /bin/bash -lc '
    echo "[container] preparing environment..."
    mkdir -p /var/tmp/ignis/jobs
    ln -sf /var/tmp/ignis-cloud/jobs/'"$JOB_ID"'.json /var/tmp/ignis/jobs/'"$JOB_ID"'.json
    chmod -R 777 /var/tmp/ignis
    chmod 777 /tmp

    echo "[container] starting backend..."
    /opt/ignis/bin/ignis-backend > /tmp/backend.log 2>&1 &
    BACKEND_PID=$!

    echo "[container] waiting for backend socket..."
    SOCK_PATH=""
    for i in $(seq 1 30); do
      SOCK_PATH=$(find /tmp /var/tmp /opt/ignis -name "*.sock" 2>/dev/null | head -1)
      if [ -n "$SOCK_PATH" ]; then
        echo "[container] socket found at $SOCK_PATH after ${i}s"
        break
      fi
      if ! kill -0 $BACKEND_PID 2>/dev/null; then
        echo "[container] ERROR: backend died before socket appeared"
        echo "===== BACKEND LOG ====="
        cat /tmp/backend.log
        echo "===== END BACKEND LOG ====="
        exit 1
      fi
      sleep 1
    done

    if [ -z "$SOCK_PATH" ]; then
      echo "[container] ERROR: socket never appeared after 30s"
      echo "===== BACKEND LOG ====="
      cat /tmp/backend.log
      echo "===== END BACKEND LOG ====="
      exit 1
    fi

    echo "[container] backend ready, launching driver..."
    '"$CMD"'
    DRIVER_RC=$?

    echo "===== BACKEND LOG ====="
    cat /tmp/backend.log
    echo "===== END BACKEND LOG ====="

    exit $DRIVER_RC
  ' > /tmp/out.txt 2>&1

rc=$?
set -e

cleanup_and_finish "$rc"