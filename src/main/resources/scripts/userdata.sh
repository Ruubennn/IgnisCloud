#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "[user-data] starting..."

# Dependencies installation
if grep -qi "Amazon Linux" /etc/os-release; then
  echo "[user-data] detected Amazon Linux"
  dnf -y makecache
  dnf -y update
  dnf -y swap curl-minimal curl-full --allowerasing || true
  dnf -y swap libcurl-minimal libcurl-full --allowerasing || true
  dnf -y install tar gzip docker awscli-2
  systemctl enable --now docker
else
  echo "[user-data] non-Amazon Linux, using apt fallback"
  apt-get update -y
  apt-get install -y docker.io awscli tar gzip curl
  systemctl enable --now docker
fi

command -v aws    >/dev/null 2>&1 || { echo "[user-data] ERROR: aws not found";    exit 1; }
command -v docker >/dev/null 2>&1 || { echo "[user-data] ERROR: docker not found"; exit 1; }

docker --version
aws --version || true

# Env variables
export REGION='{{REGION}}'
export BUCKET='{{BUCKET}}'
export JOB_ID='{{JOB_ID}}'
export JOB_NAME='{{JOB_NAME}}'
export BUNDLE_KEY='{{BUNDLE_KEY}}'
export IMAGE='{{IMAGE}}'
export CMD='{{CMD}}'
export IGNIS_SCHEDULER_ENV_JOB="$JOB_ID"
export IGNIS_JOB_ID="$JOB_ID"
export IGNIS_SUBNET_ID='{{SUBNET_ID}}'
export IGNIS_SG_ID='{{SG_ID}}'
export IGNIS_AMI='{{AMI}}'
export IGNIS_INSTANCE_TYPE='{{INSTANCE_TYPE}}'

echo "[DEBUG] Variables de entorno:"
echo "  REGION=$REGION"
echo "  BUCKET=$BUCKET"
echo "  JOB_ID=$JOB_ID"
echo "  JOB_NAME=$JOB_NAME"
echo "  BUNDLE_KEY=$BUNDLE_KEY"
echo "  IMAGE=$IMAGE"
echo "  CMD=$CMD"
echo "  IGNIS_SUBNET_ID=$IGNIS_SUBNET_ID"
echo "  IGNIS_SG_ID=$IGNIS_SG_ID"
echo "  IGNIS_AMI=$IGNIS_AMI"
echo "  IGNIS_INSTANCE_TYPE=$IGNIS_INSTANCE_TYPE"

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
echo "[DEBUG] bundle descargado OK, tamaño: $(du -sh /tmp/bundle.tar.gz | cut -f1)"

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /
echo "[DEBUG] bundle extraído OK"
echo "[DEBUG] contenido de /ignis:"
find /ignis -maxdepth 4 | head -60 || true

echo "[user-data] downloading large payload files from S3..."
aws s3 sync "s3://${BUCKET}/jobs/${JOB_ID}/payload/large/" "/ignis/dfs/payload/" --quiet || true
echo "[user-data] large files ready."
echo "[DEBUG] contenido de /ignis/dfs/payload/:"
find /ignis/dfs/payload/ -maxdepth 3 | head -40 || true

# Pull Docker image
echo "[user-data] pulling image $IMAGE"
docker pull "$IMAGE"
echo "[DEBUG] imagen pulled OK"

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
    echo "[DEBUG] subiendo /tmp/out.txt a S3..."
    aws --region "$REGION" s3 cp /tmp/out.txt \
      "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true
  fi

  # Upload driver log
  if [ -f /tmp/driver.log ]; then
    echo "[DEBUG] subiendo /tmp/driver.log a S3..."
    aws --region "$REGION" s3 cp /tmp/driver.log \
      "s3://$BUCKET/jobs/$JOB_ID/driver.log" || true
  fi

  # Upload backend log
  if [ -f /tmp/backend.log ]; then
    echo "[DEBUG] subiendo /tmp/backend.log a S3..."
    aws --region "$REGION" s3 cp /tmp/backend.log \
      "s3://$BUCKET/jobs/$JOB_ID/backend.log" || true
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
echo "[DEBUG] CMD que se va a ejecutar dentro del contenedor:"
echo "  $CMD"
echo "[DEBUG] IMAGE=$IMAGE"

# URL HealthCheck
mkdir -p /tmp/ignis-health
python3 -m http.server 18080 --bind 0.0.0.0 --directory /tmp/ignis-health \
  > /tmp/driver-health.log 2>&1 &
echo "[container] driver health server up on 18080"

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
  -e IGNIS_SUBNET_ID="$IGNIS_SUBNET_ID" \
  -e IGNIS_SG_ID="$IGNIS_SG_ID" \
  -e IGNIS_AMI="$IGNIS_AMI" \
  -e IGNIS_INSTANCE_TYPE="$IGNIS_INSTANCE_TYPE" \
  -v /ignis/dfs:/ignis/dfs \
  -v /var/tmp/ignis-cloud:/var/tmp/ignis-cloud \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "/opt/ignis/jobs/$JOB_ID/sockets:/opt/ignis/jobs/$JOB_ID/sockets" \
  -v /usr/bin/docker:/usr/bin/docker \
  "$IMAGE" /bin/bash -lc '
    echo "[container] ============================================"
    echo "[container] PID de este bash wrapper: $$"
    echo "[container] preparando environment..."
    mkdir -p /var/tmp/ignis/jobs
    ln -sf /var/tmp/ignis-cloud/jobs/'"$JOB_ID"'.json /var/tmp/ignis/jobs/'"$JOB_ID"'.json
    chmod -R 777 /var/tmp/ignis
    chmod 777 /tmp

    echo "[container] comprobando binarios disponibles:"
    which python3 && python3 --version || echo "[container] WARN: python3 no encontrado"
    which python  && python  --version || echo "[container] WARN: python no encontrado"
    ls /opt/ignis/bin/ || echo "[container] WARN: /opt/ignis/bin/ no existe"

    echo "[container] contenido de /ignis/dfs/payload/:"
    find /ignis/dfs/payload/ -maxdepth 3 | head -40 || true

    echo "[container] variables de entorno relevantes:"
    env | grep -E "IGNIS|JOB|CMD" | sort || true

    echo "[container] ============================================"
    echo "[container] arrancando backend..."
    /opt/ignis/bin/ignis-backend > /tmp/backend.log 2>&1 &
    BACKEND_PID=$!
    echo "[container] backend lanzado con PID=$BACKEND_PID (padre bash PID=$$)"

    echo "[container] esperando socket del backend..."
    SOCK_PATH=""
    for i in $(seq 1 30); do
      SOCK_PATH=$(find /tmp /var/tmp /opt/ignis -name "*.sock" 2>/dev/null | head -1)
      if [ -n "$SOCK_PATH" ]; then
        echo "[container] socket encontrado en $SOCK_PATH tras ${i}s"
        break
      fi
      if ! kill -0 $BACKEND_PID 2>/dev/null; then
        echo "[container] ERROR: backend murió antes de que apareciese el socket"
        echo "===== BACKEND LOG ====="
        cat /tmp/backend.log
        echo "===== END BACKEND LOG ====="
        exit 1
      fi
      echo "[container] esperando socket... intento $i/30 (backend PID=$BACKEND_PID vivo)"
      sleep 1
    done

    if [ -z "$SOCK_PATH" ]; then
      echo "[container] ERROR: socket nunca apareció tras 30s"
      echo "===== BACKEND LOG ====="
      cat /tmp/backend.log
      echo "===== END BACKEND LOG ====="
      exit 1
    fi

    echo "[container] ============================================"
    echo "[container] árbol de procesos antes de lanzar el driver:"
    ps -eo pid,ppid,cmd --forest 2>/dev/null || ps -eo pid,ppid,cmd
    echo "[container] PPid del backend según /proc:"
    cat /proc/$BACKEND_PID/status 2>/dev/null | grep -E "Pid|PPid|Name" || true
    echo "[container] ============================================"

    echo "[container] backend listo, lanzando driver..."
    echo "[container] CMD a ejecutar: '"$CMD"'"
    echo "[container] timestamp arranque driver: $(date -u)"

    '"$CMD"' > /tmp/driver.log 2>&1
    DRIVER_RC=$?

    echo "[container] ============================================"
    echo "[container] driver terminó con rc=$DRIVER_RC en $(date -u)"
    echo "===== DRIVER LOG ====="
    cat /tmp/driver.log
    echo "===== END DRIVER LOG ====="
    echo "===== BACKEND LOG ====="
    cat /tmp/backend.log
    echo "===== END BACKEND LOG ====="
    echo "[container] ============================================"

    exit $DRIVER_RC
  ' > /tmp/out.txt 2>&1

rc=$?
set -e

echo "[user-data] docker run terminó con rc=$rc"
echo "[user-data] últimas 50 líneas de /tmp/out.txt:"
tail -50 /tmp/out.txt || true

cleanup_and_finish "$rc"