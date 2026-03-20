#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "[user-data] starting..."

# ── Instalación de dependencias ──────────────────────────────────────────────
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

# ── Variables de entorno ─────────────────────────────────────────────────────
export REGION='{{REGION}}'
export BUCKET='{{BUCKET}}'
export JOB_ID='{{JOB_ID}}'
export JOB_NAME='{{JOB_NAME}}'
export BUNDLE_KEY='{{BUNDLE_KEY}}'
export IMAGE='{{IMAGE}}'
export CMD='{{CMD}}'
export IGNIS_SCHEDULER_ENV_JOB="$JOB_ID"
export IGNIS_JOB_ID="$JOB_ID"

# ── Instance ID desde metadata ───────────────────────────────────────────────
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

# ── Descarga del bundle y payload ────────────────────────────────────────────
echo "[user-data] downloading bundle s3://$BUCKET/$BUNDLE_KEY"
aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

echo "[user-data] downloading large payload files from S3..."
aws s3 sync "s3://${BUCKET}/jobs/${JOB_ID}/payload/large/" "/ignis/dfs/payload/" --quiet || true
echo "[user-data] large files ready."

# ── Pull de la imagen Docker ─────────────────────────────────────────────────
echo "[user-data] pulling image $IMAGE"
docker pull "$IMAGE"

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")

# ── Función de limpieza y finalización ──────────────────────────────────────
cleanup_and_finish() {
  local rc=$1
  set +e

  END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")
  local state="FAILED"
  [ "$rc" -eq 0 ] && state="FINISHED"

  echo "[user-data] job finished with rc=$rc state=$state"

  if [ -f /tmp/out.txt ]; then
    aws --region "$REGION" s3 cp /tmp/out.txt \
      "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true
  fi

  if [ -d "/ignis/dfs/output" ]; then
    aws --region "$REGION" s3 sync "/ignis/dfs/output" \
      "s3://$BUCKET/jobs/$JOB_ID/results/" || true
  fi

  aws --region "$REGION" s3 sync "/ignis/dfs/payload/" \
    "s3://$BUCKET/jobs/$JOB_ID/" \
    --exclude "*.py" --quiet || true

  echo "[user-data] looking for wordcount results..."
  find /ignis /opt/ignis/jobs/$JOB_ID -name "wordcount*" 2>/dev/null || echo "[user-data] no wordcount files found"

  if [ -f "/ignis/dfs/payload/wordcount.txt" ]; then
      echo "[user-data] found wordcount in payload"
      aws --region "$REGION" s3 cp "/ignis/dfs/payload/wordcount.txt" \
        "s3://$BUCKET/jobs/$JOB_ID/results/wordcount.txt" || true
  fi

  aws --region "$REGION" s3 sync "/opt/ignis/jobs/$JOB_ID/" \
    "s3://$BUCKET/jobs/$JOB_ID/results/" --quiet || true

  printf '{"state":"%s","rc":%s,"start":"%s","end":"%s"}\n' \
    "$state" "$rc" "$START_TS" "$END_TS" > /tmp/status.json

  aws --region "$REGION" s3 cp /tmp/status.json \
    "s3://$BUCKET/jobs/$JOB_ID/status.json" || true

  echo "[user-data] shutting down instance"
  shutdown -h now

  exit "$rc"
}

# ── Restaurar job-meta desde S3 ──────────────────────────────────────────────
echo "[user-data] restoring job meta from S3"
mkdir -p /var/tmp/ignis-cloud/jobs

aws --region "$REGION" s3 cp \
  "s3://$BUCKET/jobs/$JOB_ID/job-meta.json" \
  "/var/tmp/ignis-cloud/jobs/$JOB_ID.json"

echo "[user-data] restored meta:"
cat "/var/tmp/ignis-cloud/jobs/$JOB_ID.json" || true

# ──------ INVECIÓN DE CLAUDE con el problema de JOB_SOCKETS, elijo creer ─────────────────────────────────────────────────

mkdir -p "/opt/ignis/jobs/$JOB_ID/sockets"
chmod 777 "/opt/ignis/jobs/$JOB_ID/sockets"

# ── Ejecución del contenedor ─────────────────────────────────────────────────
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

    echo "===== FIND WORDCOUNT ====="
    find / -name "wordcount*" -not -path "*/proc/*" -not -path "*/sys/*" 2>/dev/null
    echo "===== END FIND WORDCOUNT ====="

    echo "===== BACKEND LOG ====="
    cat /tmp/backend.log
    echo "===== END BACKEND LOG ====="



    exit $DRIVER_RC
  ' > /tmp/out.txt 2>&1

rc=$?
set -e

echo "===== SSH TEST ====="
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -p 1963 root@localhost echo "SSH_OK" 2>&1 || true
echo "===== END SSH TEST ====="

echo "===== EXECUTOR LOGS ====="
docker logs "${JOB_ID}-executor-0" 2>&1 || true
echo "===== END EXECUTOR LOGS ====="

echo "===== DOCKER PS FINAL ====="
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" || true
echo "===== END DOCKER PS FINAL ====="



cleanup_and_finish "$rc"