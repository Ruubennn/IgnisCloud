#!/bin/bash
set -Eeuo pipefail

exec > >(tee -a /var/log/user-data-executor.log | logger -t ignis-executor-userdata -s 2>/dev/console) 2>&1

REGION='{{REGION}}'
JOB_ID='{{JOB_ID}}'
CONTAINER_NAME='{{CONTAINER_NAME}}'
BUCKET='{{BUCKET}}'
BUNDLE_KEY='{{BUNDLE_KEY}}'
IMAGE='{{IMAGE}}'
EXECUTOR_CMD="{{EXECUTOR_CMD}}"

DEBUG_DIR="/tmp/ignis-executor-debug"
mkdir -p "$DEBUG_DIR"

log() {
  echo "[executor] $(date -u +"%Y-%m-%dT%H:%M:%SZ") $*"
}

get_instance_id() {
  local token
  token=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)

  if [ -n "$token" ]; then
    curl -fsS -H "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/instance-id || echo "unknown"
  else
    curl -fsS http://169.254.169.254/latest/meta-data/instance-id || echo "unknown"
  fi
}

upload_file() {
  local src="$1"
  local dst="$2"
  if [ -f "$src" ]; then
    aws --region "$REGION" s3 cp "$src" "s3://$BUCKET/jobs/$JOB_ID/executors/$CONTAINER_NAME/$dst" >/dev/null 2>&1 || true
  fi
}

capture_host_debug() {
  {
    echo "=== HOST DEBUG START ==="
    date -u
    echo "INSTANCE_ID=$(get_instance_id)"
    echo "HOSTNAME=$(hostname)"
    echo "WHOAMI=$(whoami)"
    echo "PWD=$(pwd)"
    echo "REGION=$REGION"
    echo "JOB_ID=$JOB_ID"
    echo "CONTAINER_NAME=$CONTAINER_NAME"
    echo "BUCKET=$BUCKET"
    echo "BUNDLE_KEY=$BUNDLE_KEY"
    echo "IMAGE=$IMAGE"
    echo "EXECUTOR_CMD=$EXECUTOR_CMD"
    echo
    echo "--- /etc/os-release ---"
    cat /etc/os-release || true
    echo
    echo "--- docker version ---"
    docker version || true
    echo
    echo "--- docker ps -a ---"
    docker ps -a || true
    echo
    echo "--- process list ---"
    ps -ef || true
    echo
    echo "--- listening sockets ---"
    ss -ltnp || true
    echo
    echo "--- env | grep IGNIS ---"
    env | sort | grep IGNIS || true
    echo "=== HOST DEBUG END ==="
  } > "$DEBUG_DIR/host-debug.txt" 2>&1
}

capture_container_debug() {
  local cid="$1"

  docker inspect "$cid" > "$DEBUG_DIR/docker-inspect.json" 2>&1 || true
  docker logs "$cid" > "$DEBUG_DIR/docker-logs.txt" 2>&1 || true
  docker top "$cid" -eo pid,ppid,user,args > "$DEBUG_DIR/docker-top.txt" 2>&1 || true

  docker exec "$cid" bash -lc '
    echo "=== CONTAINER DEBUG START ==="
    date -u
    echo "HOSTNAME=$(hostname)"
    echo "WHOAMI=$(whoami)"
    echo "PWD=$(pwd)"
    echo
    echo "--- process list ---"
    ps -ef || true
    echo
    echo "--- listening sockets ---"
    ss -ltnp || true
    echo
    echo "--- env | grep IGNIS ---"
    env | sort | grep IGNIS || true
    echo
    echo "--- root ssh dir ---"
    ls -la /root/.ssh || true
    echo
    echo "--- authorized_keys ---"
    cat /root/.ssh/authorized_keys || true
    echo
    echo "--- command availability ---"
    command -v ignis-sshserver || true
    command -v python3 || true
    echo "=== CONTAINER DEBUG END ==="
  ' > "$DEBUG_DIR/container-debug.txt" 2>&1 || true
}

capture_ssh_banner() {
  python3 - <<'PY' > "$DEBUG_DIR/ssh-banner.txt" 2>&1
import socket
host = "127.0.0.1"
port = 1963
s = socket.create_connection((host, port), timeout=5)
s.settimeout(5)
try:
    data = s.recv(256)
    print(repr(data))
finally:
    s.close()
PY
}

upload_debug_bundle() {
  upload_file /var/log/user-data-executor.log user-data-executor.log
  upload_file "$DEBUG_DIR/host-debug.txt" host-debug.txt
  upload_file "$DEBUG_DIR/container-debug.txt" container-debug.txt
  upload_file "$DEBUG_DIR/docker-inspect.json" docker-inspect.json
  upload_file "$DEBUG_DIR/docker-logs.txt" docker-logs.txt
  upload_file "$DEBUG_DIR/docker-top.txt" docker-top.txt
  upload_file "$DEBUG_DIR/ssh-banner.txt" ssh-banner.txt
}

fail() {
  local msg="$1"
  log "ERROR: $msg"
  capture_host_debug || true
  if [ -n "${CID:-}" ]; then
    capture_container_debug "$CID" || true
    capture_ssh_banner || true
  fi
  upload_debug_bundle || true
  exit 1
}

log "starting..."
log "REGION=$REGION JOB_ID=$JOB_ID CONTAINER_NAME=$CONTAINER_NAME"
log "IMAGE=$IMAGE"
log "EXECUTOR_CMD=$EXECUTOR_CMD"

# Detect OS + install deps
if grep -qi "amzn" /etc/os-release; then
  log "detected Amazon Linux"
  yum update -y || true
  yum install -y docker awscli || true
else
  log "unknown OS"
fi

systemctl enable docker
systemctl start docker

command -v aws >/dev/null 2>&1 || fail "aws not found"
command -v docker >/dev/null 2>&1 || fail "docker not found"

IID="$(get_instance_id)"
log "instance-id=$IID"

# Descargar bundle
#log "downloading bundle s3://$BUCKET/$BUNDLE_KEY"
#mkdir -p "/opt/ignis/jobs/$JOB_ID"
#cd "/opt/ignis/jobs/$JOB_ID"

#aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" bundle.tar.gz
#tar -xzf bundle.tar.gz

# Descargar bundle
log "downloading bundle s3://$BUCKET/$BUNDLE_KEY"

mkdir -p "/opt/ignis/jobs/$JOB_ID"
cd "/opt/ignis/jobs/$JOB_ID"

aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" bundle.tar.gz

# Extraer respetando la ruta absoluta esperada por Ignis
mkdir -p /ignis/dfs
tar -xzf bundle.tar.gz -C /

log "[executor] DEBUG after untar"
pwd
find /ignis/dfs/payload -maxdepth 2 -type f | sort || true
ls -l /ignis/dfs/payload/text.txt || true

# TRAZAS
#echo "[executor] DEBUG after untar"
#pwd
#find /ignis/dfs/payload -maxdepth 2 -type f | sort || true
#find /opt/ignis/jobs/$JOB_ID -maxdepth 4 -type f | sort || true
#ls -l /ignis/dfs/payload/text.txt || true
#ls -l /opt/ignis/jobs/$JOB_ID/text.txt || true
#ls -l /opt/ignis/jobs/$JOB_ID/ignis/dfs/payload/text.txt || true


log "downloading large payload files from S3..."
# si luego añades sync real, va aquí
log "large files ready."

# Pull imagen
log "pulling image $IMAGE"
docker pull "$IMAGE" || fail "docker pull failed"

# Lanzar contenedor
log "launching executor container: $CONTAINER_NAME"
log "docker command image=$IMAGE cmd=$EXECUTOR_CMD"

log "IGNIS_HEALTHCHECK_URL=${IGNIS_HEALTHCHECK_URL:-<unset>}"

mkdir -p "/opt/ignis/jobs/$JOB_ID"
mkdir -p "/opt/ignis/jobs/$JOB_ID/sockets"
chmod -R 777 "/opt/ignis/jobs/$JOB_ID"

set +e
CID=$(docker run -d \
  --name "$CONTAINER_NAME" \
  --network host \
  -e IGNIS_JOB_ID="$JOB_ID" \
  -e IGNIS_SCHEDULER_ENV_JOB="$JOB_ID" \
  -e IGNIS_SCHEDULER_ENV_CONTAINER="$CONTAINER_NAME" \
  -e IGNIS_JOB_CONTAINER_DIR="/opt/ignis/jobs" \
  -e IGNIS_JOB_SOCKETS="/opt/ignis/jobs/$JOB_ID/sockets" \
  -e IGNIS_JOB_DIR="/opt/ignis/jobs/$JOB_ID" \
  -e IGNIS_HOME=/opt/ignis \
  -e IGNIS_WDIR="/ignis/dfs/payload" \
{{EXECUTOR_ENV}} \
  -v /ignis/dfs:/ignis/dfs \
  -v "/opt/ignis/jobs/$JOB_ID:/opt/ignis/jobs/$JOB_ID" \
  "$IMAGE" {{EXECUTOR_CMD}} 2>&1)
RC=$?
set -e

if [ $RC -ne 0 ] || [ -z "${CID:-}" ]; then
  echo "$CID" > "$DEBUG_DIR/docker-run-output.txt"
  upload_file "$DEBUG_DIR/docker-run-output.txt" docker-run-output.txt
  fail "docker run failed"
fi

echo "$CID" > "$DEBUG_DIR/container-id.txt"
log "container-id=$CID"

# Esperar a que esté corriendo
READY=0
for i in $(seq 1 120); do
  RUNNING=$(docker inspect -f '{{.State.Running}}' "$CID" 2>/dev/null || echo "false")
  if [ "$RUNNING" != "true" ]; then
    capture_host_debug || true
    capture_container_debug "$CID" || true
    upload_debug_bundle || true
    fail "container died before becoming ready"
  fi

  if ss -ltn | grep -q ':1963 '; then
    log "something is listening on port 1963"
    READY=1
    break
  fi

  sleep 2
done

if [ "$READY" -ne 1 ]; then
  capture_host_debug || true
  capture_container_debug "$CID" || true
  upload_debug_bundle || true
  fail "timeout waiting for port 1963"
fi

# Capturas finas para diagnosticar handshake real
capture_host_debug || true
capture_container_debug "$CID" || true
capture_ssh_banner || true

log "host sockets on 1963:"
ss -ltnp | grep 1963 || true

log "ssh banner captured:"
cat "$DEBUG_DIR/ssh-banner.txt" || true

# Validación fuerte: queremos que el banner sea SSH
if ! grep -q "SSH-" "$DEBUG_DIR/ssh-banner.txt"; then
  upload_debug_bundle || true
  fail "port 1963 is open but banner is not SSH"
fi

# Subir debug incluso en éxito, antes del ready
upload_debug_bundle || true

# Prueba healthcheck
HC_URL=$(docker exec "$CID" bash -lc 'printf "%s" "$IGNIS_HEALTHCHECK_URL"' 2>/dev/null || true)
if [ -z "$HC_URL" ]; then
  echo "[executor] ERROR: IGNIS_HEALTHCHECK_URL empty inside container"
  docker logs "$CID" || true
  exit 1
fi

curl -fsS "$HC_URL" >/dev/null

READY_JSON="$DEBUG_DIR/ready.json"
cat > "$READY_JSON" <<EOF
{
  "state": "READY",
  "instanceId": "$IID",
  "containerName": "$CONTAINER_NAME",
  "jobId": "$JOB_ID",
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

log "uploading ready marker to S3..."
aws --region "$REGION" s3 cp "$READY_JSON" \
  "s3://$BUCKET/jobs/$JOB_ID/executors/$CONTAINER_NAME/ready.json"

log "ready marker uploaded"


CHECK_FILE="$DEBUG_DIR/output-check.txt"
{
  echo "=== OUTPUT CHECK START ==="
  date -u
  echo "--- /ignis/dfs/payload ---"
  ls -l /ignis/dfs/payload || true
  echo
  echo "--- wordcount dir ---"
  ls -l /ignis/dfs/payload/wordcount.txt || true
  echo
  echo "--- part000000 ---"
  ls -l /ignis/dfs/payload/wordcount.txt/part000000 || true
  echo
  echo "--- find wordcount ---"
  find /ignis/dfs/payload/wordcount.txt -maxdepth 2 -type f | sort || true
  echo "=== OUTPUT CHECK END ==="
} > "$CHECK_FILE" 2>&1

upload_file "$CHECK_FILE" output-check.txt

# Mantener instancia viva mientras viva el contenedor
set +e
docker wait "$CID"
RC=$?
set -e

log "container finished with rc=$RC"
docker logs "$CID" > "$DEBUG_DIR/docker-logs-final.txt" 2>&1 || true
upload_file "$DEBUG_DIR/docker-logs-final.txt" docker-logs-final.txt
upload_debug_bundle || true

# Comprobación
echo "Mi Prueba"
ls -l /ignis/dfs/payload
ls -l /ignis/dfs/payload/wordcount.txt/part000000 || true
find /ignis/dfs/payload/wordcount.txt -maxdepth 2 -type f | sort || true

# Subir resultados
RESULTS_DIR="/tmp/results"
rm -rf "$RESULTS_DIR"
mkdir -p "$RESULTS_DIR"

cp -r /ignis/dfs/payload/"$JOB_ID" "$RESULTS_DIR/" 2>/dev/null || true
cp -r /ignis/dfs/payload/wordcount.txt "$RESULTS_DIR/" 2>/dev/null || true

echo "[executor] uploaded content preview:"
find "$RESULTS_DIR" -maxdepth 3 | sort || true

aws --region "$REGION" s3 sync "$RESULTS_DIR" "s3://$BUCKET/jobs/$JOB_ID/results/" || true


upload_debug_bundle || true
shutdown -h now
exit "$RC"

