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
  dnf -y install tar gzip docker awscli-2 amazon-efs-utils
  systemctl enable --now docker
else
  echo "[user-data] non-Amazon Linux, using apt fallback"
  apt-get update -y
  apt-get install -y docker.io awscli tar gzip curl
  systemctl enable --now docker
fi

command -v aws >/dev/null 2>&1 || { echo "[user-data] ERROR: aws not found"; exit 1; }
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
export IGNIS_IAM_INSTANCE_PROFILE='{{IAM_INSTANCE_PROFILE}}'
export IGNIS_AMI='{{AMI}}'
export IGNIS_INSTANCE_TYPE='{{INSTANCE_TYPE}}'
export EFS_FS_ID='{{EFS_FS_ID}}'
export IGNIS_EFS_FS_ID="$EFS_FS_ID"
export IGNIS_DRIVER_URL_OVERRIDE="${IGNIS_DRIVER_URL_OVERRIDE:-}"
export IGNIS_DRIVER_PORT_OVERRIDE="${IGNIS_DRIVER_PORT_OVERRIDE:-}"
export IGNIS_DRIVER_HOST_OVERRIDE="${IGNIS_DRIVER_HOST_OVERRIDE:-}"

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")

cleanup_and_finish() {
  local rc="${1:-1}"
  set +e

  END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")
  local state="FAILED"
  [ "$rc" -eq 0 ] && state="FINISHED"

  echo "[user-data] job finished with rc=$rc state=$state"

  aws --region "$REGION" s3 cp /var/log/user-data.log \
    "s3://$BUCKET/jobs/$JOB_ID/user-data.log" || true

  if [ -f /tmp/out.txt ]; then
    aws --region "$REGION" s3 cp /tmp/out.txt \
      "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true
  fi

  if [ -f "/opt/ignis/jobs/$JOB_ID/discovery/discovery.env" ]; then
    aws --region "$REGION" s3 cp "/opt/ignis/jobs/$JOB_ID/discovery/discovery.env" \
      "s3://$BUCKET/jobs/$JOB_ID/discovery.env" || true
  fi

  if [ -f "/opt/ignis/jobs/$JOB_ID/discovery/discovery.json" ]; then
    aws --region "$REGION" s3 cp "/opt/ignis/jobs/$JOB_ID/discovery/discovery.json" \
      "s3://$BUCKET/jobs/$JOB_ID/discovery.json" || true
  fi

  if [ -f "/opt/ignis/jobs/$JOB_ID/discovery/runtime.env" ]; then
    aws --region "$REGION" s3 cp "/opt/ignis/jobs/$JOB_ID/discovery/runtime.env" \
      "s3://$BUCKET/jobs/$JOB_ID/runtime.env" || true
  fi

  if [ -d "/ignis/dfs/output" ]; then
    aws --region "$REGION" s3 sync "/ignis/dfs/output" \
      "s3://$BUCKET/jobs/$JOB_ID/results/" --quiet || true
  fi

  if [ -d "/ignis/dfs/payload" ]; then
    find /ignis/dfs/payload/ -mindepth 1 -maxdepth 1 -type d | while read -r dir; do
      dirname=$(basename "$dir")
      aws --region "$REGION" s3 sync "$dir/" \
        "s3://$BUCKET/jobs/$JOB_ID/results/$dirname/" --quiet || true
    done
  fi

  printf '{"state":"%s","rc":%s,"start":"%s","end":"%s"}\n' \
    "$state" "$rc" "$START_TS" "$END_TS" > /tmp/status.json

  aws --region "$REGION" s3 cp /tmp/status.json \
    "s3://$BUCKET/jobs/$JOB_ID/status.json" || true

  echo "[user-data] shutting down instance"
  shutdown -h now

  exit "$rc"
}

trap 'rc=$?; trap - EXIT; cleanup_and_finish "$rc"' EXIT

mount_jobs_efs() {
  if [ -z "${IGNIS_EFS_FS_ID:-}" ]; then
    echo "[user-data] ERROR: IGNIS_EFS_FS_ID is empty"
    return 1
  fi

  mkdir -p /opt/ignis/jobs

  for i in $(seq 1 18); do
    if mountpoint -q /opt/ignis/jobs; then
      echo "[user-data] EFS already mounted on /opt/ignis/jobs"
      return 0
    fi

    if mount -t efs -o tls,_netdev "${IGNIS_EFS_FS_ID}:/" /opt/ignis/jobs; then
      echo "[user-data] EFS mounted on /opt/ignis/jobs"
      return 0
    fi

    echo "[user-data] EFS mount attempt $i failed, retrying in 5s..."
    sleep 5
  done

  echo "[user-data] ERROR: unable to mount EFS ${IGNIS_EFS_FS_ID}"
  return 1
}

fetch_instance_metadata() {
  IID="unknown"
  PRIVATE_IP=""
  TOKEN=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)

  if [ -n "$TOKEN" ]; then
    IID=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
      http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
    PRIVATE_IP=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
      http://169.254.169.254/latest/meta-data/local-ipv4 || echo "")
  else
    IID=$(curl -fsS http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
    PRIVATE_IP=$(curl -fsS http://169.254.169.254/latest/meta-data/local-ipv4 || echo "")
  fi
}

write_discovery_files() {
  local discovery_dir="/opt/ignis/jobs/$JOB_ID/discovery"
  mkdir -p "$discovery_dir"

  local driver_host="$PRIVATE_IP"
  [ -n "$IGNIS_DRIVER_HOST_OVERRIDE" ] && driver_host="$IGNIS_DRIVER_HOST_OVERRIDE"

  cat > "$discovery_dir/discovery.env" <<EOF_ENV
JOB_ID=$JOB_ID
DRIVER_INSTANCE_ID=$IID
DRIVER_PRIVATE_IP=$PRIVATE_IP
DRIVER_HOST=$driver_host
DRIVER_PORT=$IGNIS_DRIVER_PORT_OVERRIDE
DRIVER_URL=$IGNIS_DRIVER_URL_OVERRIDE
DISCOVERY_MODE=tcp
UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF_ENV

  cat > "$discovery_dir/discovery.json" <<EOF_JSON
{
  "job_id": "$JOB_ID",
  "driver_instance_id": "$IID",
  "driver_private_ip": "$PRIVATE_IP",
  "driver_host": "$driver_host",
  "driver_port": "$IGNIS_DRIVER_PORT_OVERRIDE",
  "driver_url": "$IGNIS_DRIVER_URL_OVERRIDE",
  "discovery_mode": "tcp",
  "note": "local unix sockets are kept off EFS; only normal discovery files are shared",
  "updated_at": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF_JSON

  echo "[user-data] wrote discovery files to $discovery_dir"
  cat "$discovery_dir/discovery.env" || true
}

fetch_instance_metadata
export IGNIS_SCHEDULER_ENV_CONTAINER="$IID"
echo "[user-data] instance-id=$IID private-ip=$PRIVATE_IP"

echo "[user-data] downloading bundle s3://$BUCKET/$BUNDLE_KEY"
aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

echo "[user-data] downloading large payload files from S3..."
aws --region "$REGION" s3 sync "s3://${BUCKET}/jobs/${JOB_ID}/payload/large/" "/ignis/dfs/payload/" --quiet || true
echo "[user-data] large files ready."

echo "[user-data] pulling image $IMAGE"
docker pull "$IMAGE"

mount_jobs_efs

echo "[user-data] restoring job meta from S3"
mkdir -p /var/tmp/ignis-cloud/jobs
aws --region "$REGION" s3 cp \
  "s3://$BUCKET/jobs/$JOB_ID/job-meta.json" \
  "/var/tmp/ignis-cloud/jobs/$JOB_ID.json"

echo "[user-data] restored meta:"
cat "/var/tmp/ignis-cloud/jobs/$JOB_ID.json" || true

# IMPORTANT: keep sockets off EFS. Only normal files stay on EFS.
LOCAL_RUNTIME_ROOT="/run/ignis/$JOB_ID"
LOCAL_SOCKETS_DIR="$LOCAL_RUNTIME_ROOT/sockets"
mkdir -p "$LOCAL_SOCKETS_DIR"
chmod 777 "$LOCAL_RUNTIME_ROOT" "$LOCAL_SOCKETS_DIR"

mkdir -p "/opt/ignis/jobs/$JOB_ID"
chmod 777 "/opt/ignis/jobs/$JOB_ID"

# Remove any stale socket files from the shared tree so executors do not mistake them for valid remote endpoints.
find "/opt/ignis/jobs/$JOB_ID" -type s -delete || true

write_discovery_files

echo "===== JOB DIR TREE (driver host, shared EFS) ====="
find "/opt/ignis/jobs/$JOB_ID" -maxdepth 4 -printf "%y %p\n" | sort || true
echo "===== END JOB DIR TREE (driver host, shared EFS) ====="

echo "===== LOCAL SOCKET TREE (driver host, NOT shared) ====="
find "$LOCAL_RUNTIME_ROOT" -maxdepth 4 -printf "%y %p\n" | sort || true
echo "===== END LOCAL SOCKET TREE (driver host, NOT shared) ====="

cat > /tmp/ignis-driver-entry.sh <<'EOF_ENTRY'
#!/bin/bash
set -euo pipefail

echo "[container] preparing environment..."
mkdir -p /var/tmp/ignis/jobs
ln -sf "/var/tmp/ignis-cloud/jobs/${IGNIS_JOB_ID}.json" "/var/tmp/ignis/jobs/${IGNIS_JOB_ID}.json"
chmod -R 777 /var/tmp/ignis || true
chmod 777 /tmp || true
mkdir -p "$IGNIS_JOB_SOCKETS" "$IGNIS_JOB_DIR/discovery"

echo "[container] starting backend..."
/opt/ignis/bin/ignis-backend > /tmp/backend.log 2>&1 &
BACKEND_PID=$!

echo "[container] waiting for backend socket in local-only dirs..."
SOCK_PATH=""
for i in $(seq 1 30); do
  SOCK_PATH=$(find "$IGNIS_JOB_SOCKETS" /tmp /var/tmp -name "*.sock" 2>/dev/null | head -1 || true)
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

cat > "$IGNIS_JOB_DIR/discovery/runtime.env" <<EOF_RUNTIME
DRIVER_READY=1
BACKEND_SOCKET_PATH=$SOCK_PATH
UPDATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF_RUNTIME

echo "[container] backend ready, launching driver..."
eval "$CMD"
DRIVER_RC=$?

echo "===== BACKEND LOG ====="
cat /tmp/backend.log
echo "===== END BACKEND LOG ====="

exit $DRIVER_RC
EOF_ENTRY
chmod +x /tmp/ignis-driver-entry.sh

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
  -e IGNIS_JOB_SOCKETS="/opt/ignis/local-sockets" \
  -e IGNIS_WDIR=/ignis/dfs/payload \
  -e IGNIS_JOBS_BUCKET="$BUCKET" \
  -e IGNIS_SUBNET_ID="$IGNIS_SUBNET_ID" \
  -e IGNIS_SG_ID="$IGNIS_SG_ID" \
  -e IGNIS_AMI="$IGNIS_AMI" \
  -e IGNIS_INSTANCE_TYPE="$IGNIS_INSTANCE_TYPE" \
  -e IGNIS_EFS_FS_ID="$IGNIS_EFS_FS_ID" \
  -e IGNIS_IAM_INSTANCE_PROFILE="$IGNIS_IAM_INSTANCE_PROFILE" \
  -e IGNIS_DRIVER_PRIVATE_IP="$PRIVATE_IP" \
  -e IGNIS_DRIVER_HOST_OVERRIDE="$IGNIS_DRIVER_HOST_OVERRIDE" \
  -e IGNIS_DRIVER_PORT_OVERRIDE="$IGNIS_DRIVER_PORT_OVERRIDE" \
  -e IGNIS_DRIVER_URL_OVERRIDE="$IGNIS_DRIVER_URL_OVERRIDE" \
  -e CMD="$CMD" \
  -v /ignis/dfs:/ignis/dfs \
  -v /var/tmp/ignis-cloud:/var/tmp/ignis-cloud \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "/opt/ignis/jobs/$JOB_ID:/opt/ignis/jobs/$JOB_ID" \
  -v "$LOCAL_SOCKETS_DIR:/opt/ignis/local-sockets" \
  -v /tmp/ignis-driver-entry.sh:/driver-entry.sh:ro \
  -v /usr/bin/docker:/usr/bin/docker \
  "$IMAGE" /driver-entry.sh > /tmp/out.txt 2>&1
rc=$?
set -e

exit "$rc"