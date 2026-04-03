#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

#echo "[executor] starting..."

# -----------------------------
# Dependencies
# -----------------------------
#if grep -qi "Amazon Linux" /etc/os-release; then
#  echo "[executor] detected Amazon Linux"
#  dnf -y makecache
#  dnf -y update
#  dnf -y swap curl-minimal curl-full --allowerasing || true
#  dnf -y swap libcurl-minimal libcurl-full --allowerasing || true
#  dnf -y install tar gzip docker awscli-2 --setopt=install_weak_deps=False
#  systemctl enable --now docker
#else
#  echo "[executor] non-Amazon Linux, using apt fallback"
#  apt-get update -y
#  apt-get install -y docker.io awscli tar gzip curl
#  systemctl enable --now docker
#fi

#command -v aws >/dev/null 2>&1 || { echo "[executor] ERROR: aws not found"; exit 1; }
#command -v docker >/dev/null 2>&1 || { echo "[executor] ERROR: docker not found"; exit 1; }

#docker --version
#aws --version || true

# -----------------------------
# Template vars
# -----------------------------
export REGION='{{REGION}}'
export BUCKET='{{BUCKET}}'
export JOB_ID='{{JOB_ID}}'
export BUNDLE_KEY='{{BUNDLE_KEY}}'
export IMAGE='{{IMAGE}}'
export CONTAINER_NAME='{{CONTAINER_NAME}}'
export EXECUTOR_CMD='{{EXECUTOR_CMD}}'
export EXECUTOR_ENV_B64='{{EXECUTOR_ENV_B64}}'

# -----------------------------
# Metadata
# -----------------------------
IID="unknown"
PRIVATE_IP="unknown"

TOKEN=$(curl -fsS -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)

if [ -n "$TOKEN" ]; then
  IID=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
  PRIVATE_IP=$(curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/local-ipv4 || echo "unknown")
else
  IID=$(curl -fsS http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
  PRIVATE_IP=$(curl -fsS http://169.254.169.254/latest/meta-data/local-ipv4 || echo "unknown")
fi

#echo "[executor] instance-id=$IID"
#echo "[executor] private-ip=$PRIVATE_IP"

# -----------------------------
# Bundle + payload
# -----------------------------
#echo "[executor] downloading bundle s3://$BUCKET/$BUNDLE_KEY"
aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

#echo "[executor] downloading large payload files from S3..."
aws --region "$REGION" s3 sync "s3://${BUCKET}/jobs/${JOB_ID}/payload/large/" "/ignis/dfs/payload/" --quiet || true
#echo "[executor] large files ready."

#echo "[executor] pulling image $IMAGE"
#docker pull "$IMAGE"

# -----------------------------
# Wait for driver IP
# -----------------------------
echo "[executor] waiting for driver IP..."
DRIVER_IP=""
for i in $(seq 1 300); do
  DRIVER_IP=$(aws --region "$REGION" s3 cp "s3://$BUCKET/jobs/$JOB_ID/driver-ip.txt" - 2>/dev/null || true)
  DRIVER_IP=$(echo "${DRIVER_IP:-}" | tr -d '\r\n[:space:]')
  if [ -n "$DRIVER_IP" ]; then
    break
  fi
  sleep 1
done

if [ -z "$DRIVER_IP" ]; then
  echo "[executor] ERROR: driver-ip.txt not found"
  printf '{"container":"%s","instanceId":"%s","ip":"%s","state":"FAILED","reason":"DRIVER_IP_NOT_FOUND"}\n' \
    "$CONTAINER_NAME" "$IID" "$PRIVATE_IP" > /tmp/executor.failed.json
  aws --region "$REGION" s3 cp /tmp/executor.failed.json \
    "s3://$BUCKET/jobs/$JOB_ID/ready/$CONTAINER_NAME.failed.json" --quiet || true
  exit 1
fi

echo "[executor] driver IP resolved: $DRIVER_IP"

# -----------------------------
# Decode executor env
# -----------------------------
EXEC_ENV_FILE=/tmp/executor.env
: > "$EXEC_ENV_FILE"

if [ -n "${EXECUTOR_ENV_B64:-}" ]; then
  echo "$EXECUTOR_ENV_B64" | base64 -d > "$EXEC_ENV_FILE"
fi

# Añadimos variables mínimas del scheduler
cat >> "$EXEC_ENV_FILE" <<EOF
IGNIS_SCHEDULER_ENV_JOB=$JOB_ID
IGNIS_SCHEDULER_ENV_CONTAINER=$CONTAINER_NAME
IGNIS_JOB_ID=$JOB_ID
IGNIS_JOB_DIR=/opt/ignis/jobs/$JOB_ID
IGNIS_JOB_CONTAINER_DIR=/opt/ignis/jobs
EOF

# -----------------------------
# Launch executor container
# -----------------------------
mkdir -p "/opt/ignis/jobs/$JOB_ID"
chmod 777 "/opt/ignis/jobs/$JOB_ID" || true

echo "[executor] launching executor container: $CONTAINER_NAME"
echo "[executor] EXECUTOR_CMD will be: $EXECUTOR_CMD"

set +e
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

docker run -d \
  --network host \
  --name "$CONTAINER_NAME" \
  --env-file "$EXEC_ENV_FILE" \
  -e IGNIS_DRIVER_IP="$DRIVER_IP" \
  -e IGNIS_SCHEDULER_NAME=Cloud \
  -e IGNIS_SCHEDULER_URL=cloud://aws \
  -e IGNIS_WDIR=/ignis/dfs/payload \
  -v /ignis/dfs:/ignis/dfs \
  -v "/opt/ignis/jobs/$JOB_ID:/opt/ignis/jobs/$JOB_ID" \
  "$IMAGE" /bin/bash -lc "$EXECUTOR_CMD" > /tmp/docker-run.out 2>&1
RC=$?
set -e

if [ "$RC" -ne 0 ]; then
  echo "[executor] ERROR: docker run failed"
  cat /tmp/docker-run.out || true
  printf '{"container":"%s","instanceId":"%s","ip":"%s","state":"FAILED","reason":"DOCKER_RUN_FAILED"}\n' \
    "$CONTAINER_NAME" "$IID" "$PRIVATE_IP" > /tmp/executor.failed.json
  aws --region "$REGION" s3 cp /tmp/executor.failed.json \
    "s3://$BUCKET/jobs/$JOB_ID/ready/$CONTAINER_NAME.failed.json" --quiet || true
  exit 1
fi

# -----------------------------
# Wait for listening port
# -----------------------------
echo "[executor] waiting for executor port in logs..."
PORT=""

for i in $(seq 1 180); do
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    echo "[executor] ERROR: container exited before publishing ready"
    docker logs "$CONTAINER_NAME" || true
    printf '{"container":"%s","instanceId":"%s","ip":"%s","state":"FAILED","reason":"CONTAINER_EXITED_EARLY"}\n' \
      "$CONTAINER_NAME" "$IID" "$PRIVATE_IP" > /tmp/executor.failed.json
    aws --region "$REGION" s3 cp /tmp/executor.failed.json \
      "s3://$BUCKET/jobs/$JOB_ID/ready/$CONTAINER_NAME.failed.json" --quiet || true
    exit 1
  fi

  LOGS=$(docker logs "$CONTAINER_NAME" 2>&1 || true)

  PORT=$(
    printf '%s\n' "$LOGS" \
      | sed -nE 's/.*port[[:space:]]+([0-9]+).*/\1/p' \
      | tail -1
  )

  if [ -n "$PORT" ]; then
    break
  fi

  sleep 1
done

if [ -z "$PORT" ]; then
  echo "[executor] ERROR: could not detect executor port from logs"
  docker logs "$CONTAINER_NAME" || true
  printf '{"container":"%s","instanceId":"%s","ip":"%s","state":"FAILED","reason":"PORT_NOT_DETECTED"}\n' \
    "$CONTAINER_NAME" "$IID" "$PRIVATE_IP" > /tmp/executor.failed.json
  aws --region "$REGION" s3 cp /tmp/executor.failed.json \
    "s3://$BUCKET/jobs/$JOB_ID/ready/$CONTAINER_NAME.failed.json" --quiet || true
  exit 1
fi

# -----------------------------
# Publish ready.json
# -----------------------------
printf '{"container":"%s","instanceId":"%s","ip":"%s","port":%s,"driverIp":"%s"}\n' \
  "$CONTAINER_NAME" "$IID" "$PRIVATE_IP" "$PORT" "$DRIVER_IP" > /tmp/executor.ready.json

aws --region "$REGION" s3 cp /tmp/executor.ready.json \
  "s3://$BUCKET/jobs/$JOB_ID/ready/$CONTAINER_NAME.json" --quiet

echo "[executor] ready published: ip=$PRIVATE_IP port=$PORT"

# -----------------------------
# End: leave instance running
# -----------------------------
exit 0