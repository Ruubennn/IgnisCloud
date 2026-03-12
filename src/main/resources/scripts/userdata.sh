#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "[user-data] starting..."

if grep -qi "Amazon Linux" /etc/os-release; then
  echo "[user-data] detected Amazon Linux"

  dnf -y makecache
  dnf -y update

  # Cambiar curl-minimal/libcurl-minimal por las versiones completas
  dnf -y swap curl-minimal curl-full --allowerasing || true
  dnf -y swap libcurl-minimal libcurl-full --allowerasing || true

  # Instalar dependencias en el HOST
  dnf -y install tar gzip docker awscli-2

  systemctl enable --now docker
else
  echo "[user-data] non-Amazon Linux, using apt fallback"
  apt-get update -y
  apt-get install -y docker.io awscli tar gzip curl
  systemctl enable --now docker
fi

# Verificaciones en host
command -v aws >/dev/null 2>&1 || { echo "[user-data] ERROR: aws not found"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "[user-data] ERROR: docker not found"; exit 1; }

docker --version
aws --version || true

export REGION='{{REGION}}'
export BUCKET='{{BUCKET}}'
export JOB_ID='{{JOB_ID}}'
export JOB_NAME='{{JOB_NAME}}'
export BUNDLE_KEY='{{BUNDLE_KEY}}'
export IMAGE='{{IMAGE}}'
export CMD='{{CMD}}'
export IGNIS_SCHEDULER_ENV_JOB='{{JOB_NAME}}'

# Obtener instance-id en el HOST
IID="unknown"
TOKEN=""
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
echo "[user-data] downloading bundle s3://$BUCKET/$BUNDLE_KEY"

aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

docker pull "$IMAGE"

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")

cleanup_and_finish() {
  rc=$1
  set +e

  END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")
  state="FAILED"
  [ "$rc" -eq 0 ] && state="FINISHED"

  echo "[user-data] job finished with rc=$rc state=$state"

  # Subir salida estándar/errores del job
  if [ -f /tmp/out.txt ]; then
    aws --region "$REGION" s3 cp /tmp/out.txt \
      "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true
  fi

  # Subir resultados si existen
  RESULTS_DIR="/ignis/dfs/output"
  if [ -d "$RESULTS_DIR" ]; then
    aws --region "$REGION" s3 sync "$RESULTS_DIR" \
      "s3://$BUCKET/jobs/$JOB_ID/results/" || true
  fi

  # Escribir y subir status.json
  printf '{"state":"%s","rc":%s,"start":"%s","end":"%s"}\n' \
    "$state" "$rc" "$START_TS" "$END_TS" > /tmp/status.json

  aws --region "$REGION" s3 cp /tmp/status.json \
    "s3://$BUCKET/jobs/$JOB_ID/status.json" || true

# Autoterminar la instancia (best effort)
#if [ -n "${IID:-}" ] && [ "$IID" != "unknown" ]; then
#  echo "[user-data] terminating instance $IID"
#  if ! aws --region "$REGION" ec2 terminate-instances --instance-ids "$IID"; then
#    echo "[user-data] WARNING: could not self-terminate instance due to IAM permissions"
#  fi
#else
#  echo "[user-data] could not resolve instance-id, skipping self-termination"
#fi

echo "[user-data] shutting down instance"
shutdown -h now

exit "$rc"
}

echo "[user-data] running job in container"
echo "[user-data] CMD=$CMD"

set +e

echo "[user-data] checking bundled jarlibs"
ls -lah /ignis/dfs || true
ls -lah /ignis/dfs/jarlibs || true

#mkdir -p /tmp/ignis-jars
#if [ -d /ignis/dfs/jarlibs ] && compgen -G "/ignis/dfs/jarlibs/*.jar" > /dev/null; then
#  cp /ignis/dfs/jarlibs/*.jar /opt/ignis/lib/java/
#else
#  echo "[user-data] ERROR: no jarlibs found in /ignis/dfs/jarlibs"
#  exit 1
#fi

docker run --rm \
  --network host \
  -e IGNIS_SCHEDULER_TYPE=cloud \
  -e IGNIS_SCHEDULER_URL=cloud://aws \
  -e IGNIS_SCHEDULER_ENV_JOB \
  -e IGNIS_SCHEDULER_ENV_CONTAINER \
  -v /ignis/dfs:/ignis/dfs \
  -v /ignis/dfs/jarlibs:/opt/ignis/lib/java:ro \
  "$IMAGE" /bin/bash -lc "$CMD" > /tmp/out.txt 2>&1
rc=$?
set -e

cleanup_and_finish "$rc"