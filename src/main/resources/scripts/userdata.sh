#!/bin/bash
set -euxo pipefail

# deps + docker
if command -v yum >/dev/null 2>&1; then
  yum -y update || true
  yum -y install docker awscli tar gzip curl || true
  systemctl enable docker || true
  systemctl start docker || true
elif command -v apt-get >/dev/null 2>&1; then
  apt-get update -y || true
  apt-get install -y docker.io awscli tar gzip curl || true
  systemctl enable docker || true
  systemctl start docker || true
fi

export REGION='{{REGION}}'
export BUCKET='{{BUCKET}}'
export JOB_ID='{{JOB_ID}}'
export JOB_NAME='{{JOB_NAME}}'
export BUNDLE_KEY='{{BUNDLE_KEY}}'
export IMAGE='{{IMAGE}}'
export CMD='{{CMD}}'
export IGNIS_SCHEDULER_ENV_JOB='{{JOB_NAME}}'

IID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
export IGNIS_SCHEDULER_ENV_CONTAINER="$IID"

aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz
mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

docker pull "$IMAGE"

# ------------ WRAPPER ------------------------------

mkdir -p /opt/ignis

cat >/opt/ignis/run.sh << 'EOF'
#!/bin/bash
set -euo pipefail

START_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")

finish() {
  rc=$?
  set +e
  END_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "")
  state="FAILED"
  [ "$rc" -eq 0 ] && state="FINISHED"

  # logs
  aws --region "$REGION" s3 cp /tmp/out.txt "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true

  # results (ruta recomendada)
  RESULTS_DIR="/ignis/dfs/output"
  if [ -d "$RESULTS_DIR" ]; then
    aws --region "$REGION" s3 sync "$RESULTS_DIR" "s3://$BUCKET/jobs/$JOB_ID/results/" || true
  fi

  # status sentinel (clave para getContainerStatus/getJob)
  printf "{\"state\":\"%s\",\"rc\":%s,\"start\":\"%s\",\"end\":\"%s\"}\n" \
    "$state" "$rc" "$START_TS" "$END_TS" > /tmp/status.json
  aws --region "$REGION" s3 cp /tmp/status.json "s3://$BUCKET/jobs/$JOB_ID/status.json" || true

  # terminate instance (al final)
  IID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id || true)
  if [ -n "${IID:-}" ]; then
    aws --region "$REGION" ec2 terminate-instances --instance-ids "$IID" || true
  fi

  exit $rc
}
trap finish EXIT

echo "[ignis-wrapper] job=$JOB_ID name=$JOB_NAME instance=$IGNIS_SCHEDULER_ENV_CONTAINER"
echo "[ignis-wrapper] running CMD: $CMD"

# ejecuta el comando del usuario, capturando salida
set +e
/bin/bash -lc "$CMD" > /tmp/out.txt 2>&1
rc=$?
set -e
exit $rc
EOF

chmod +x /opt/ignis/run.sh
# -----------------------------------

set +e
docker run --rm \
  -e REGION -e BUCKET -e JOB_ID -e JOB_NAME -e BUNDLE_KEY -e IMAGE -e CMD \
  -e IGNIS_SCHEDULER_ENV_JOB \
  -e IGNIS_SCHEDULER_ENV_CONTAINER \
  -v /ignis/dfs:/ignis/dfs \
  -v /opt/ignis/run.sh:/run.sh:ro \
  "$IMAGE" /run.sh
rc=$?
set -e

exit $rc