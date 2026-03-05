#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "[user-data] starting..."

if grep -qi "Amazon Linux" /etc/os-release; then
  echo "[user-data] detected Amazon Linux"

  dnf -y makecache
  dnf -y update

  # --- FIX curl-minimal -> curl-full (NO 'dnf remove curl-minimal') ---
  # This avoids trying to remove protected grub packages.
  dnf -y swap curl-minimal curl-full --allowerasing || true
  dnf -y swap libcurl-minimal libcurl-full --allowerasing || true

  # --- Install required packages ---
  # awscli2 does NOT exist in AL2023; use awscli-2 (or fallback)
  dnf -y install tar gzip docker awscli-2 || {
    echo "[user-data] awscli-2 not available, trying official installer..."
    dnf -y install unzip curl-full tar gzip docker
    tmpdir="$(mktemp -d)"
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$tmpdir/awscliv2.zip"
    unzip -q "$tmpdir/awscliv2.zip" -d "$tmpdir"
    "$tmpdir/aws/install" || true
  }

  systemctl enable --now docker

else
  echo "[user-data] non-Amazon Linux, using apt fallback"
  apt-get update -y
  apt-get install -y docker.io awscli tar gzip curl
  systemctl enable --now docker
fi

# --- sanity checks: fail early if essentials missing ---
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

IID=$(curl -fsS http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
export IGNIS_SCHEDULER_ENV_CONTAINER="$IID"

echo "[user-data] downloading bundle s3://$BUCKET/$BUNDLE_KEY"
aws --region "$REGION" s3 cp "s3://$BUCKET/$BUNDLE_KEY" /tmp/bundle.tar.gz

mkdir -p /ignis
tar -xzf /tmp/bundle.tar.gz -C /

docker pull "$IMAGE"

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

  aws --region "$REGION" s3 cp /tmp/out.txt "s3://$BUCKET/jobs/$JOB_ID/out.txt" || true

  RESULTS_DIR="/ignis/dfs/output"
  if [ -d "$RESULTS_DIR" ]; then
    aws --region "$REGION" s3 sync "$RESULTS_DIR" "s3://$BUCKET/jobs/$JOB_ID/results/" || true
  fi

  printf "{\"state\":\"%s\",\"rc\":%s,\"start\":\"%s\",\"end\":\"%s\"}\n" \
    "$state" "$rc" "$START_TS" "$END_TS" > /tmp/status.json
  aws --region "$REGION" s3 cp /tmp/status.json "s3://$BUCKET/jobs/$JOB_ID/status.json" || true

  IID=$(curl -fsS http://169.254.169.254/latest/meta-data/instance-id || true)
  if [ -n "${IID:-}" ]; then
    aws --region "$REGION" ec2 terminate-instances --instance-ids "$IID" || true
  fi

  exit $rc
}
trap finish EXIT

echo "[ignis-wrapper] job=$JOB_ID name=$JOB_NAME instance=$IGNIS_SCHEDULER_ENV_CONTAINER"
echo "[ignis-wrapper] running CMD: $CMD"

set +e
/bin/bash -lc "$CMD" > /tmp/out.txt 2>&1
rc=$?
set -e
exit $rc
EOF

chmod +x /opt/ignis/run.sh

docker run --rm \
  -e REGION -e BUCKET -e JOB_ID -e JOB_NAME -e BUNDLE_KEY -e IMAGE -e CMD \
  -e IGNIS_SCHEDULER_ENV_JOB \
  -e IGNIS_SCHEDULER_ENV_CONTAINER \
  -v /ignis/dfs:/ignis/dfs \
  -v /opt/ignis/run.sh:/run.sh:ro \
  "$IMAGE" /run.sh