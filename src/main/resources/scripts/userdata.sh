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

export IGNIS_SCHEDULER_ENV_JOB='{{JOB_NAME}}'
IID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id || echo "unknown")
export IGNIS_SCHEDULER_ENV_CONTAINER="$IID"

aws --region {{REGION}} s3 cp 's3://{{BUCKET}}/{{BUNDLE_KEY}}' /tmp/bundle.tar.gz
tar -xzf /tmp/bundle.tar.gz -C /

docker pull '{{IMAGE}}'

set +e
docker run --rm \
  -e IGNIS_SCHEDULER_ENV_JOB \
  -e IGNIS_SCHEDULER_ENV_CONTAINER \
  -v /ignis/dfs:/ignis/dfs \
  '{{IMAGE}}' /bin/bash -lc '{{CMD}}' > /tmp/out.txt 2>&1
rc=$?
set -e

aws --region {{REGION}} s3 cp /tmp/out.txt 's3://{{BUCKET}}/jobs/{{JOB_ID}}/out.txt' || true
exit $rc