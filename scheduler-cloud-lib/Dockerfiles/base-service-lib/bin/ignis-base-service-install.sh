#!/bin/env bash
set -e

apt update
apt -y --no-install-recommends install \
  wget \
  unzip \
  ca-certificates \
  gnupg \
  lsb-release

rm -rf /var/lib/apt/lists/*

TERRAFORM_VERSION=1.8.5
wget -O /tmp/terraform.zip https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip
unzip /tmp/terraform.zip -d /usr/local/bin
rm /tmp/terraform.zip

terraform -version