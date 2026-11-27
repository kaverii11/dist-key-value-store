#!/bin/bash
set -eux
# 2 GB swap so the Go/Docker build has headroom
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
dnf install -y docker git
systemctl enable --now docker
usermod -aG docker ec2-user
mkdir -p /usr/local/lib/docker/cli-plugins
curl -fsSL https://github.com/docker/compose/releases/download/v2.39.1/docker-compose-linux-x86_64 -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
cd /home/ec2-user && sudo -u ec2-user git clone https://github.com/kaverii11/dist-key-value-store.git
cd dist-key-value-store && docker compose -f deploy/docker-compose.yml up -d --build
touch /home/ec2-user/READY
