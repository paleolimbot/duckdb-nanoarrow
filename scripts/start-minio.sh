#!/usr/bin/env bash
# Starts a fresh S3 server (SeaweedFS) with an empty mybucket for test/configs/minio.json
set -euxo pipefail

cd "$(dirname "$0")"

docker compose down --volumes --remove-orphans
docker compose up --detach minio
# Runs in the foreground, so the bucket exists once this returns
docker compose run --rm mc
