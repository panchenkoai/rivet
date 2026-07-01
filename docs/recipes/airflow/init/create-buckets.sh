#!/bin/sh
set -e
until mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; do
  echo "waiting for minio…"; sleep 2
done
mc mb -p local/rivet-lake || true
echo "MinIO bucket rivet-lake ready"
until wget -q -O- http://fake-gcs:4443/storage/v1/b >/dev/null 2>&1; do
  echo "waiting for fake-gcs…"; sleep 2
done
wget -q -O- --post-data='{"name":"rivet-lake-gcs"}' --header='Content-Type: application/json' \
  http://fake-gcs:4443/storage/v1/b >/dev/null 2>&1 || true
echo "fake-gcs bucket rivet-lake-gcs ready"
