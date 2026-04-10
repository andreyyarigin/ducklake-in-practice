#!/bin/sh
# init-minio.sh — создание бакетов в MinIO через AWS CLI.
# Запускается однократно сервисом init-minio в docker-compose.

ENDPOINT="http://minio:9000"
BUCKET="${MINIO_BUCKET:-ducklake-flights}"

echo "Waiting for MinIO to be ready..."
for i in $(seq 1 30); do
    if curl -sf "$ENDPOINT/minio/health/live" > /dev/null 2>&1; then
        echo "MinIO is ready."
        break
    fi
    echo "  attempt $i/30..."
    sleep 2
done

echo "Creating bucket: $BUCKET"
aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null && \
    echo "Bucket $BUCKET created." || \
    echo "Bucket $BUCKET already exists, skipping."

echo "Verifying bucket list:"
aws --endpoint-url "$ENDPOINT" s3 ls

echo "MinIO init complete."
