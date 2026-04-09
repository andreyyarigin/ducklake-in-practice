#!/bin/sh
# init-rustfs.sh — создание бакетов в RustFS через AWS CLI.
# Запускается однократно сервисом init-rustfs в docker-compose.

ENDPOINT="http://rustfs:9000"
BUCKET="${RUSTFS_BUCKET:-ducklake-flights}"

echo "Waiting for RustFS to be ready..."
for i in $(seq 1 30); do
    if curl -sf "$ENDPOINT/minio/health/live" > /dev/null 2>&1; then
        echo "RustFS is ready."
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

echo "RustFS init complete."
