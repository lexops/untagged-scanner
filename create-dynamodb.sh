#!/usr/bin/env bash
set -euo pipefail
trap 's=$?; echo >&2 "$0: Error on line $LINENO: $BASH_COMMAND"; exit $s' ERR

export AWS_PAGER=""

TABLE_NAME="${1:-UntaggedResources}"
HASH_KEY="ARN"
RANGE_KEY="AccountId"
TTL_ATTR="ExpireAt"

echo "Creating DynamoDB table '${TABLE_NAME}' ..."

aws dynamodb create-table \
  --table-name "${TABLE_NAME}" \
  --attribute-definitions \
      AttributeName=${HASH_KEY},AttributeType=S \
      AttributeName=${RANGE_KEY},AttributeType=S \
  --key-schema \
      AttributeName=${HASH_KEY},KeyType=HASH \
      AttributeName=${RANGE_KEY},KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \

echo "Waiting for table to become ACTIVE..."
aws dynamodb wait table-exists --table-name "${TABLE_NAME}"

echo "Enabling TTL on attribute '${TTL_ATTR}' ..."
aws dynamodb update-time-to-live \
  --table-name "${TABLE_NAME}" \
  --time-to-live-specification "Enabled=true,AttributeName=${TTL_ATTR}"

echo "Table '${TABLE_NAME}' is ready (PK: ${HASH_KEY}, SK: ${RANGE_KEY}, TTL: ${TTL_ATTR})."