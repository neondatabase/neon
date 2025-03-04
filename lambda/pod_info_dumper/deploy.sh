#!/usr/bin/env bash
set -euo pipefail

aws lambda delete-function \
 --function-name "${LAMBDA_NAME}" || echo "function not found"

exec aws lambda create-function \
 --function-name "${LAMBDA_NAME}" \
 --runtime "provided.al2023" \
 --handler bootstrap \
 --role "arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_ROLE}" \
 --zip-file fileb://./target/lambda/pod_info_dumper/bootstrap.zip \
 --environment Variables="{
   NEON_S3_BUCKET_REGION=${NEON_S3_BUCKET_REGION},
   NEON_S3_BUCKET_NAME=${NEON_S3_BUCKET_NAME},
   NEON_S3_BUCKET_OWNER=${NEON_S3_BUCKET_OWNER},
   NEON_EKS_CLUSTER_REGION=${NEON_EKS_CLUSTER_REGION},
   NEON_EKS_CLUSTER_NAME=${NEON_EKS_CLUSTER_NAME},
   NEON_K8S_CLIENT_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${LAMBDA_ROLE}",
   KUBECONFIG=/tmp/kubeconfig
 }" \
 --region "${LAMBDA_REGION}"

# exec aws lambda update-function-code \
#   --function-name "${LAMBDA_NAME}" \
#   --zip-file fileb://./target/lambda/pod_info_dumper/bootstrap.zip
