#!/usr/bin/env bash
# Launch a debug Pod in the hadron namespace.
#
# Usage:
#   launch_debug_pod.sh <k8s-context> [image-ref]
#
# The script will:
#   • Create a Pod named "debug-<timestamp>" in namespace "hadron" using the supplied
#     image-ref. If no image-ref is supplied it will reuse the image (and imagePullSecrets)
#     from the existing Deployment "hadron-cluster-coordinator" in the same namespace.
#   • Attach the ServiceAccount "hadron".
#   • Request/limit 4 CPUs and 16 GiB of RAM.
#   • Start the container in an endless sleep ("/bin/bash -c 'sleep infinity'") so that
#     human operators can exec into it.
#
# Prerequisites:
#   – kubectl must be installed and authenticated.
set -euo pipefail

if [[ ${#} -lt 1 ]]; then
  echo "Usage: $0 <k8s-context> [image-ref]" >&2
  exit 1
fi

CTX="$1"
IMAGE="${2:-}"
NAMESPACE="hadron"
DEPLOYMENT="hadron-cluster-coordinator"
SERVICE_ACCOUNT="hadron"
CPU_REQ="4"
MEM_REQ="16Gi"

# Determine if the context refers to Azure (case-insensitive substring match)
AZURE_MODE=0
# Convert the context name to lowercase once for an easy, readable comparison.
lower_ctx=$(printf '%s' "${CTX}" | tr '[:upper:]' '[:lower:]')
if [[ "${lower_ctx}" == *azure* ]]; then
  AZURE_MODE=1
fi

# Helper to run kubectl with the selected context and namespace
k() {
  kubectl --context "${CTX}" -n "${NAMESPACE}" "$@"
}

# Auto-detect image and imagePullSecrets from the deployment if image not provided
if [[ -z "${IMAGE}" ]]; then
  if ! k get deployment "${DEPLOYMENT}" >/dev/null 2>&1; then
    echo "Deployment '${DEPLOYMENT}' not found in namespace '${NAMESPACE}'. You must provide an image-ref." >&2
    exit 1
  fi
  IMAGE="$(k get deployment "${DEPLOYMENT}" -o jsonpath='{.spec.template.spec.containers[0].image}')"
fi

# Fetch the first imagePullSecret name from the coordinator deployment
PULL_SECRET=""
if k get deployment "${DEPLOYMENT}" >/dev/null 2>&1; then
  PULL_SECRET="$(k get deployment "${DEPLOYMENT}" -o jsonpath='{.spec.template.spec.imagePullSecrets[0].name}')"
fi

POD_NAME="debug-$(date +%s)"
TMPFILE="$(mktemp /tmp/${POD_NAME}.yaml.XXXX)"

# Azure-specific YAML snippets for authentication with blob storage. AWS doesn't need this as it's all handled
# through the k8s ServiceAccount and EKS OIDC.
AZURE_VOLUME_YAML=""
AZURE_MOUNT_YAML=""
AZURE_ENV_YAML=""

if [[ ${AZURE_MODE} -eq 1 ]]; then
  read -r -d '' AZURE_VOLUME_YAML <<'YAML'
volumes:
  - name: brickstore-hadron-storage-account-service-principal-secret
    secret:
      defaultMode: 420
      secretName: brickstore-hadron-storage-account-service-principal-secret
YAML

  read -r -d '' AZURE_MOUNT_YAML <<'YAML'
    volumeMounts:
      - mountPath: /databricks/secrets/azure-service-principal
        name: brickstore-hadron-storage-account-service-principal-secret
        readOnly: true
YAML

  read -r -d '' AZURE_ENV_YAML <<'YAML'
    env:
      - name: AZURE_CLIENT_ID
        valueFrom:
          secretKeyRef:
            key: client-id
            name: brickstore-hadron-storage-account-service-principal-secret
            optional: false
      - name: AZURE_CLIENT_CERTIFICATE_PATH
        value: /databricks/secrets/azure-service-principal/client.pem
YAML
fi

# Build full manifest in one shot for readability.
cat >"${TMPFILE}" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: hadron-debug
spec:
  restartPolicy: Never
  serviceAccountName: ${SERVICE_ACCOUNT}
  imagePullSecrets:
  - name: ${PULL_SECRET}
${AZURE_VOLUME_YAML}
  containers:
  - name: debug
    image: ${IMAGE}
${AZURE_MOUNT_YAML}
${AZURE_ENV_YAML}
    command: ["/bin/bash", "-c", "sleep infinity"]
    resources:
      requests:
        cpu: "${CPU_REQ}"
        memory: "${MEM_REQ}"
      limits:
        cpu: "${CPU_REQ}"
        memory: "${MEM_REQ}"
EOF

# Apply and clean up
k apply -f "${TMPFILE}"

printf "\nDebug Pod '%s' created in namespace '%s'.\n" "${POD_NAME}" "${NAMESPACE}"
printf "Exec into the Pod with:\n  kubectl --context %s -n %s exec -it %s -- /bin/bash\n" "${CTX}" "${NAMESPACE}" "${POD_NAME}"

rm -f "${TMPFILE}"
