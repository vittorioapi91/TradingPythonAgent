#!/usr/bin/env bash
set -euo pipefail

# Simple helper to bootstrap a local Kubernetes-in-Docker cluster (via kind)
# and install the Kubernetes Dashboard.
#
# This script is intentionally conservative:
# - It only ensures a kind cluster exists.
# - It installs/updates the dashboard and admin user.
# - It prints the exact commands to get a login token and run kubectl proxy.
#
# It does NOT:
# - Start Docker Desktop for you.
# - Block on kubectl proxy (you run that separately).
#
# Usage (from project root or from .ops/.kubernetes):
#   bash .ops/.kubernetes/start-kubernetes.sh
#

CLUSTER_NAME="trading-cluster"
DASHBOARD_VERSION="v2.7.0"
DASHBOARD_YAML_URL="https://raw.githubusercontent.com/kubernetes/dashboard/${DASHBOARD_VERSION}/aio/deploy/recommended.yaml"
MONITORING_STACK_YAML=".ops/.kubernetes/monitoring-stack.yaml"

echo "=== Kubernetes bootstrap ==="
echo "Cluster name       : ${CLUSTER_NAME}"
echo "Dashboard manifest : ${DASHBOARD_YAML_URL}"
echo "Monitoring stack   : ${MONITORING_STACK_YAML} (Prometheus, Grafana, MLflow, Airflow, Feast, Postgres – apply manually with kubectl if you want them in the cluster)"
echo
echo "=== Checking prerequisites (kubectl, kind) ==="

if ! command -v kubectl >/dev/null 2>&1; then
  echo "Error: kubectl is not installed or not on PATH."
  echo "On macOS with Homebrew: brew install kubectl"
  exit 1
fi

if ! command -v kind >/dev/null 2>&1; then
  echo "Error: kind is not installed or not on PATH."
  echo "On macOS with Homebrew: brew install kind"
  exit 1
fi

echo "=== Ensuring kind cluster '${CLUSTER_NAME}' exists ==="
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "kind cluster '${CLUSTER_NAME}' already exists."
else
  echo "Creating kind cluster '${CLUSTER_NAME}' (this may take a minute)..."
  kind create cluster --name "${CLUSTER_NAME}"
fi

echo "=== Setting kubectl context to kind-${CLUSTER_NAME} ==="
kubectl config use-context "kind-${CLUSTER_NAME}" >/dev/null

echo "=== Verifying cluster is reachable ==="
kubectl cluster-info
kubectl get nodes

echo "=== Installing / updating Kubernetes Dashboard (${DASHBOARD_VERSION}) ==="
echo "Applying: ${DASHBOARD_YAML_URL}"
kubectl apply -f "${DASHBOARD_YAML_URL}"

echo "=== Enabling skip-login for Dashboard ==="
# Wait a moment for the deployment to be created
sleep 2

# Patch the deployment to enable skip-login
kubectl patch deployment kubernetes-dashboard -n kubernetes-dashboard --type='json' -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/args/-",
    "value": "--enable-skip-login"
  },
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/args/-",
    "value": "--enable-insecure-login"
  }
]' 2>/dev/null || {
  # If patch fails (deployment might not exist yet), create a patch file for later
  echo "Note: Dashboard deployment patching may need to be done manually"
  echo "Run: kubectl patch deployment kubernetes-dashboard -n kubernetes-dashboard --type='json' -p='[{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--enable-skip-login\"},{\"op\":\"add\",\"path\":\"/spec/template/spec/containers/0/args/-\",\"value\":\"--enable-insecure-login\"}]'"
}

echo "=== Creating admin user and cluster-admin binding (if not present) ==="
cat << 'EOF' | kubectl apply -f -
apiVersion: v1
kind: ServiceAccount
metadata:
  name: admin-user
  namespace: kubernetes-dashboard
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-user-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: admin-user
  namespace: kubernetes-dashboard
EOF

echo
echo "Generating Dashboard admin-user token (for non–skip-login setups)..."
ADMIN_TOKEN="$(kubectl -n kubernetes-dashboard create token admin-user 2>/dev/null || true)"
if [ -n "$ADMIN_TOKEN" ]; then
  echo "Dashboard admin-user token:"
  echo "$ADMIN_TOKEN"
else
  echo "Could not generate token automatically (Dashboard API may not be ready yet)."
fi

echo
echo "=== Kubernetes cluster and Dashboard are set up ==="
echo
echo "Starting 'kubectl proxy' on port 8001 in the background..."
if pgrep -f "kubectl proxy" >/dev/null 2>&1; then
  echo "kubectl proxy already running; leaving existing process in place."
else
  nohup kubectl proxy --port=8001 >/dev/null 2>&1 &
  echo "kubectl proxy started."
fi

echo
echo "Next steps:"
echo "1) (Optional) Get a dashboard login token (if you haven't enabled skip-login):"
echo "   kubectl -n kubernetes-dashboard create token admin-user"
echo
echo "2) Open this URL in your browser:"
echo "   http://127.0.0.1:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"
echo
echo "You can re-run this script any time to ensure the cluster, dashboard, and proxy are running."


