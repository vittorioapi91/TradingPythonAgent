#!/bin/bash
# Stop and clean up the local kind-based Kubernetes cluster and dashboard.
#
# This script is the counterpart to start-kubernetes.sh. It:
# - switches to the kind cluster context
# - deletes the kind cluster (stops all its containers)
# - stops any running `kubectl proxy` processes
#
# Usage (from project root or from .ops/.kubernetes):
#   bash .ops/.kubernetes/stop-kubernetes.sh

set -e

CLUSTER_NAME="trading-cluster"

echo "=== Stopping local kind Kubernetes cluster '${CLUSTER_NAME}' ==="

if ! command -v kind >/dev/null 2>&1; then
  echo "kind binary not found on PATH; nothing to delete."
else
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Deleting kind cluster '${CLUSTER_NAME}'..."
    kind delete cluster --name "${CLUSTER_NAME}"
  else
    echo "No kind cluster named '${CLUSTER_NAME}' found; nothing to delete."
  fi
fi

echo
echo "=== Stopping any running 'kubectl proxy' processes ==="
if pgrep -f "kubectl proxy" >/dev/null 2>&1; then
  pkill -f "kubectl proxy" || true
  echo "Stopped existing kubectl proxy processes."
else
  echo "No kubectl proxy processes found."
fi

echo
echo "Cleanup complete. If you are using Docker Desktop's built-in Kubernetes,"
echo "you can stop it from Docker Desktop's settings UI if needed."


