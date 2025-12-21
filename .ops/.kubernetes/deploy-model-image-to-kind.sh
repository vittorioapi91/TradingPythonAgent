#!/usr/bin/env bash
# Load Docker image into kind cluster
# Usage: bash .ops/.kubernetes/load-model-image-to-kind.sh
#
# Prerequisites:
# - Docker image must be built first using: bash .ops/.kubernetes/build-model-image.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="hmm-model-training"
IMAGE_TAG="latest"
KIND_CLUSTER="trading-cluster"

# Check if image exists locally
if ! docker images --format "{{.Repository}}:{{.Tag}}" | grep -q "^${IMAGE_NAME}:${IMAGE_TAG}$"; then
  echo "Error: Docker image ${IMAGE_NAME}:${IMAGE_TAG} not found locally."
  echo "Please build the image first using:"
  echo "  bash .ops/.kubernetes/build-model-image.sh"
  exit 1
fi

# Check if kind cluster exists
if ! kind get clusters 2>/dev/null | grep -q "^${KIND_CLUSTER}$"; then
  echo "Error: kind cluster '${KIND_CLUSTER}' does not exist."
  echo "Please create the cluster first using:"
  echo "  bash .ops/.kubernetes/start-kubernetes.sh"
  exit 1
fi

echo "Loading image ${IMAGE_NAME}:${IMAGE_TAG} into kind cluster: ${KIND_CLUSTER}"
kind load docker-image "${IMAGE_NAME}:${IMAGE_TAG}" --name "${KIND_CLUSTER}"

echo "✓ Image loaded into kind cluster successfully"
echo "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "Ready to use in Kubernetes Jobs"

