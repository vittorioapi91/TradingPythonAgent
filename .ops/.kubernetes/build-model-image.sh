#!/usr/bin/env bash
# Build Docker image for HMM model training
# Usage: bash .ops/.kubernetes/build-model-image.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IMAGE_NAME="hmm-model-training"
IMAGE_TAG="latest"

cd "$PROJECT_ROOT"

echo "Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
docker build \
  -f .ops/.kubernetes/Dockerfile.model-training \
  -t "${IMAGE_NAME}:${IMAGE_TAG}" \
  .

echo "✓ Image built successfully"
echo "Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "To load the image into kind cluster, run:"
echo "  bash .ops/.kubernetes/load-model-image-to-kind.sh"

