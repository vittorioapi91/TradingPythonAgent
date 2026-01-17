#!/bin/bash
#
# Push base Docker images to local registry
#
# This script builds and pushes base images to the local Docker registry
# so they can be reused by kind clusters without rebuilding every time.
#
# Usage:
#   ./push-base-images.sh [--rebuild]
#
# Options:
#   --rebuild    Force rebuild of base images even if they exist locally
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Registry configuration
REGISTRY_HOST="${REGISTRY_HOST:-localhost:5000}"
REGISTRY_URL="http://${REGISTRY_HOST}"

# Base image configuration
BASE_IMAGE_NAME="hmm-model-training-base"
BASE_IMAGE_TAG="base"
BASE_IMAGE_FULL="${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}"
REGISTRY_IMAGE="${REGISTRY_HOST}/${BASE_IMAGE_FULL}"

# Check if --rebuild flag is set
REBUILD=false
if [[ "${1:-}" == "--rebuild" ]]; then
    REBUILD=true
    log_info "Rebuild flag set - will rebuild base images"
fi

# Check if registry is running
log_info "Checking if local registry is running at ${REGISTRY_URL}..."
if ! curl -s -f "${REGISTRY_URL}/v2/" > /dev/null 2>&1; then
    log_error "Registry is not accessible at ${REGISTRY_URL}"
    log_info "Start the registry with: docker-compose -f .ops/.docker/docker-compose.registry.yml up -d"
    exit 1
fi
log_info "✓ Registry is running"

# Check if base image exists locally
if docker images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${BASE_IMAGE_FULL}\$"; then
    if [ "${REBUILD}" = "false" ]; then
        log_info "Base image ${BASE_IMAGE_FULL} already exists locally"
        log_info "Skipping build (use --rebuild to force rebuild)"
    else
        log_info "Rebuilding base image ${BASE_IMAGE_FULL}..."
        docker rmi "${BASE_IMAGE_FULL}" 2>/dev/null || true
    fi
else
    log_info "Base image ${BASE_IMAGE_FULL} not found locally"
fi

# Build base image if it doesn't exist or rebuild was requested
if ! docker images --format '{{.Repository}}:{{.Tag}}' | grep -q "^${BASE_IMAGE_FULL}\$"; then
    log_info "Building base image: ${BASE_IMAGE_FULL}"
    log_info "Dockerfile: .ops/.kubernetes/Dockerfile.model-training.base"
    
    cd "${PROJECT_ROOT}"
    docker build \
        --platform linux/amd64 \
        -f .ops/.kubernetes/Dockerfile.model-training.base \
        -t "${BASE_IMAGE_FULL}" \
        .
    
    log_info "✓ Base image built successfully"
else
    log_info "Using existing base image: ${BASE_IMAGE_FULL}"
fi

# Tag image for registry
log_info "Tagging image for registry: ${REGISTRY_IMAGE}"
docker tag "${BASE_IMAGE_FULL}" "${REGISTRY_IMAGE}"

# Push to registry
log_info "Pushing ${REGISTRY_IMAGE} to local registry..."
docker push "${REGISTRY_IMAGE}"

log_info "✓ Base image pushed to registry: ${REGISTRY_IMAGE}"

# Verify image is in registry
log_info "Verifying image in registry..."
if curl -s -f "${REGISTRY_URL}/v2/${BASE_IMAGE_NAME}/manifests/${BASE_IMAGE_TAG}" > /dev/null 2>&1; then
    log_info "✓ Image verified in registry"
else
    log_warn "Could not verify image in registry (this may be normal for first push)"
fi

log_info "Done! Base images are now available in local registry at ${REGISTRY_URL}"
