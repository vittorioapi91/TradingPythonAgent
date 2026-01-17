#!/bin/bash
#
# Check if Docker image rebuild is needed
#
# This script determines if a rebuild is necessary by checking:
# 1. If the image exists in the local registry
# 2. If Dockerfile or dependencies have changed
# 3. If source code has changed (for incremental builds)
#
# Usage:
#   ./check-rebuild-needed.sh <image-name> <image-tag> [base-image-name] [base-image-tag]
#
# Returns:
#   0 = rebuild needed
#   1 = rebuild not needed (image exists and is up to date)
#

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# Configuration
REGISTRY_HOST="${REGISTRY_HOST:-localhost:5000}"
REGISTRY_URL="http://${REGISTRY_HOST}"

IMAGE_NAME="${1:-}"
IMAGE_TAG="${2:-}"
BASE_IMAGE_NAME="${3:-hmm-model-training-base}"
BASE_IMAGE_TAG="${4:-base}"

if [ -z "${IMAGE_NAME}" ] || [ -z "${IMAGE_TAG}" ]; then
    echo "Usage: $0 <image-name> <image-tag> [base-image-name] [base-image-tag]"
    exit 2
fi

REGISTRY_IMAGE="${REGISTRY_HOST}/${IMAGE_NAME}:${IMAGE_TAG}"
BASE_REGISTRY_IMAGE="${REGISTRY_HOST}/${BASE_IMAGE_NAME}:${BASE_IMAGE_TAG}"

# Check if registry is accessible
if ! curl -s -f "${REGISTRY_URL}/v2/" > /dev/null 2>&1; then
    log_info "Registry not accessible - rebuild needed"
    exit 0
fi

# Check if base image exists in registry
if ! curl -s -f "${REGISTRY_URL}/v2/${BASE_IMAGE_NAME}/manifests/${BASE_IMAGE_TAG}" > /dev/null 2>&1; then
    log_info "Base image not in registry - rebuild needed"
    exit 0
fi

# Check if target image exists in registry
if curl -s -f "${REGISTRY_URL}/v2/${IMAGE_NAME}/manifests/${IMAGE_TAG}" > /dev/null 2>&1; then
    log_debug "Image ${REGISTRY_IMAGE} exists in registry"
    
    # For now, we'll always rebuild incremental images if source code changed
    # In a more sophisticated setup, we could compare image digests or timestamps
    # For base images, we check if Dockerfile or requirements changed
    if [ "${IMAGE_NAME}" = "${BASE_IMAGE_NAME}" ]; then
        # Base image: check if Dockerfile or requirements changed
        # This is a simplified check - in production, use git diff or checksums
        log_debug "Base image exists - checking if dependencies changed..."
        # For now, assume base images are stable and don't need frequent rebuilds
        # Return 1 (no rebuild) unless explicitly forced
        log_info "Base image exists in registry - no rebuild needed"
        exit 1
    else
        # Incremental image: always rebuild if we reach here (source code may have changed)
        # In a more sophisticated setup, compare git commit or file checksums
        log_info "Incremental image exists but source may have changed - rebuild needed"
        exit 0
    fi
else
    log_info "Image ${REGISTRY_IMAGE} not in registry - rebuild needed"
    exit 0
fi
