#!/bin/bash
#
# Configure kind cluster to use local Docker registry
#
# This script configures a kind cluster to pull images from the local
# Docker registry running on localhost:5000.
#
# Usage:
#   ./configure-kind-registry.sh [cluster-name]
#
# Default cluster name: trading-cluster
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Cluster configuration
CLUSTER_NAME="${1:-trading-cluster}"
REGISTRY_HOST="localhost:5000"
REGISTRY_URL="http://${REGISTRY_HOST}"

# Check if kind is installed
if ! command -v kind &> /dev/null; then
    log_error "kind is not installed. Please install kind first."
    exit 1
fi

# Check if cluster exists
if ! kind get clusters | grep -q "^${CLUSTER_NAME}\$"; then
    log_error "Cluster '${CLUSTER_NAME}' does not exist"
    log_info "Create it first with: kind create cluster --name ${CLUSTER_NAME}"
    exit 1
fi

log_info "Configuring kind cluster '${CLUSTER_NAME}' to use local registry at ${REGISTRY_URL}"

# Get the kind network name
KIND_NETWORK="kind"

# Connect registry container to kind network (if not already connected)
if ! docker network inspect "${KIND_NETWORK}" 2>/dev/null | grep -q "docker-registry"; then
    log_info "Connecting registry container to kind network..."
    docker network connect "${KIND_NETWORK}" docker-registry 2>/dev/null || {
        log_warn "Could not connect registry to kind network (may already be connected)"
    }
fi

# Configure kind cluster to use local registry
# Create/update containerd config to allow insecure registry
log_info "Configuring containerd in kind cluster to use local registry..."

# Get the kind node container name
NODE_NAME="${CLUSTER_NAME}-control-plane"

# Create containerd config patch
cat <<EOF | docker exec -i "${NODE_NAME}" bash -c 'cat > /tmp/registry-config.toml'
[plugins."io.containerd.grpc.v1.cri".registry]
  [plugins."io.containerd.grpc.v1.cri".registry.mirrors]
    [plugins."io.containerd.grpc.v1.cri".registry.mirrors."${REGISTRY_HOST}"]
      endpoint = ["${REGISTRY_URL}"]
  [plugins."io.containerd.grpc.v1.cri".registry.configs]
    [plugins."io.containerd.grpc.v1.cri".registry.configs."${REGISTRY_HOST}".tls]
      insecure_skip_verify = true
EOF

# Apply the config
docker exec "${NODE_NAME}" cp /tmp/registry-config.toml /etc/containerd/config.toml

# Restart containerd to apply changes
log_info "Restarting containerd in kind node..."
docker exec "${NODE_NAME}" systemctl restart containerd || {
    # Fallback: restart the container
    log_warn "Could not restart containerd, restarting node container..."
    docker restart "${NODE_NAME}"
    sleep 5
}

log_info "✓ Kind cluster configured to use local registry"
log_info "Cluster '${CLUSTER_NAME}' can now pull images from ${REGISTRY_URL}"
