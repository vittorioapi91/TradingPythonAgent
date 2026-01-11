#!/usr/bin/env bash
# Quick setup script for Jenkins in Docker with access to Docker, kubectl, and kind
# Usage: bash .ops/.jenkins/setup-docker.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JENKINS_NAME="jenkins"
JENKINS_PORT="8080"
JENKINS_AGENT_PORT="50000"

echo "=== Jenkins Docker Setup ==="
echo "This script will set up Jenkins in Docker with access to:"
echo "  - Docker daemon (for building images)"
echo "  - kubectl (for Kubernetes operations)"
echo "  - kind (for loading images into kind cluster)"
echo ""

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if Jenkins container already exists
if docker ps -a --format "{{.Names}}" | grep -q "^${JENKINS_NAME}$"; then
    echo "Jenkins container '${JENKINS_NAME}' already exists."
    read -p "Do you want to remove it and create a new one? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Stopping and removing existing Jenkins container..."
        docker stop "${JENKINS_NAME}" 2>/dev/null || true
        docker rm "${JENKINS_NAME}" 2>/dev/null || true
    else
        echo "Using existing Jenkins container."
        echo "Start it with: docker start ${JENKINS_NAME}"
        exit 0
    fi
fi

# Check if kubectl and kind are available
KUBECTL_PATH=$(which kubectl 2>/dev/null || echo "")
KIND_PATH=$(which kind 2>/dev/null || echo "")

if [ -z "$KUBECTL_PATH" ]; then
    echo "Warning: kubectl not found in PATH. Jenkins won't be able to deploy to Kubernetes."
    echo "Install kubectl or update PATH before running Jenkins jobs."
fi

if [ -z "$KIND_PATH" ]; then
    echo "Warning: kind not found in PATH. Jenkins won't be able to load images into kind cluster."
    echo "Install kind or update PATH before running Jenkins jobs."
fi

# Create Jenkins container
echo "Creating Jenkins container..."
docker run -d \
    --name "${JENKINS_NAME}" \
    --restart unless-stopped \
    -p "${JENKINS_PORT}:8080" \
    -p "${JENKINS_AGENT_PORT}:50000" \
    -v jenkins_home:/var/jenkins_home \
    -v /var/run/docker.sock:/var/run/docker.sock \
    ${KUBECTL_PATH:+-v "$KUBECTL_PATH:/usr/local/bin/kubectl"} \
    ${KIND_PATH:+-v "$KIND_PATH:/usr/local/bin/kind"} \
    -v "$HOME/.kube:/var/jenkins_home/.kube:ro" \
    jenkins/jenkins:lts

# Wait for Jenkins to start
echo "Waiting for Jenkins to start..."
sleep 5

# Get initial admin password
echo ""
echo "=== Jenkins Setup Complete ==="
echo ""
echo "Jenkins is starting. Access it at: http://localhost:${JENKINS_PORT}"
echo ""
echo "Initial admin password:"
docker exec "${JENKINS_NAME}" cat /var/jenkins_home/secrets/initialAdminPassword 2>/dev/null || echo "   (Container is still starting, check with: docker exec ${JENKINS_NAME} cat /var/jenkins_home/secrets/initialAdminPassword)"
echo ""
echo "Next steps:"
echo "1. Open http://localhost:${JENKINS_PORT} in your browser"
echo "2. Enter the admin password above"
echo "3. Install suggested plugins"
echo "4. Create admin user"
echo "5. Configure Jenkins pipeline (see README_JENKINS.md)"
echo ""
echo "Useful commands:"
echo "  Start:   docker start ${JENKINS_NAME}"
echo "  Stop:    docker stop ${JENKINS_NAME}"
echo "  Logs:    docker logs -f ${JENKINS_NAME}"
echo "  Shell:   docker exec -it ${JENKINS_NAME} bash"
echo ""

