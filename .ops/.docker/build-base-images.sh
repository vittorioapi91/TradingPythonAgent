#!/bin/bash
#
# Build base Docker images for infra-platform and trading_agent
# These base images are built once and reused for all incremental builds
#
# Usage:
#   ./build-base-images.sh [jenkins|trading-agent|all]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../.."

cd "${PROJECT_ROOT}" || exit 1

BUILD_JENKINS=true
BUILD_TRADING_AGENT=true

# Parse arguments
if [ $# -gt 0 ]; then
    BUILD_JENKINS=false
    BUILD_TRADING_AGENT=false
    case "$1" in
        jenkins)
            BUILD_JENKINS=true
            ;;
        trading-agent)
            BUILD_TRADING_AGENT=true
            ;;
        all)
            BUILD_JENKINS=true
            BUILD_TRADING_AGENT=true
            ;;
        *)
            echo "Usage: $0 [jenkins|trading-agent|all]"
            echo "  jenkins       - Build only jenkins-custom:base"
            echo "  trading-agent - Build only hmm-model-training-base:base"
            echo "  all           - Build both base images (default if no argument)"
            exit 1
            ;;
    esac
fi

echo "Building Base Docker Images"
echo "==========================="
echo ""

# Build Jenkins base image
if [ "${BUILD_JENKINS}" = "true" ]; then
    echo "Building Jenkins base image: jenkins-custom:base..."
    echo "This may take 10-20 minutes (installs tools, dependencies)..."
    
    if docker build \
        --platform linux/amd64 \
        -f .ops/.docker/Dockerfile.jenkins.base \
        -t jenkins-custom:base \
        .ops/.docker/; then
        echo ""
        echo "✓ Jenkins base image built successfully: jenkins-custom:base"
        docker images jenkins-custom:base --format "  Size: {{.Size}}"
    else
        echo ""
        echo "❌ Failed to build Jenkins base image"
        exit 1
    fi
    echo ""
fi

# Build trading agent base image
if [ "${BUILD_TRADING_AGENT}" = "true" ]; then
    echo "Building trading agent base image: hmm-model-training-base:base..."
    echo "This may take 5-15 minutes (installs Python dependencies)..."
    
    if docker build \
        --platform linux/amd64 \
        -f .ops/.kubernetes/Dockerfile.model-training.base \
        -t hmm-model-training-base:base \
        .; then
        echo ""
        echo "✓ Trading agent base image built successfully: hmm-model-training-base:base"
        docker images hmm-model-training-base:base --format "  Size: {{.Size}}"
    else
        echo ""
        echo "❌ Failed to build trading agent base image"
        exit 1
    fi
    echo ""
fi

echo "✓ All base images built successfully!"
echo ""
echo "These base images will be used by incremental builds in pipelines."
echo "Base images are protected from cleanup (never deleted)."
echo ""
echo "To verify base images exist:"
echo "  docker images | grep ':base'"
