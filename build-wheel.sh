#!/bin/bash
#
# Build environment-specific wheel for TradingPythonAgent
#
# Usage:
#   ./build-wheel.sh [dev|staging|prod]
#
# If no environment is specified, defaults to 'dev'
# The wheel will be named: trading_agent-{env}-{version}.whl
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Get environment from argument or default to 'dev'
ENV="${1:-dev}"
ENV="${ENV,,}"  # Convert to lowercase

# Validate environment
if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
    log_warn "Invalid environment: $ENV. Defaulting to 'dev'"
    ENV="dev"
fi

log_info "Building wheel for environment: $ENV"

# Check if setuptools and wheel are installed
if ! python3 -c "import setuptools" 2>/dev/null; then
    log_warn "setuptools not found. Installing..."
    pip install setuptools
fi

if ! python3 -c "import wheel" 2>/dev/null; then
    log_warn "wheel not found. Installing..."
    pip install wheel
fi

# Create dist directory if it doesn't exist
mkdir -p "${PROJECT_ROOT}/dist"

# Clean previous builds (optional - remove if you want to keep old wheels)
log_info "Cleaning previous builds..."
rm -rf "${PROJECT_ROOT}/build" "${PROJECT_ROOT}/src/*.egg-info"
find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Build wheel with environment-specific name
log_info "Building wheel: trading_agent-${ENV}-*.whl"
cd "${PROJECT_ROOT}"
ENV="${ENV}" python3 setup.py bdist_wheel

# Find the generated wheel
WHEEL_FILE=$(find "${PROJECT_ROOT}/dist" -name "trading_agent-${ENV}-*.whl" | head -n 1)

if [ -n "${WHEEL_FILE}" ]; then
    log_info "✓ Wheel built successfully: $(basename "${WHEEL_FILE}")"
    log_info "  Location: ${WHEEL_FILE}"
    log_info "  Size: $(du -h "${WHEEL_FILE}" | cut -f1)"
else
    log_warn "⚠️  Wheel file not found in dist/ directory"
    exit 1
fi

# Display wheel contents (optional)
log_info "Wheel contents:"
unzip -l "${WHEEL_FILE}" | head -n 20

log_info "Build complete!"
