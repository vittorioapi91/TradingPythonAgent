#!/bin/bash
#
# Copy wheels from dist/ to .ops/.airflow/wheels/
# This script helps sync built wheels to the Airflow wheels directory
#
# Usage:
#   ./install-wheel.sh [dev|staging|prod]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

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

# Create wheels directory if it doesn't exist
WHEELS_DIR="${SCRIPT_DIR}/wheels"
mkdir -p "${WHEELS_DIR}"

# Find the latest wheel for this environment
WHEEL_FILE=$(find "${PROJECT_ROOT}/dist" -name "trading_agent-${ENV}-*.whl" 2>/dev/null | sort -V | tail -n 1)

if [ -z "${WHEEL_FILE}" ]; then
    log_warn "No wheel found for environment '${ENV}' in ${PROJECT_ROOT}/dist/"
    log_warn "Please build the wheel first: ./build-wheel.sh ${ENV}"
    exit 1
fi

# Copy wheel to Airflow wheels directory
WHEEL_NAME=$(basename "${WHEEL_FILE}")
TARGET="${WHEELS_DIR}/${WHEEL_NAME}"

log_info "Copying wheel for environment: ${ENV}"
log_info "  From: ${WHEEL_FILE}"
log_info "  To:   ${TARGET}"

cp "${WHEEL_FILE}" "${TARGET}"

log_info "✓ Wheel installed: ${WHEEL_NAME}"
log_info "  Airflow will install this wheel on startup"

# List all wheels in the directory
log_info "Available wheels in ${WHEELS_DIR}:"
ls -lh "${WHEELS_DIR}"/trading_agent-*.whl 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}' || log_warn "  No wheels found"
