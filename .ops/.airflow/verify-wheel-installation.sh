#!/bin/bash
#
# Verify and install trading_agent wheel in Airflow container
#
# This script helps verify if the wheel is installed and provides
# instructions for manual installation if needed.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WHEELS_DIR="${SCRIPT_DIR}/wheels"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# Check if wheel files exist
log_info "Checking for wheel files in ${WHEELS_DIR}..."
if [ ! -d "${WHEELS_DIR}" ]; then
    log_error "Wheels directory does not exist: ${WHEELS_DIR}"
    exit 1
fi

WHEEL_FILES=$(find "${WHEELS_DIR}" -name "trading_agent*.whl" 2>/dev/null | sort)

if [ -z "${WHEEL_FILES}" ]; then
    log_warn "No wheel files found in ${WHEELS_DIR}"
    log_info "To install a wheel:"
    log_info "  1. Build the wheel: ./build-wheel.sh dev"
    log_info "  2. Install it: .ops/.airflow/install-wheel.sh dev"
    exit 1
fi

log_info "Found wheel files:"
while IFS= read -r wheel_file; do
    if [ -n "${wheel_file}" ]; then
        wheel_name=$(basename "${wheel_file}")
        wheel_size=$(du -h "${wheel_file}" | cut -f1)
        log_info "  ✓ ${wheel_name} (${wheel_size})"
    fi
done <<< "${WHEEL_FILES}"

# Check if running in Airflow container
if [ -n "${AIRFLOW_HOME:-}" ] || [ -f "/opt/airflow/airflow.cfg" ] || [ -d "/opt/airflow" ]; then
    log_info "Running in Airflow container environment"
    
    # Check if package is installed
    log_info "Checking if trading_agent package is installed..."
    if python3 -c "import trading_agent" 2>/dev/null; then
        log_info "✓ trading_agent package is importable"
        
        # Try to get version
        VERSION=$(python3 -c "import trading_agent; print(getattr(trading_agent, '__version__', 'unknown'))" 2>/dev/null || echo "unknown")
        log_info "  Version: ${VERSION}"
    else
        log_warn "✗ trading_agent package is NOT importable"
        log_info "Installing wheel from /opt/airflow/wheels..."
        
        # Find the latest wheel
        LATEST_WHEEL=$(find /opt/airflow/wheels -name "trading_agent*.whl" 2>/dev/null | sort -V | tail -n 1)
        
        if [ -n "${LATEST_WHEEL}" ]; then
            log_info "Installing: $(basename "${LATEST_WHEEL}")"
            pip install --force-reinstall --no-deps "${LATEST_WHEEL}" || {
                log_error "Failed to install wheel"
                exit 1
            }
            log_info "✓ Wheel installed successfully"
        else
            log_error "No wheel files found in /opt/airflow/wheels"
            log_info "Make sure the wheels directory is mounted correctly"
            exit 1
        fi
    fi
    
    # Check installed packages
    log_info "Checking installed trading_agent packages..."
    python3 -c "
import importlib.metadata
try:
    dists = list(importlib.metadata.distributions())
    trading_agent_dists = [d for d in dists if 'trading_agent' in d.metadata.get('Name', '').lower()]
    if trading_agent_dists:
        print('Installed packages:')
        for d in trading_agent_dists:
            print(f\"  - {d.metadata['Name']} {d.version}\")
    else:
        print('  No trading_agent packages found in installed distributions')
except Exception as e:
    print(f'Error checking distributions: {e}')
" || log_warn "Could not check installed distributions"
    
else
    log_info "Not running in Airflow container"
    log_info "To verify installation in Airflow:"
    log_info "  1. Restart Airflow container: docker restart airflow-dev"
    log_info "  2. Or exec into container and run this script:"
    log_info "     docker exec -it airflow-dev bash -c '.ops/.airflow/verify-wheel-installation.sh'"
fi

log_info "Done!"
