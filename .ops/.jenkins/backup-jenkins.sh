#!/bin/bash
#
# Jenkins Backup Script
# Creates a backup of the current Jenkins configuration including:
# - All plugins
# - Credentials
# - Pipeline configuration
# - User data
# - System configuration
#
# Usage: ./backup-jenkins.sh [backup-name]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JENKINS_DATA_DIR="${SCRIPT_DIR}/data"
BACKUP_DIR="${SCRIPT_DIR}/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_NAME="${1:-jenkins_backup_${TIMESTAMP}}"
BACKUP_FILE="${BACKUP_DIR}/${BACKUP_NAME}.tar.gz"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

# Check if Jenkins data exists
if [ ! -d "${JENKINS_DATA_DIR}" ]; then
    log_error "Jenkins data directory not found: ${JENKINS_DATA_DIR}"
    exit 1
fi

# Create backup directory
mkdir -p "${BACKUP_DIR}"

log_info "Creating Jenkins backup..."
log_info "Source: ${JENKINS_DATA_DIR}"
log_info "Destination: ${BACKUP_FILE}"

# Create backup archive
tar -czf "${BACKUP_FILE}" \
    -C "${SCRIPT_DIR}" \
    --exclude='data/logs' \
    --exclude='data/workspace' \
    --exclude='data/jobs/*/workspace' \
    --exclude='data/jobs/*/builds/*/log' \
    --exclude='data/jobs/*/builds/*/workflow-completed' \
    --exclude='data/jobs/*/builds/*/build.xml' \
    --exclude='data/jobs/*/builds/*/changelog*.xml' \
    data

if [ $? -eq 0 ]; then
    BACKUP_SIZE=$(du -h "${BACKUP_FILE}" | cut -f1)
    log_info "Backup created successfully!"
    log_info "Backup file: ${BACKUP_FILE}"
    log_info "Backup size: ${BACKUP_SIZE}"
    
    # List what's included
    log_info "Backup includes:"
    tar -tzf "${BACKUP_FILE}" | head -20 | sed 's/^/  - /'
    local total_files=$(tar -tzf "${BACKUP_FILE}" | wc -l | tr -d ' ')
    log_info "  ... and $(($total_files - 20)) more files"
    
    # Count plugins
    local plugin_count=$(tar -tzf "${BACKUP_FILE}" | grep -c '\.jpi$' || echo "0")
    log_info "Plugins in backup: ${plugin_count}"
    
    # Check for pipeline config
    if tar -tzf "${BACKUP_FILE}" | grep -q 'jobs/TradingPythonAgent/config.xml'; then
        log_info "Pipeline configuration: Included"
    else
        log_warn "Pipeline configuration: Not found"
    fi
    
    # Check for credentials
    if tar -tzf "${BACKUP_FILE}" | grep -q 'credentials\.xml\|secrets/'; then
        log_info "Credentials: Included"
    else
        log_warn "Credentials: Not found"
    fi
    
    log_info "Backup completed successfully!"
else
    log_error "Backup failed!"
    exit 1
fi
