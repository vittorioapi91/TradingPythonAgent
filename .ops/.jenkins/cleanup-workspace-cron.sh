#!/bin/bash
#
# Wrapper script for Jenkins workspace cleanup
# Designed to be run from cron/systemd timer
# Logs output to file and sends summary to console
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLEANUP_SCRIPT="${SCRIPT_DIR}/cleanup-workspace.sh"
LOG_FILE="${SCRIPT_DIR}/cleanup.log"
PROJECT_ROOT="${SCRIPT_DIR}/../../.."

# Change to project root
cd "${PROJECT_ROOT}" || exit 1

# Run cleanup (keeps workspaces from last 1 day, last 3 per job)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting Jenkins workspace cleanup..." | tee -a "${LOG_FILE}"

"${CLEANUP_SCRIPT}" --keep-days=1 --keep-workspaces=3 2>&1 | tee -a "${LOG_FILE}"

EXIT_CODE=${PIPESTATUS[0]}
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cleanup finished with exit code: ${EXIT_CODE}" | tee -a "${LOG_FILE}"
echo "---" | tee -a "${LOG_FILE}"

# Trim log file to last 1000 lines (prevent it from growing too large)
tail -n 1000 "${LOG_FILE}" > "${LOG_FILE}.tmp" && mv "${LOG_FILE}.tmp" "${LOG_FILE}" 2>/dev/null || true

exit ${EXIT_CODE}
