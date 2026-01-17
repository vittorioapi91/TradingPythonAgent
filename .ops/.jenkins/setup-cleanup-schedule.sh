#!/bin/bash
#
# Setup daily Jenkins workspace cleanup at 2 AM
# Creates a LaunchAgent (macOS) to run cleanup daily
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../../.."
PLIST_NAME="com.tradingagent.jenkins-cleanup"
PLIST_FILE="${HOME}/Library/LaunchAgents/${PLIST_NAME}.plist"

echo "Setting up Jenkins workspace cleanup schedule (daily at 2 AM)..."

# Create plist content
cat > "${PLIST_FILE}" << PLIST_EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_NAME}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>${PROJECT_ROOT}/.ops/.jenkins/cleanup-workspace-cron.sh</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>RunAtLoad</key>
    <false/>
    <key>StandardOutPath</key>
    <string>${PROJECT_ROOT}/.ops/.jenkins/cleanup-cron.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>${PROJECT_ROOT}/.ops/.jenkins/cleanup-cron.stderr.log</string>
</dict>
</plist>
PLIST_EOF

echo "✓ Created LaunchAgent plist: ${PLIST_FILE}"

# Unload if already loaded
launchctl list | grep -q "${PLIST_NAME}" && {
    echo "Unloading existing agent..."
    launchctl unload "${PLIST_FILE}" 2>/dev/null || true
}

# Load the agent
echo "Loading LaunchAgent..."
launchctl load "${PLIST_FILE}"

echo ""
echo "✓ Jenkins workspace cleanup scheduled successfully!"
echo ""
echo "Schedule: Daily at 2:00 AM"
echo "Config: Keep workspaces from last 1 day (everything older than yesterday is deleted)"
echo "Logs: ${PROJECT_ROOT}/.ops/.jenkins/cleanup.log"
echo ""
echo "To check status:"
echo "  launchctl list | grep ${PLIST_NAME}"
echo ""
echo "To unload (stop):"
echo "  launchctl unload ${PLIST_FILE}"
echo ""
echo "To reload after changes:"
echo "  launchctl unload ${PLIST_FILE} && launchctl load ${PLIST_FILE}"
