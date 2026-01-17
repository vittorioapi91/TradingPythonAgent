#!/bin/bash
# Fix Jenkins reverse proxy configuration
# Sets Jenkins root URL to work with nginx HTTPS proxy

set -euo pipefail

JENKINS_DATA_DIR="${1:-.ops/.jenkins/data}"
LOCATION_CONFIG="${JENKINS_DATA_DIR}/jenkins.model.JenkinsLocationConfiguration.xml"

if [ ! -f "$LOCATION_CONFIG" ]; then
    echo "Creating Jenkins location configuration file..."
    mkdir -p "$(dirname "$LOCATION_CONFIG")"
fi

echo "Fixing Jenkins reverse proxy configuration..."

# Backup config if it exists
if [ -f "$LOCATION_CONFIG" ]; then
    cp "$LOCATION_CONFIG" "${LOCATION_CONFIG}.backup.$(date +%Y%m%d_%H%M%S)"
fi

# Use Python to update the location config file (more reliable than sed for XML)
python3 <<PYTHON_EOF
import xml.etree.ElementTree as ET
import sys
import os

config_file = "${LOCATION_CONFIG}"

try:
    # Parse existing file or create new root
    if os.path.exists(config_file):
        tree = ET.parse(config_file)
        root = tree.getroot()
    else:
        root = ET.Element('jenkins.model.JenkinsLocationConfiguration')
        tree = ET.ElementTree(root)
    
    # Set jenkinsUrl
    jenkins_url = root.find('jenkinsUrl')
    if jenkins_url is None:
        jenkins_url = ET.SubElement(root, 'jenkinsUrl')
    jenkins_url.text = 'https://jenkins.local.info/'
    
    # Set adminAddress (optional but recommended)
    admin_addr = root.find('adminAddress')
    if admin_addr is None:
        admin_addr = ET.SubElement(root, 'adminAddress')
    admin_addr.text = 'admin@jenkins.local.info'
    
    # Write with proper XML declaration and formatting
    tree.write(config_file, encoding='UTF-8', xml_declaration=True)
    print("✓ Updated Jenkins root URL to https://jenkins.local.info/")
    
except Exception as e:
    print(f"Error updating location config: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON_EOF

if [ $? -eq 0 ]; then
    echo "✓ Jenkins reverse proxy configuration fixed"
    echo "  Root URL set to: https://jenkins.local.info/"
    echo "  Restart Jenkins for changes to take effect"
else
    echo "✗ Failed to update configuration"
    exit 1
fi
