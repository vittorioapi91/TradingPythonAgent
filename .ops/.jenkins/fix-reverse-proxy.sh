#!/bin/bash
# Fix Jenkins reverse proxy configuration
# Sets Jenkins root URL to work with nginx HTTPS proxy

set -euo pipefail

JENKINS_DATA_DIR="${1:-.ops/.jenkins/data}"
CONFIG_XML="${JENKINS_DATA_DIR}/config.xml"

if [ ! -f "$CONFIG_XML" ]; then
    echo "Error: Jenkins config.xml not found at $CONFIG_XML"
    exit 1
fi

echo "Fixing Jenkins reverse proxy configuration..."

# Backup config
cp "$CONFIG_XML" "${CONFIG_XML}.backup.$(date +%Y%m%d_%H%M%S)"

# Use Python to update config.xml (more reliable than sed for XML)
python3 <<PYTHON_EOF
import xml.etree.ElementTree as ET
import sys
from datetime import datetime

config_file = "${CONFIG_XML}"

try:
    tree = ET.parse(config_file)
    root = tree.getroot()
    
    # Find or create jenkinsUrl property
    properties = root.find('.//properties')
    if properties is None:
        # If properties doesn't exist, create it
        properties = ET.SubElement(root.find('.//mode') or root.find('.//version') or root, 'properties')
    
    # Look for jenkins.model.JenkinsLocationConfiguration or create it
    jenkins_loc = None
    for prop in properties.findall('.//jenkins.model.JenkinsLocationConfiguration'):
        jenkins_loc = prop
        break
    
    if jenkins_loc is None:
        jenkins_loc = ET.SubElement(properties, 'jenkins.model.JenkinsLocationConfiguration')
    
    # Set jenkinsUrl
    jenkins_url = jenkins_loc.find('jenkinsUrl')
    if jenkins_url is None:
        jenkins_url = ET.SubElement(jenkins_loc, 'jenkinsUrl')
    jenkins_url.text = 'https://jenkins.local.info/'
    
    # Set adminAddress (optional but recommended)
    admin_addr = jenkins_loc.find('adminAddress')
    if admin_addr is None:
        admin_addr = ET.SubElement(jenkins_loc, 'adminAddress')
    admin_addr.text = 'admin@jenkins.local.info'
    
    tree.write(config_file, encoding='UTF-8', xml_declaration=True)
    print("✓ Updated Jenkins root URL to https://jenkins.local.info/")
    
except Exception as e:
    print(f"Error updating config.xml: {e}", file=sys.stderr)
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
