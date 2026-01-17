#!/bin/bash
# Script to check GitHub API authentication status in Jenkins

echo "Checking GitHub API authentication in Jenkins..."
echo ""

# Check if Jenkins is running
if ! docker ps | grep -q jenkins; then
    echo "❌ Jenkins container is not running"
    exit 1
fi

echo "✅ Jenkins is running"
echo ""

# Check pipeline credentials
echo "Pipeline Credentials Configuration:"
echo "-----------------------------------"
docker exec jenkins cat /var/jenkins_home/jobs/TradingPythonAgent/config.xml 2>/dev/null | grep -A 2 "credentialsId" | grep "39a94d87" && echo "✅ TradingPythonAgent has credentials configured" || echo "❌ TradingPythonAgent missing credentials"
docker exec jenkins cat /var/jenkins_home/jobs/infra-platform/config.xml 2>/dev/null | grep -A 2 "credentialsId" | grep "39a94d87" && echo "✅ infra-platform has credentials configured" || echo "❌ infra-platform missing credentials"
echo ""

# Instructions for checking in UI
echo "To verify authentication status:"
echo "1. Go to: http://localhost:8081/configure"
echo "2. Scroll to 'GitHub API usage' section"
echo "3. Check the displayed rate limit:"
echo "   - 5000/hour = ✅ Authenticated (using credentials)"
echo "   - 60/hour = ❌ Unauthenticated (not using credentials)"
echo ""
echo "If it shows 60/hour, you need to:"
echo "1. In 'GitHub API usage' section, ensure credentials are selected"
echo "2. Change 'Rate limit strategy' to 'Only when near or above limit'"
echo "3. Save and restart Jenkins if needed"
echo ""
