#!/bin/bash
#
# Jenkins Deployment Script
# Deploys Jenkins with all plugins, credentials, and pipeline configuration
# exactly as configured on the development machine.
#
# Usage: ./deploy-jenkins.sh [--fresh] [--backup]
#
# Options:
#   --fresh    : Start with a fresh Jenkins instance (removes existing data)
#   --backup   : Create a backup of current Jenkins data before deployment
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JENKINS_DATA_DIR="${SCRIPT_DIR}/data"
DOCKER_COMPOSE_FILE="${PROJECT_ROOT}/.ops/.docker/docker-compose.yml"
BACKUP_DIR="${SCRIPT_DIR}/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

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

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "docker-compose is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    log_info "Prerequisites check passed"
}

# Build Jenkins custom image if needed
build_jenkins_image() {
    log_info "Checking Jenkins custom image..."
    
    # Always rebuild to ensure latest changes (including buildx) are included
    # Check if image exists and is recent (less than 1 day old)
    local image_age=$(docker images --format "{{.CreatedAt}}" jenkins-custom:lts 2>/dev/null | head -1)
    local should_rebuild=true
    
    if [ -n "$image_age" ]; then
        # Check if image was created today (simple check)
        local today=$(date +%Y-%m-%d)
        if echo "$image_age" | grep -q "$today"; then
            log_info "Jenkins custom image exists and was built today"
            log_info "Rebuilding to ensure buildx and latest dependencies are included..."
        else
            log_info "Jenkins custom image exists but is older than today - rebuilding..."
        fi
    else
        log_info "Jenkins custom image not found - building..."
    fi
    
    log_info "Building Jenkins custom image with buildx support..."
    log_info "This may take 10-20 minutes due to Python dependency installation..."
    docker build -t jenkins-custom:lts -f "${PROJECT_ROOT}/.ops/.docker/Dockerfile.jenkins" "${PROJECT_ROOT}/.ops/.docker"
    
    if [ $? -eq 0 ]; then
        log_info "✓ Jenkins custom image built successfully"
        
        # Verify buildx is installed
        log_info "Verifying buildx installation..."
        if docker run --rm jenkins-custom:lts docker buildx version >/dev/null 2>&1; then
            log_info "✓ buildx verified in Jenkins image"
        else
            log_warn "⚠️  buildx verification failed - image may need to be rebuilt"
        fi
    else
        log_error "Failed to build Jenkins custom image"
        return 1
    fi
}

# Create backup of current Jenkins data
create_backup() {
    if [ ! -d "${JENKINS_DATA_DIR}" ]; then
        log_warn "No existing Jenkins data to backup"
        return
    fi
    
    log_info "Creating backup of current Jenkins data..."
    mkdir -p "${BACKUP_DIR}"
    BACKUP_FILE="${BACKUP_DIR}/jenkins_backup_${TIMESTAMP}.tar.gz"
    
    tar -czf "${BACKUP_FILE}" -C "${SCRIPT_DIR}" data
    log_info "Backup created: ${BACKUP_FILE}"
}

# Restore Jenkins data from backup
restore_from_backup() {
    local backup_file="$1"
    
    if [ ! -f "${backup_file}" ]; then
        log_error "Backup file not found: ${backup_file}"
        exit 1
    fi
    
    log_info "Restoring Jenkins data from backup: ${backup_file}"
    
    # Stop Jenkins if running
    if docker-compose -f "${DOCKER_COMPOSE_FILE}" ps jenkins | grep -q "Up"; then
        log_info "Stopping Jenkins..."
        docker-compose -f "${DOCKER_COMPOSE_FILE}" stop jenkins
    fi
    
    # Remove existing data if fresh install
    if [ -d "${JENKINS_DATA_DIR}" ]; then
        log_info "Removing existing Jenkins data..."
        rm -rf "${JENKINS_DATA_DIR}"
    fi
    
    # Extract backup
    mkdir -p "${SCRIPT_DIR}"
    tar -xzf "${backup_file}" -C "${SCRIPT_DIR}"
    log_info "Jenkins data restored"
}

# Initialize Jenkins data structure
initialize_jenkins_data() {
    log_info "Initializing Jenkins data structure..."
    
    mkdir -p "${JENKINS_DATA_DIR}"/{plugins,jobs,users,war}
    
    # Create or update config.xml with GitHub API usage strategy
    if [ ! -f "${JENKINS_DATA_DIR}/config.xml" ]; then
        log_info "Creating minimal Jenkins config.xml..."
        cat > "${JENKINS_DATA_DIR}/config.xml" <<'EOF'
<?xml version='1.1' encoding='UTF-8'?>
<hudson>
  <disabledAdministrativeMonitors>
    <string>hudson.util.DoubleLaunchChecker</string>
  </disabledAdministrativeMonitors>
  <version>2.528.3</version>
  <numExecutors>2</numExecutors>
  <mode>NORMAL</mode>
  <useSecurity>true</useSecurity>
  <authorizationStrategy class="hudson.security.FullControlOnceLoggedInAuthorizationStrategy">
    <denyAnonymousReadAccess>true</denyAnonymousReadAccess>
  </authorizationStrategy>
  <securityRealm class="hudson.security.HudsonPrivateSecurityRealm">
    <disableSignup>false</disableSignup>
    <enableCaptcha>false</enableCaptcha>
  </securityRealm>
  <disableRememberMe>false</disableRememberMe>
  <projectNamingStrategy class="jenkins.model.ProjectNamingStrategy$DefaultProjectNamingStrategy"/>
  <workspaceDir>${JENKINS_HOME}/workspace/${ITEM_FULL_NAME}</workspaceDir>
  <buildsDir>${ITEM_ROOTDIR}/builds</buildsDir>
  <markupFormatter class="hudson.markup.EscapedMarkupFormatter"/>
  <jdks/>
  <viewsTabBar class="hudson.views.DefaultViewsTabBar"/>
  <myViewsTabBar class="hudson.views.DefaultMyViewsTabBar"/>
  <clouds/>
  <quietPeriod>5</quietPeriod>
  <scmCheckoutRetryCount>0</scmCheckoutRetryCount>
  <views>
    <hudson.model.AllView>
      <owner class="hudson" reference="../../.."/>
      <name>all</name>
      <filterExecutors>false</filterExecutors>
      <filterQueue>false</filterQueue>
      <properties class="hudson.model.View$PropertyList"/>
    </hudson.model.AllView>
  </views>
  <primaryView>all</primaryView>
  <slaveAgentPort>50000</slaveAgentPort>
  <label></label>
  <crumbIssuer class="hudson.security.csrf.DefaultCrumbIssuer">
    <excludeClientIPFromCrumb>false</excludeClientIPFromCrumb>
  </crumbIssuer>
  <nodeProperties/>
  <globalNodeProperties/>
  <nodeRenameMigrationNeeded>false</nodeRenameMigrationNeeded>
  <org.jenkinsci.plugins.github_branch_source.GitHubSCMSource>
    <apiRateLimitChecker class="org.jenkinsci.plugins.github_branch_source.ThrottleForNormalize"/>
  </org.jenkinsci.plugins.github_branch_source.GitHubSCMSource>
</hudson>
EOF
        log_info "✓ Created config.xml with GitHub API usage strategy: 'Only when near or above limit'"
    else
        # Update existing config.xml to include GitHub API usage strategy
        log_info "Updating config.xml with GitHub API usage strategy..."
        python3 <<PYTHON_CONFIG_EOF
import xml.etree.ElementTree as ET
import sys
from datetime import datetime
import shutil

config_file = "${JENKINS_DATA_DIR}/config.xml"

try:
    # Backup existing file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{config_file}.backup.{timestamp}"
    shutil.copy(config_file, backup_file)
    print(f"✓ Backup created: {backup_file}")
    
    tree = ET.parse(config_file)
    root = tree.getroot()
    
    # Check if GitHubSCMSource config already exists
    github_config = root.find('.//org.jenkinsci.plugins.github_branch_source.GitHubSCMSource')
    
    if github_config is None:
        # Add GitHub API usage strategy configuration
        github_config = ET.SubElement(root, 'org.jenkinsci.plugins.github_branch_source.GitHubSCMSource')
        rate_limit_checker = ET.SubElement(github_config, 'apiRateLimitChecker')
        rate_limit_checker.set('class', 'org.jenkinsci.plugins.github_branch_source.ThrottleForNormalize')
        tree.write(config_file, encoding='UTF-8', xml_declaration=True)
        print("✓ Added GitHub API usage strategy: 'Only when near or above limit'")
    else:
        # Check if apiRateLimitChecker is already set correctly
        rate_checker = github_config.find('apiRateLimitChecker')
        if rate_checker is None:
            rate_checker = ET.SubElement(github_config, 'apiRateLimitChecker')
            rate_checker.set('class', 'org.jenkinsci.plugins.github_branch_source.ThrottleForNormalize')
            tree.write(config_file, encoding='UTF-8', xml_declaration=True)
            print("✓ Added GitHub API usage strategy: 'Only when near or above limit'")
        elif rate_checker.get('class') != 'org.jenkinsci.plugins.github_branch_source.ThrottleForNormalize':
            rate_checker.set('class', 'org.jenkinsci.plugins.github_branch_source.ThrottleForNormalize')
            tree.write(config_file, encoding='UTF-8', xml_declaration=True)
            print("✓ Updated GitHub API usage strategy: 'Only when near or above limit'")
        else:
            print("✓ GitHub API usage strategy already configured correctly")
    
except Exception as e:
    print(f"Error updating config.xml: {e}", file=sys.stderr)
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON_CONFIG_EOF
        
        if [ $? -eq 0 ]; then
            log_info "✓ GitHub API usage strategy configured in config.xml"
        else
            log_warn "Failed to update config.xml automatically"
            log_warn "Please configure manually: Manage Jenkins → Configure System → GitHub API usage → 'Only when near or above limit'"
        fi
    fi
    
    # Create or update global credentials.xml
    log_info "Setting up global GitHub credentials..."
    local CRED_ID="39a94d87-8a43-468b-9138-14b4f86d7b93"
    
    if [ ! -f "${JENKINS_DATA_DIR}/credentials.xml" ]; then
        log_info "Creating global credentials.xml..."
        cat > "${JENKINS_DATA_DIR}/credentials.xml" <<'CREDENTIALS_EOF'
<?xml version='1.1' encoding='UTF-8'?>
<com.cloudbees.plugins.credentials.SystemCredentialsProvider plugin="credentials@2.8.1">
  <domainCredentialsMap class="hudson.util.CopyOnWriteMap$Hash">
    <entry>
      <com.cloudbees.plugins.credentials.domains.Domain>
        <specifications/>
      </com.cloudbees.plugins.credentials.domains.Domain>
      <java.util.concurrent.CopyOnWriteArrayList>
        <com.cloudbees.plugins.credentials.impl.UsernamePasswordCredentialsImpl>
          <scope>GLOBAL</scope>
          <id>39a94d87-8a43-468b-9138-14b4f86d7b93</id>
          <description>GitHub credentials for all pipelines</description>
          <username>vittorioapi</username>
          <password>{AQAAABAAAAAwAE/LDOOFpZxnqI9m2WyXgytqc+SiBfQhsVqywQNtetFXvoYMadSSb1FQdflKbz/nr2LkfnFAYBUIwHouLU8HUQ==}</password>
          <usernameSecret>false</usernameSecret>
        </com.cloudbees.plugins.credentials.impl.UsernamePasswordCredentialsImpl>
      </java.util.concurrent.CopyOnWriteArrayList>
    </entry>
  </domainCredentialsMap>
</com.cloudbees.plugins.credentials.SystemCredentialsProvider>
CREDENTIALS_EOF
        log_info "✓ Global credentials.xml created"
    else
        # Check if credentials already exist
        if grep -q "${CRED_ID}" "${JENKINS_DATA_DIR}/credentials.xml" 2>/dev/null; then
            log_info "✓ Global credentials already configured"
        else
            log_warn "credentials.xml exists but GitHub credentials (${CRED_ID}) not found."
            log_warn "Adding credentials to existing credentials.xml..."
            
            # Backup existing file
            cp "${JENKINS_DATA_DIR}/credentials.xml" "${JENKINS_DATA_DIR}/credentials.xml.backup.${TIMESTAMP}"
            
            # Use Python or sed to add credentials (Python is more reliable for XML)
            python3 <<PYTHON_EOF
import xml.etree.ElementTree as ET
import sys

try:
    tree = ET.parse("${JENKINS_DATA_DIR}/credentials.xml")
    root = tree.getroot()
    
    # Find or create the domainCredentialsMap
    domain_map = root.find('.//domainCredentialsMap')
    if domain_map is None:
        # Create structure if it doesn't exist
        provider = root.find('.//com.cloudbees.plugins.credentials.SystemCredentialsProvider')
        if provider is None:
            print("Error: Could not find SystemCredentialsProvider", file=sys.stderr)
            sys.exit(1)
        domain_map = ET.SubElement(provider, 'domainCredentialsMap')
        domain_map.set('class', 'hudson.util.CopyOnWriteMap\$Hash')
    
    # Find or create entry
    entry = domain_map.find('entry')
    if entry is None:
        entry = ET.SubElement(domain_map, 'entry')
        domain = ET.SubElement(entry, 'com.cloudbees.plugins.credentials.domains.Domain')
        specs = ET.SubElement(domain, 'specifications')
        cred_list = ET.SubElement(entry, 'java.util.concurrent.CopyOnWriteArrayList')
    else:
        cred_list = entry.find('java.util.concurrent.CopyOnWriteArrayList')
        if cred_list is None:
            cred_list = ET.SubElement(entry, 'java.util.concurrent.CopyOnWriteArrayList')
    
    # Check if credential already exists
    if cred_list.find(f".//id[.='${CRED_ID}']") is None:
        cred = ET.SubElement(cred_list, 'com.cloudbees.plugins.credentials.impl.UsernamePasswordCredentialsImpl')
        ET.SubElement(cred, 'scope').text = 'GLOBAL'
        ET.SubElement(cred, 'id').text = '${CRED_ID}'
        ET.SubElement(cred, 'description').text = 'GitHub credentials for all pipelines'
        ET.SubElement(cred, 'username').text = 'vittorioapi'
        ET.SubElement(cred, 'password').text = '{AQAAABAAAAAwAE/LDOOFpZxnqI9m2WyXgytqc+SiBfQhsVqywQNtetFXvoYMadSSb1FQdflKbz/nr2LkfnFAYBUIwHouLU8HUQ==}'
        ET.SubElement(cred, 'usernameSecret').text = 'false'
        tree.write("${JENKINS_DATA_DIR}/credentials.xml", encoding='UTF-8', xml_declaration=True)
        print("✓ Credentials added to existing credentials.xml")
    else:
        print("✓ Credentials already exist in credentials.xml")
except Exception as e:
    print(f"Error updating credentials.xml: {e}", file=sys.stderr)
    print("Please add credentials manually via Jenkins UI: Manage Jenkins → Manage Credentials → Global", file=sys.stderr)
    sys.exit(1)
PYTHON_EOF
            
            if [ $? -eq 0 ]; then
                log_info "✓ Credentials added to existing credentials.xml"
            else
                log_error "Failed to add credentials automatically"
                log_warn "Please add credentials manually via Jenkins UI:"
                log_warn "  Manage Jenkins → Manage Credentials → Global → Add Credentials"
                log_warn "  Or restore from backup: ${JENKINS_DATA_DIR}/credentials.xml.backup.${TIMESTAMP}"
            fi
        fi
    fi
    
    log_info "Jenkins data structure initialized"
}

# Verify Jenkins data integrity
verify_jenkins_data() {
    log_info "Verifying Jenkins data integrity..."
    
    local errors=0
    
    # Check for critical files
    if [ ! -f "${JENKINS_DATA_DIR}/config.xml" ]; then
        log_error "Missing: config.xml"
        errors=$((errors + 1))
    fi
    
    # Check for plugins
    local plugin_count=$(find "${JENKINS_DATA_DIR}/plugins" -name "*.jpi" 2>/dev/null | wc -l | tr -d ' ')
    if [ "${plugin_count}" -eq 0 ]; then
        log_warn "No plugin JAR files found (expected ~106 plugins)"
    else
        log_info "Found ${plugin_count} plugin JAR files"
    fi
    
    # Check for pipeline job
    # Check for TradingPythonAgent pipeline
    if [ ! -f "${JENKINS_DATA_DIR}/jobs/TradingPythonAgent/config.xml" ]; then
        log_warn "TradingPythonAgent pipeline config not found. It will need to be created manually or via Jenkins UI."
    fi
    
    # Check for infra-platform pipeline
    if [ ! -f "${JENKINS_DATA_DIR}/jobs/infra-platform/config.xml" ]; then
        log_info "Creating infra-platform pipeline configuration..."
        mkdir -p "${JENKINS_DATA_DIR}/jobs/infra-platform"
        cat > "${JENKINS_DATA_DIR}/jobs/infra-platform/config.xml" << 'INFRAPLATFORM_CONFIG_EOF'
<?xml version='1.1' encoding='UTF-8'?>
<org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject plugin="workflow-multibranch@821.vc3b_4ea_780798">
  <actions/>
  <description>Infrastructure platform pipeline - validates and builds infrastructure components (Airflow, Grafana, etc.). Triggers on .ops/ directory changes.</description>
  <displayName>infra-platform</displayName>
  <properties>
    <hudson.plugins.jira.JiraFolderProperty plugin="jira@3.21">
      <sites>
        <hudson.plugins.jira.JiraSite>
          <url>https://vittorioapi91.atlassian.net/</url>
          <useHTTPAuth>false</useHTTPAuth>
          <credentialsId>65d65507-43f0-4806-b1ae-f526b96fe236</credentialsId>
          <useBearerAuth>false</useBearerAuth>
          <supportsWikiStyleComment>false</supportsWikiStyleComment>
          <recordScmChanges>false</recordScmChanges>
          <disableChangelogAnnotations>false</disableChangelogAnnotations>
          <updateJIRAIssueForAllStatus>false</updateJIRAIssueForAllStatus>
          <timeout>10</timeout>
          <readTimeout>30</readTimeout>
          <threadExecutorNumber>10</threadExecutorNumber>
          <appendChangeTimestamp>false</appendChangeTimestamp>
          <maxIssuesFromJqlSearch>100</maxIssuesFromJqlSearch>
          <ioThreadCount>2</ioThreadCount>
        </hudson.plugins.jira.JiraSite>
      </sites>
    </hudson.plugins.jira.JiraFolderProperty>
    <com.cloudbees.hudson.plugins.folder.properties.FolderCredentialsProvider_-FolderCredentialsProperty plugin="cloudbees-folder@6.1073.va_7888eb_dd514">
      <domainCredentialsMap class="hudson.util.CopyOnWriteMap\$Hash">
        <entry>
          <com.cloudbees.plugins.credentials.domains.Domain plugin="credentials@1480.v2246fd131e83">
            <specifications/>
          </com.cloudbees.plugins.credentials.domains.Domain>
          <java.util.concurrent.CopyOnWriteArrayList>
            <com.cloudbees.plugins.credentials.impl.UsernamePasswordCredentialsImpl plugin="credentials@1480.v2246fd131e83">
              <id>39a94d87-8a43-468b-9138-14b4f86d7b93</id>
              <description></description>
              <username>vittorioapi</username>
              <password>{AQAAABAAAAAwAE/LDOOFpZxnqI9m2WyXgytqc+SiBfQhsVqywQNtetFXvoYMadSSb1FQdflKbz/nr2LkfnFAYBUIwHouLU8HUQ==}</password>
              <usernameSecret>false</usernameSecret>
            </com.cloudbees.plugins.credentials.impl.UsernamePasswordCredentialsImpl>
          </java.util.concurrent.CopyOnWriteArrayList>
        </entry>
      </domainCredentialsMap>
    </com.cloudbees.hudson.plugins.folder.properties.FolderCredentialsProvider_-FolderCredentialsProperty>
  </properties>
  <folderViews class="jenkins.branch.MultiBranchProjectViewHolder" plugin="branch-api@2.1268.v044a_87612da_8">
    <owner class="org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject" reference="../.."/>
  </folderViews>
  <healthMetrics/>
  <icon class="jenkins.branch.MetadataActionFolderIcon" plugin="branch-api@2.1268.v044a_87612da_8">
    <owner class="org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject" reference="../.."/>
  </icon>
  <orphanedItemStrategy class="com.cloudbees.hudson.plugins.folder.computed.DefaultOrphanedItemStrategy" plugin="cloudbees-folder@6.1073.va_7888eb_dd514">
    <pruneDeadBranches>true</pruneDeadBranches>
    <daysToKeep>-1</daysToKeep>
    <numToKeep>-1</numToKeep>
    <abortBuilds>false</abortBuilds>
  </orphanedItemStrategy>
  <triggers>
    <com.cloudbees.hudson.plugins.folder.computed.PeriodicFolderTrigger plugin="cloudbees-folder@6.1073.va_7888eb_dd514">
      <spec>H/5 * * * *</spec>
      <interval>300000</interval>
    </com.cloudbees.hudson.plugins.folder.computed.PeriodicFolderTrigger>
  </triggers>
  <disabled>false</disabled>
  <sources class="jenkins.branch.MultiBranchProject$BranchSourceList" plugin="branch-api@2.1268.v044a_87612da_8">
    <data>
      <jenkins.branch.BranchSource>
        <source class="org.jenkinsci.plugins.github_branch_source.GitHubSCMSource" plugin="github-branch-source@1917.v9ee8a_39b_3d0d">
          <id>1</id>
          <apiUri>https://api.github.com</apiUri>
          <credentialsId>39a94d87-8a43-468b-9138-14b4f86d7b93</credentialsId>
          <repoOwner>vittorioapi91</repoOwner>
          <repository>TradingPythonAgent</repository>
          <repositoryUrl>https://github.com/vittorioapi91/TradingPythonAgent.git</repositoryUrl>
          <traits>
            <org.jenkinsci.plugins.github__branch__source.BranchDiscoveryTrait>
              <strategyId>1</strategyId>
            </org.jenkinsci.plugins.github__branch__source.BranchDiscoveryTrait>
            <org.jenkinsci.plugins.github__branch__source.OriginPullRequestDiscoveryTrait>
              <strategyId>2</strategyId>
            </org.jenkinsci.plugins.github__branch__source.OriginPullRequestDiscoveryTrait>
            <org.jenkinsci.plugins.github__branch__source.ForkPullRequestDiscoveryTrait>
              <strategyId>2</strategyId>
              <trust class="org.jenkinsci.plugins.github_branch_source.ForkPullRequestDiscoveryTrait$TrustPermission"/>
            </org.jenkinsci.plugins.github__branch__source.ForkPullRequestDiscoveryTrait>
          </traits>
        </source>
        <strategy class="jenkins.branch.DefaultBranchPropertyStrategy">
          <properties class="empty-list"/>
        </strategy>
      </jenkins.branch.BranchSource>
    </data>
    <owner class="org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject" reference="../.."/>
  </sources>
  <factory class="org.jenkinsci.plugins.workflow.multibranch.WorkflowBranchProjectFactory">
    <owner class="org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject" reference="../.."/>
    <scriptPath>.ops/Jenkinsfile.infra-platform</scriptPath>
  </factory>
</org.jenkinsci.plugins.workflow.multibranch.WorkflowMultiBranchProject>
INFRAPLATFORM_CONFIG_EOF
        log_info "✓ infra-platform pipeline configuration created"
    else
        log_info "✓ infra-platform pipeline configuration already exists"
    fi
    
    # Original check (keeping for backward compatibility)
    if [ ! -f "${JENKINS_DATA_DIR}/jobs/TradingPythonAgent/config.xml" ]; then
        log_warn "Pipeline job config not found (will be created on first scan)"
    else
        log_info "Pipeline job config found"
    fi
    
    if [ ${errors} -eq 0 ]; then
        log_info "Jenkins data verification passed"
    else
        log_error "Jenkins data verification failed with ${errors} errors"
        return 1
    fi
}

# Start Jenkins
start_jenkins() {
    log_info "Starting Jenkins..."
    
    cd "${PROJECT_ROOT}"
    docker-compose -f "${DOCKER_COMPOSE_FILE}" up -d jenkins
    
    log_info "Waiting for Jenkins to be ready..."
    local max_attempts=60
    local attempt=0
    
    while [ ${attempt} -lt ${max_attempts} ]; do
        if curl -s -f http://localhost:8081/login > /dev/null 2>&1; then
            log_info "Jenkins is ready!"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    log_error "Jenkins failed to start within expected time"
    return 1
}

# Wait for Jenkins to be fully initialized
wait_for_jenkins_ready() {
    log_info "Waiting for Jenkins to be fully initialized..."
    
    local max_attempts=120
    local attempt=0
    
    while [ ${attempt} -lt ${max_attempts} ]; do
        # Check if Jenkins is responding and not in setup wizard
        if curl -s http://localhost:8081/api/json 2>/dev/null | grep -q "nodeName"; then
            log_info "Jenkins is fully initialized"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    log_warn "Jenkins may still be initializing"
}

# Print deployment summary
print_summary() {
    log_info "=========================================="
    log_info "Jenkins Deployment Summary"
    log_info "=========================================="
    log_info "Jenkins URL: http://localhost:8081"
    log_info "Data Directory: ${JENKINS_DATA_DIR}"
    
    if [ -d "${JENKINS_DATA_DIR}/plugins" ]; then
        local plugin_count=$(find "${JENKINS_DATA_DIR}/plugins" -name "*.jpi" 2>/dev/null | wc -l | tr -d ' ')
        log_info "Plugins installed: ${plugin_count}"
    fi
    
    if [ -d "${JENKINS_DATA_DIR}/jobs/TradingPythonAgent" ]; then
        log_info "Pipeline job: TradingPythonAgent (configured)"
    else
        log_info "Pipeline job: TradingPythonAgent (will be created on first branch scan)"
    fi
    
    if [ -d "${JENKINS_DATA_DIR}/jobs/infra-platform" ]; then
        log_info "Pipeline job: infra-platform (configured)"
    else
        log_info "Pipeline job: infra-platform (will be created automatically)"
    fi
    
    if [ -d "${JENKINS_DATA_DIR}/users" ]; then
        local user_count=$(find "${JENKINS_DATA_DIR}/users" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | tr -d ' ')
        log_info "Users configured: ${user_count}"
    fi
    
    log_info "=========================================="
    log_info "Next steps:"
    log_info "1. Access Jenkins at http://localhost:8081"
    log_info "2. Log in with your credentials"
    log_info "3. Pipelines will scan for branches automatically:"
    log_info "   - TradingPythonAgent (application code)"
    log_info "   - infra-platform (infrastructure, triggers on .ops/ changes)"
    log_info "=========================================="
}

# Main deployment function
main() {
    local fresh_install=false
    local create_backup_flag=false
    local restore_backup=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --fresh)
                fresh_install=true
                shift
                ;;
            --backup)
                create_backup_flag=true
                shift
                ;;
            --restore)
                restore_backup="$2"
                shift 2
                ;;
            *)
                log_error "Unknown option: $1"
                echo "Usage: $0 [--fresh] [--backup] [--restore <backup-file>]"
                exit 1
                ;;
        esac
    done
    
    log_info "Starting Jenkins deployment..."
    
    # Prerequisites check
    check_prerequisites
    
    # Handle restore
    if [ -n "${restore_backup}" ]; then
        restore_from_backup "${restore_backup}"
    fi
    
    # Create backup if requested
    if [ "${create_backup_flag}" = true ]; then
        create_backup
    fi
    
    # Build Jenkins image
    build_jenkins_image
    
    # Handle fresh install
    if [ "${fresh_install}" = true ]; then
        log_warn "Fresh install requested - existing data will be removed"
        if [ -d "${JENKINS_DATA_DIR}" ]; then
            log_info "Stopping Jenkins..."
            docker-compose -f "${DOCKER_COMPOSE_FILE}" stop jenkins 2>/dev/null || true
            log_info "Removing existing Jenkins data..."
            rm -rf "${JENKINS_DATA_DIR}"
        fi
        initialize_jenkins_data
    else
        # Verify existing data or initialize if missing
        if [ ! -d "${JENKINS_DATA_DIR}" ]; then
            log_warn "Jenkins data directory not found - initializing..."
            initialize_jenkins_data
        else
            verify_jenkins_data || {
                log_error "Jenkins data verification failed"
                exit 1
            }
        fi
    fi
    
    # Start Jenkins
    start_jenkins || {
        log_error "Failed to start Jenkins"
        exit 1
    }
    
    # Wait for full initialization
    wait_for_jenkins_ready
    
    # Print summary
    print_summary
    
    log_info "Jenkins deployment completed successfully!"
}

# Run main function
main "$@"
