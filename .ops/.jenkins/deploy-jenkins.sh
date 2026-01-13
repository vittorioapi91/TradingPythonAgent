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
    
    if ! docker images | grep -q "jenkins-custom.*lts"; then
        log_info "Building Jenkins custom image..."
        docker build -t jenkins-custom:lts -f "${PROJECT_ROOT}/.ops/.docker/Dockerfile.jenkins" "${PROJECT_ROOT}/.ops/.docker"
        log_info "Jenkins custom image built successfully"
    else
        log_info "Jenkins custom image already exists"
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
    
    # Create minimal config.xml if it doesn't exist
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
</hudson>
EOF
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
        log_info "Pipeline job: Will be created on first branch scan"
    fi
    
    if [ -d "${JENKINS_DATA_DIR}/users" ]; then
        local user_count=$(find "${JENKINS_DATA_DIR}/users" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | tr -d ' ')
        log_info "Users configured: ${user_count}"
    fi
    
    log_info "=========================================="
    log_info "Next steps:"
    log_info "1. Access Jenkins at http://localhost:8081"
    log_info "2. Log in with your credentials"
    log_info "3. The TradingPythonAgent pipeline will scan for branches automatically"
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
