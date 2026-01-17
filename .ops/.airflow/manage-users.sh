#!/bin/bash
#
# Manage Airflow users across all environments
#
# Usage:
#   ./manage-users.sh list [dev|test|prod|all]
#   ./manage-users.sh create <username> <password> <email> [dev|test|prod|all]
#   ./manage-users.sh reset-password <username> <new-password> [dev|test|prod|all]
#   ./manage-users.sh delete <username> [dev|test|prod|all]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

# Get containers based on environment
get_containers() {
    local env="${1:-all}"
    case "$env" in
        dev)
            echo "airflow-dev"
            ;;
        test|staging)
            echo "airflow-test"
            ;;
        prod)
            echo "airflow-prod"
            ;;
        all)
            echo "airflow-dev airflow-test airflow-prod"
            ;;
        *)
            log_error "Invalid environment: $env. Use: dev, test, staging, prod, or all"
            exit 1
            ;;
    esac
}

# Execute command on container
exec_on_container() {
    local container="$1"
    local command="$2"
    
    if ! docker ps --format "{{.Names}}" | grep -q "^${container}$"; then
        log_warn "Container ${container} is not running. Skipping..."
        return 1
    fi
    
    docker exec "${container}" bash -c "${command}" 2>&1
}

# List users
list_users() {
    local env="${1:-all}"
    local containers=$(get_containers "$env")
    
    for container in $containers; do
        log_info "Users in ${container}:"
        exec_on_container "$container" "airflow users list" || log_warn "Failed to list users in ${container}"
        echo ""
    done
}

# Create user
create_user() {
    local username="$1"
    local password="$2"
    local email="$3"
    local env="${4:-all}"
    local containers=$(get_containers "$env")
    
    for container in $containers; do
        log_info "Creating user ${username} in ${container}..."
        result=$(exec_on_container "$container" "airflow users create --username ${username} --firstname $(echo ${username} | cut -d'@' -f1) --lastname User --role Admin --email ${email} --password ${password}" 2>&1)
        if echo "$result" | grep -q "already exist"; then
            log_warn "User ${username} already exists in ${container}"
        elif echo "$result" | grep -q "Created"; then
            log_info "✓ User ${username} created in ${container}"
        else
            log_error "Failed to create user in ${container}: $result"
        fi
    done
}

# Reset password
reset_password() {
    local username="$1"
    local new_password="$2"
    local env="${3:-all}"
    local containers=$(get_containers "$env")
    
    for container in $containers; do
        log_info "Resetting password for ${username} in ${container}..."
        result=$(exec_on_container "$container" "airflow users reset-password --username ${username} --password ${new_password}" 2>&1)
        if echo "$result" | grep -q "Password reset"; then
            log_info "✓ Password reset for ${username} in ${container}"
        else
            log_error "Failed to reset password in ${container}: $result"
        fi
    done
}

# Delete user
delete_user() {
    local username="$1"
    local env="${2:-all}"
    local containers=$(get_containers "$env")
    
    log_warn "This will delete user ${username} from: ${containers}"
    read -p "Are you sure? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        log_info "Cancelled."
        return
    fi
    
    for container in $containers; do
        log_info "Deleting user ${username} from ${container}..."
        exec_on_container "$container" "airflow users delete --username ${username}" || log_warn "Failed to delete user in ${container}"
    done
}

# Main command handler
case "${1:-}" in
    list)
        list_users "${2:-all}"
        ;;
    create)
        if [ $# -lt 4 ]; then
            log_error "Usage: $0 create <username> <password> <email> [dev|test|prod|all]"
            exit 1
        fi
        create_user "$2" "$3" "$4" "${5:-all}"
        ;;
    reset-password)
        if [ $# -lt 3 ]; then
            log_error "Usage: $0 reset-password <username> <new-password> [dev|test|prod|all]"
            exit 1
        fi
        reset_password "$2" "$3" "${4:-all}"
        ;;
    delete)
        if [ $# -lt 2 ]; then
            log_error "Usage: $0 delete <username> [dev|test|prod|all]"
            exit 1
        fi
        delete_user "$2" "${3:-all}"
        ;;
    *)
        echo "Airflow User Management"
        echo ""
        echo "Usage:"
        echo "  $0 list [dev|test|prod|all]"
        echo "  $0 create <username> <password> <email> [dev|test|prod|all]"
        echo "  $0 reset-password <username> <new-password> [dev|test|prod|all]"
        echo "  $0 delete <username> [dev|test|prod|all]"
        echo ""
        echo "Examples:"
        echo "  $0 list all"
        echo "  $0 create vittorioapi mypassword apicellavittorio@hotmail.it all"
        echo "  $0 reset-password vittorioapi newpassword dev"
        exit 1
        ;;
esac
