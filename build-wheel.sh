#!/bin/bash
#
# Build environment-specific wheel for TradingPythonAgent
#
# Usage:
#   ./build-wheel.sh [dev|staging|prod]
#
# If no environment is specified, automatically detects from git branch:
#   - dev/* branches → dev
#   - staging branch → staging
#   - main/master branch → prod
#
# The wheel will be named: trading_agent-{env}-{version}.whl
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# Function to get current git branch
get_git_branch() {
    local branch
    if command -v git &> /dev/null; then
        branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
        if [ -n "$branch" ]; then
            echo "$branch"
            return 0
        fi
    fi
    # Fallback to environment variable
    echo "${GIT_BRANCH:-${BRANCH_NAME:-}}"
}

# Function to determine environment from branch
get_env_from_branch() {
    local branch="$1"
    local branch_lower="${branch,,}"  # Convert to lowercase
    
    # Check for staging branch
    if [[ "$branch_lower" == "staging" ]]; then
        echo "staging"
        return 0
    fi
    
    # Check for main/master branch
    if [[ "$branch_lower" == "main" || "$branch_lower" == "master" ]]; then
        echo "prod"
        return 0
    fi
    
    # Check for dev branches (dev/* or starts with dev)
    if [[ "$branch_lower" =~ ^dev/ ]] || [[ "$branch_lower" =~ ^dev ]]; then
        echo "dev"
        return 0
    fi
    
    # Default to dev for any other branch
    echo "dev"
}

# Get environment from argument or auto-detect from branch
if [ $# -gt 0 ]; then
    ENV="${1}"
    ENV="${ENV,,}"  # Convert to lowercase
    
    # Validate environment
    if [[ ! "$ENV" =~ ^(dev|staging|prod)$ ]]; then
        log_warn "Invalid environment: $ENV. Auto-detecting from branch..."
        ENV=""
    fi
else
    ENV=""
fi

# Auto-detect environment from branch if not specified
if [ -z "$ENV" ]; then
    CURRENT_BRANCH=$(get_git_branch)
    if [ -n "$CURRENT_BRANCH" ]; then
        ENV=$(get_env_from_branch "$CURRENT_BRANCH")
        log_info "Auto-detected environment from branch '$CURRENT_BRANCH': $ENV"
    else
        log_warn "Could not determine git branch. Defaulting to 'dev'"
        ENV="dev"
    fi
fi

log_debug "Building wheel for environment: $ENV"

log_info "Building wheel for environment: $ENV"

# Show branch info if available
CURRENT_BRANCH=$(get_git_branch)
if [ -n "$CURRENT_BRANCH" ]; then
    log_info "Current branch: $CURRENT_BRANCH"
fi

# Check if setuptools and wheel are installed
if ! python3 -c "import setuptools" 2>/dev/null; then
    log_warn "setuptools not found. Installing..."
    pip install setuptools
fi

if ! python3 -c "import wheel" 2>/dev/null; then
    log_warn "wheel not found. Installing..."
    pip install wheel
fi

# Create dist directory if it doesn't exist
mkdir -p "${PROJECT_ROOT}/dist"

# Clean previous builds (optional - remove if you want to keep old wheels)
log_info "Cleaning previous builds..."
rm -rf "${PROJECT_ROOT}/build" "${PROJECT_ROOT}/src/*.egg-info"
find "${PROJECT_ROOT}/src" -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Build wheel with environment-specific name
log_info "Building wheel: trading_agent-${ENV}-*.whl"
cd "${PROJECT_ROOT}"
ENV="${ENV}" python3 setup.py bdist_wheel

# Find the generated wheel
WHEEL_FILE=$(find "${PROJECT_ROOT}/dist" -name "trading_agent-${ENV}-*.whl" | head -n 1)

if [ -n "${WHEEL_FILE}" ]; then
    log_info "✓ Wheel built successfully: $(basename "${WHEEL_FILE}")"
    log_info "  Location: ${WHEEL_FILE}"
    log_info "  Size: $(du -h "${WHEEL_FILE}" | cut -f1)"
else
    log_warn "⚠️  Wheel file not found in dist/ directory"
    exit 1
fi

# Display wheel contents (optional)
log_info "Wheel contents:"
unzip -l "${WHEEL_FILE}" | head -n 20

log_info "Build complete!"
