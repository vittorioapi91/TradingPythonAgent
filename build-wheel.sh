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

# Function to convert to lowercase (bash 3 compatible)
to_lower() {
    echo "$1" | tr '[:upper:]' '[:lower:]'
}

# Function to determine environment from branch
get_env_from_branch() {
    local branch="$1"
    local branch_lower=$(to_lower "$branch")
    
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
EXPLICIT_ENV=""
if [ $# -gt 0 ]; then
    EXPLICIT_ENV="${1}"
    EXPLICIT_ENV=$(to_lower "$EXPLICIT_ENV")
    
    # Validate environment
    if [[ ! "$EXPLICIT_ENV" =~ ^(dev|staging|prod)$ ]]; then
        log_warn "Invalid environment: $EXPLICIT_ENV. Auto-detecting from branch..."
        EXPLICIT_ENV=""
    fi
fi

# Track if we need to restore the original branch
ORIGINAL_BRANCH=""
RESTORE_BRANCH=false

# Check if we're in a CI environment (Jenkins, GitHub Actions, etc.)
# In CI, source is already checked out, so skip checkout
IS_CI=false
if [ -n "${CI:-}" ] || [ -n "${JENKINS_HOME:-}" ] || [ -n "${BUILD_NUMBER:-}" ]; then
    IS_CI=true
    log_info "CI environment detected. Skipping branch checkout (source already checked out)."
fi

# If environment is explicitly provided, checkout the corresponding branch from origin
# (unless we're in CI, where source is already checked out)
if [ -n "$EXPLICIT_ENV" ] && [ "$IS_CI" = false ]; then
    ORIGINAL_BRANCH=$(get_git_branch)
    if [ -n "$ORIGINAL_BRANCH" ]; then
        RESTORE_BRANCH=true
    fi
    
    log_info "Environment explicitly specified: $EXPLICIT_ENV"
    log_info "Checking out corresponding branch from origin..."
    
    case "$EXPLICIT_ENV" in
        dev)
            # For dev, checkout the latest dev/* branch from origin
            # First, fetch all branches
            git fetch origin --prune 2>/dev/null || log_warn "Could not fetch from origin"
            
            # Find the latest dev/* branch (by commit date)
            DEV_BRANCH=$(git branch -r --sort=-committerdate 2>/dev/null | grep -E 'origin/dev/' | head -n 1 | sed 's|origin/||' | xargs)
            
            if [ -z "$DEV_BRANCH" ]; then
                log_warn "No dev/* branch found on origin. Using current branch."
                ENV="dev"
            else
                log_info "Found latest dev branch on origin: $DEV_BRANCH"
                if git checkout "$DEV_BRANCH" 2>/dev/null; then
                    git pull origin "$DEV_BRANCH" 2>/dev/null || true
                    ENV="dev"
                elif git checkout -b "$DEV_BRANCH" "origin/$DEV_BRANCH" 2>/dev/null; then
                    ENV="dev"
                else
                    log_warn "Could not checkout $DEV_BRANCH. Using current branch."
                    ENV="dev"
                    RESTORE_BRANCH=false
                fi
            fi
            ;;
        staging)
            log_info "Checking out 'staging' branch from origin..."
            git fetch origin staging 2>/dev/null || log_warn "Could not fetch staging from origin"
            if git checkout staging 2>/dev/null; then
                git pull origin staging 2>/dev/null || true
                ENV="staging"
            else
                log_warn "Could not checkout staging. Using current branch."
                ENV="staging"
                RESTORE_BRANCH=false
            fi
            ;;
        prod)
            # Try main first, then master
            if git ls-remote --heads origin main 2>/dev/null | grep -q main; then
                log_info "Checking out 'main' branch from origin..."
                git fetch origin main 2>/dev/null || log_warn "Could not fetch main from origin"
                if git checkout main 2>/dev/null; then
                    git pull origin main 2>/dev/null || true
                    ENV="prod"
                else
                    log_warn "Could not checkout main. Using current branch."
                    ENV="prod"
                    RESTORE_BRANCH=false
                fi
            elif git ls-remote --heads origin master 2>/dev/null | grep -q master; then
                log_info "Checking out 'master' branch from origin..."
                git fetch origin master 2>/dev/null || log_warn "Could not fetch master from origin"
                if git checkout master 2>/dev/null; then
                    git pull origin master 2>/dev/null || true
                    ENV="prod"
                else
                    log_warn "Could not checkout master. Using current branch."
                    ENV="prod"
                    RESTORE_BRANCH=false
                fi
            else
                log_warn "Neither 'main' nor 'master' branch found on origin. Using current branch."
                ENV="prod"
                RESTORE_BRANCH=false
            fi
            ;;
    esac
    
    CURRENT_BRANCH=$(get_git_branch)
    log_info "Now on branch: $CURRENT_BRANCH"
elif [ -n "$EXPLICIT_ENV" ] && [ "$IS_CI" = true ]; then
    # In CI, use the explicitly provided environment but don't checkout
    ENV="$EXPLICIT_ENV"
    CURRENT_BRANCH=$(get_git_branch)
    log_info "CI environment: Using explicitly specified environment '$ENV' on branch '$CURRENT_BRANCH'"
else
    # Auto-detect environment from current branch (don't checkout)
    CURRENT_BRANCH=$(get_git_branch)
    if [ -n "$CURRENT_BRANCH" ]; then
        ENV=$(get_env_from_branch "$CURRENT_BRANCH")
        log_info "Auto-detected environment from current branch '$CURRENT_BRANCH': $ENV"
    else
        log_warn "Could not determine git branch. Defaulting to 'dev'"
        ENV="dev"
    fi
fi

log_info "Building wheel for environment: $ENV"

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
# Note: setuptools converts hyphens to underscores in package names
# So trading_agent-dev becomes trading_agent_dev
WHEEL_FILE=$(find "${PROJECT_ROOT}/dist" -name "trading_agent_${ENV}-*.whl" | head -n 1)

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

# Restore original branch if we checked out a different one
if [ "$RESTORE_BRANCH" = true ] && [ -n "$ORIGINAL_BRANCH" ]; then
    CURRENT_BRANCH=$(get_git_branch)
    if [ "$CURRENT_BRANCH" != "$ORIGINAL_BRANCH" ]; then
        log_info "Restoring original branch: $ORIGINAL_BRANCH"
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null || log_warn "Could not restore branch $ORIGINAL_BRANCH"
    fi
fi
