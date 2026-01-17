#!/bin/bash
#
# Jenkins Workspace Cleanup Script
# Removes old build workspaces, virtual environments, and build artifacts
# to free up disk space in Jenkins data directory.
#
# PROTECTED RESOURCES (never deleted):
#   - Docker images (especially :base images and all build images)
#   - Pip caches (both Docker BuildKit cache and host pip caches)
#
# Usage:
#   ./cleanup-workspace.sh [--dry-run] [--keep-days=N] [--keep-workspaces=N]
#
# Options:
#   --dry-run              Show what would be deleted without deleting
#   --keep-days=N          Keep workspaces modified in last N days (default: 7)
#   --keep-workspaces=N    Keep last N workspaces per job (default: 5)
#

set -euo pipefail

# Default options
DRY_RUN=false
KEEP_DAYS=1  # Keep only workspaces from yesterday or today (older than yesterday will be deleted)
KEEP_WORKSPACES=3  # Keep last 3 workspace versions per job
JENKINS_DATA_DIR="${JENKINS_DATA_DIR:-.ops/.jenkins/data}"
WORKSPACE_DIR="${JENKINS_DATA_DIR}/workspace"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --keep-days=*)
            KEEP_DAYS="${1#*=}"
            shift
            ;;
        --keep-workspaces=*)
            KEEP_WORKSPACES="${1#*=}"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--dry-run] [--keep-days=N] [--keep-workspaces=N]"
            exit 1
            ;;
    esac
done

echo "Jenkins Workspace Cleanup"
echo "========================"
echo "Workspace directory: ${WORKSPACE_DIR}"
echo "Keep workspaces from last ${KEEP_DAYS} days"
echo "Keep last ${KEEP_WORKSPACES} workspaces per job"
echo "Dry run: ${DRY_RUN}"
echo ""
echo "⚠️  PROTECTED (will NOT be deleted):"
echo "   - Docker images (all images, including :base images)"
echo "   - Pip caches (Docker BuildKit cache and host pip caches)"
echo ""

if [ ! -d "${WORKSPACE_DIR}" ]; then
    echo "Error: Workspace directory not found: ${WORKSPACE_DIR}"
    exit 1
fi

TOTAL_SIZE_BEFORE=$(du -sh "${WORKSPACE_DIR}" 2>/dev/null | cut -f1)
echo "Total workspace size before cleanup: ${TOTAL_SIZE_BEFORE}"
echo ""

# Function to delete directory
delete_dir() {
    local dir="$1"
    local size=$(du -sh "${dir}" 2>/dev/null | cut -f1)
    if [ "${DRY_RUN}" = "true" ]; then
        echo "  [DRY-RUN] Would delete: ${dir} (${size})"
    else
        echo "  Deleting: ${dir} (${size})"
        rm -rf "${dir}"
    fi
}

# Function to clean virtual environments
clean_venv() {
    local workspace="$1"
    find "${workspace}" -type d -name "venv" -o -name ".venv" -o -name ".venv-jenkins" 2>/dev/null | while read -r venv_dir; do
        if [ -d "${venv_dir}" ]; then
            local size=$(du -sh "${venv_dir}" 2>/dev/null | cut -f1)
            if [ "${DRY_RUN}" = "true" ]; then
                echo "  [DRY-RUN] Would delete venv: ${venv_dir} (${size})"
            else
                echo "  Deleting venv: ${venv_dir} (${size})"
                rm -rf "${venv_dir}"
            fi
        fi
    done
}

# Function to clean build artifacts
clean_build_artifacts() {
    local workspace="$1"
    # Clean Python build artifacts
    find "${workspace}" -type d \( -name "build" -o -name "dist" -o -name "*.egg-info" -o -name "__pycache__" \) 2>/dev/null | while read -r artifact; do
        if [ -d "${artifact}" ]; then
            delete_dir "${artifact}"
        fi
    done
}

CLEANED_COUNT=0
CLEANED_SIZE=0

# Find all workspace directories (job workspaces)
find "${WORKSPACE_DIR}" -maxdepth 1 -type d ! -path "${WORKSPACE_DIR}" | sort | while read -r workspace; do
    workspace_name=$(basename "${workspace}")
    
    # Check if workspace is older than KEEP_DAYS
    if find "${workspace}" -type f -mtime +${KEEP_DAYS} | head -1 | grep -q .; then
        # Workspace has files older than KEEP_DAYS - mark for deletion
        # But first, check if we should keep recent workspaces
        if find "${workspace}" -type f -mtime -${KEEP_DAYS} 2>/dev/null | head -1 | grep -q .; then
            # Workspace has recent files - clean old artifacts instead of deleting whole workspace
            echo "Cleaning old artifacts in: ${workspace_name}"
            clean_venv "${workspace}"
            clean_build_artifacts "${workspace}"
        else
            # Workspace is completely old - delete it
            echo "Removing old workspace: ${workspace_name}"
            delete_dir "${workspace}"
            ((CLEANED_COUNT++)) || true
        fi
    fi
done

# For each job, keep only the most recent N workspaces (if there are multiple versions)
# Group workspaces by job name (e.g., "TradingPythonAgent_staging" and "TradingPythonAgent_staging@2" are from same job)
echo ""
echo "Cleaning duplicate/old workspace versions per job..."
for job_base in $(find "${WORKSPACE_DIR}" -maxdepth 1 -type d ! -path "${WORKSPACE_DIR}" -exec basename {} \; | sed 's/@.*//' | sort -u); do
    # Find all workspace versions for this job (including @2, @3, etc.)
    workspaces=$(find "${WORKSPACE_DIR}" -maxdepth 1 -type d -name "${job_base}*" ! -path "${WORKSPACE_DIR}" | sort -t'@' -k2 -n)
    workspace_count=$(echo "${workspaces}" | wc -l | tr -d ' ')
    
    if [ "${workspace_count}" -gt "${KEEP_WORKSPACES}" ]; then
        echo "Job '${job_base}': Found ${workspace_count} workspaces, keeping last ${KEEP_WORKSPACES}"
        # Keep the most recent N, delete the rest (oldest first)
        # Use awk to skip the last N lines (macOS head doesn't support negative numbers)
        echo "${workspaces}" | awk -v keep="${KEEP_WORKSPACES}" '{lines[NR]=$0} END {for(i=1; i<=NR-keep; i++) print lines[i]}' | while read -r old_workspace; do
            if [ -n "${old_workspace}" ] && [ -d "${old_workspace}" ]; then
                delete_dir "${old_workspace}"
            fi
        done
    fi
done

# Clean virtual environments from remaining workspaces
echo ""
echo "Cleaning virtual environments from all workspaces..."
find "${WORKSPACE_DIR}" -maxdepth 2 -type d \( -name "venv" -o -name ".venv" -o -name ".venv-jenkins" \) 2>/dev/null | while read -r venv_dir; do
    if [ -d "${venv_dir}" ]; then
        size=$(du -sh "${venv_dir}" 2>/dev/null | cut -f1)
        if [ "${DRY_RUN}" = "true" ]; then
            echo "  [DRY-RUN] Would delete venv: ${venv_dir} (${size})"
        else
            echo "  Deleting venv: ${venv_dir} (${size})"
            rm -rf "${venv_dir}"
        fi
    fi
done

# Clean build artifacts from remaining workspaces
echo ""
echo "Cleaning build artifacts from all workspaces..."
find "${WORKSPACE_DIR}" -type d \( -name "build" -o -name "dist" -o -name "*.egg-info" \) 2>/dev/null | while read -r artifact; do
    if [ -d "${artifact}" ]; then
        delete_dir "${artifact}"
    fi
done

# Clean Python cache files (but NOT pip caches)
echo ""
echo "Cleaning Python cache files (excluding pip caches)..."
if [ "${DRY_RUN}" = "true" ]; then
    find "${WORKSPACE_DIR}" -type d -name "__pycache__" ! -path "*/.cache/pip/*" 2>/dev/null | while read -r cache; do
        echo "  [DRY-RUN] Would delete: ${cache}"
    done
    find "${WORKSPACE_DIR}" -type f -name "*.pyc" -o -name "*.pyo" ! -path "*/.cache/pip/*" 2>/dev/null | while read -r cache; do
        echo "  [DRY-RUN] Would delete: ${cache}"
    done
else
    # Exclude pip caches from deletion
    find "${WORKSPACE_DIR}" -type d -name "__pycache__" ! -path "*/.cache/pip/*" -exec rm -rf {} + 2>/dev/null || true
    find "${WORKSPACE_DIR}" -type f \( -name "*.pyc" -o -name "*.pyo" \) ! -path "*/.cache/pip/*" -delete 2>/dev/null || true
fi

# Explicitly protect pip caches (both Docker BuildKit cache and host pip caches)
echo ""
echo "Protecting pip caches..."
if [ -d "${HOME}/.cache/pip" ] || [ -d "/root/.cache/pip" ]; then
    echo "  ✓ Pip caches are protected (not deleted)"
fi

# Explicitly protect Docker images (reminder)
echo ""
echo "Protecting Docker images..."
if command -v docker &> /dev/null; then
    base_image_count=$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -c ':base$' || echo "0")
    total_image_count=$(docker images -q | wc -l | tr -d ' ')
    echo "  ✓ Docker images are protected (${total_image_count} total, ${base_image_count} base images)"
    echo "  ✓ Images are never deleted by this cleanup script"
fi

echo ""
TOTAL_SIZE_AFTER=$(du -sh "${WORKSPACE_DIR}" 2>/dev/null | cut -f1)
echo "Total workspace size after cleanup: ${TOTAL_SIZE_AFTER}"
echo ""
echo "Cleanup completed!"
echo ""
echo "Note: Docker images and pip caches remain untouched and available for future builds."
