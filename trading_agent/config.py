"""
Environment Configuration Module

This module handles environment-specific configuration by detecting the current
git branch and loading the appropriate .env file.

Environment mapping:
- dev/* branches → .env.dev (dev.tradingAgent@localhost)
- staging branch → .env.staging (test.tradingAgent@localhost)
- main branch → .env.prod (prod.tradingAgent@localhost)
"""

import os
import subprocess
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    # If python-dotenv is not installed, provide a no-op function
    def load_dotenv(*args, **kwargs):
        pass


def get_git_branch() -> Optional[str]:
    """
    Get the current git branch name.
    
    Returns:
        Branch name or None if not in a git repository or if git command fails
    """
    try:
        # Try to get branch from git command
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).parent.parent.parent.parent,
            timeout=5  # Add timeout to prevent hanging
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, PermissionError, OSError, subprocess.TimeoutExpired):
        # Fallback: try to get from environment variable (useful in CI/CD or containers)
        # This handles cases where git is not available or not accessible
        return os.getenv('GIT_BRANCH') or os.getenv('BRANCH_NAME') or os.getenv('AIRFLOW_ENV')


def get_environment_from_branch(branch: Optional[str]) -> str:
    """
    Determine environment name from git branch.
    
    Args:
        branch: Git branch name
        
    Returns:
        Environment name: 'dev', 'staging', or 'prod'
    """
    if not branch:
        # Default to dev if branch cannot be determined
        return 'dev'
    
    branch_lower = branch.lower()
    
    # Check for staging branch
    if branch_lower == 'staging':
        return 'staging'
    
    # Check for main/master branch
    if branch_lower in ('main', 'master'):
        return 'prod'
    
    # Check for dev branches (dev/* or starts with dev)
    if branch_lower.startswith('dev') or '/' in branch_lower:
        # If it's a branch like dev/DEV-4, extract the prefix
        if '/' in branch_lower:
            prefix = branch_lower.split('/')[0]
            if prefix == 'dev':
                return 'dev'
    
    # Default to dev for any other branch
    return 'dev'


def get_environment() -> str:
    """
    Get the current environment name based on git branch.
    
    Returns:
        Environment name: 'dev', 'staging', or 'prod'
    """
    branch = get_git_branch()
    return get_environment_from_branch(branch)


def get_requirements_file() -> Path:
    """
    Get the path to the environment-specific requirements file.
    
    Returns:
        Path to requirements file (requirements-dev.txt, requirements-staging.txt, or requirements-prod.txt)
        Falls back to requirements.txt if environment-specific file doesn't exist
    """
    env = get_environment()
    project_root = Path(__file__).parent.parent.parent
    
    if env == 'dev':
        req_file = project_root / 'requirements-dev.txt'
    elif env == 'staging':
        req_file = project_root / 'requirements-staging.txt'
    else:  # prod
        req_file = project_root / 'requirements-prod.txt'
    
    # Fallback to base requirements.txt if environment-specific file doesn't exist
    if not req_file.exists():
        return project_root / 'requirements.txt'
    
    return req_file


def load_environment_config(env_name: Optional[str] = None) -> None:
    """
    Load environment variables from the appropriate .env file based on branch.
    
    Args:
        env_name: Optional environment name ('dev', 'staging', 'prod').
                 If None, will be determined from git branch.
    """
    if env_name is None:
        branch = get_git_branch()
        env_name = get_environment_from_branch(branch)
    
    # Get project root directory (parent of src/)
    project_root = Path(__file__).parent.parent.parent
    
    # Construct .env file path
    env_file = project_root / f'.env.{env_name}'
    
    # Load the environment-specific .env file
    if env_file.exists():
        load_dotenv(env_file, override=True)
        print(f"Loaded environment configuration from: {env_file.name}")
    else:
        print(f"Warning: Environment file {env_file.name} not found. Using system environment variables.")
    
    # Also load base .env if it exists (for shared variables)
    base_env_file = project_root / '.env'
    if base_env_file.exists():
        load_dotenv(base_env_file, override=False)


# Auto-load environment configuration when module is imported
# This ensures environment variables are available throughout the application
load_environment_config()
