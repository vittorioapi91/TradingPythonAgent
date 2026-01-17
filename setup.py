"""
Setup configuration for TradingPythonAgent

This setup.py builds environment-specific wheels:
- trading_agent-dev-{version}.whl for development
- trading_agent-staging-{version}.whl for staging
- trading_agent-prod-{version}.whl for production

Usage:
    # Build for dev environment
    ENV=dev python setup.py bdist_wheel

    # Build for staging environment
    ENV=staging python setup.py bdist_wheel

    # Build for prod environment
    ENV=prod python setup.py bdist_wheel
"""

import os
from pathlib import Path
from setuptools import setup, find_packages

# Read the README file for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Get environment from environment variable (default to 'dev')
env = os.getenv("ENV", "dev").lower()
if env not in ["dev", "staging", "prod"]:
    env = "dev"

# Read version from package __init__.py
version_file = Path(__file__).parent / "src" / "trading_agent" / "__init__.py"
version = "0.1.0"  # Default version
if version_file.exists():
    with open(version_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("__version__"):
                version = line.split("=")[1].strip().strip('"').strip("'")
                break

# Read requirements.txt for dependencies
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    with open(requirements_file, "r", encoding="utf-8") as f:
        requirements = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]

# Environment-specific requirements files
env_requirements_file = Path(__file__).parent / f"requirements-{env}.txt"
if env_requirements_file.exists():
    with open(env_requirements_file, "r", encoding="utf-8") as f:
        env_requirements = [
            line.strip()
            for line in f
            if line.strip()
            and not line.strip().startswith("#")
            and not line.strip().startswith("-r")
        ]
        # Extend base requirements with environment-specific ones
        requirements.extend(env_requirements)

# Create environment-specific package name
# Example: trading_agent-dev, trading_agent-staging, trading_agent-prod
package_name = f"trading_agent-{env}"

setup(
    name=package_name,
    version=version,
    description="TradingPythonAgent - A Python trading agent package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Vittorio Apicella",
    author_email="apicellavittorio@hotmail.it",
    url="https://github.com/vittorioapi91/TradingPythonAgent",
    package_dir={"": "src"},
    packages=find_packages(where="src", include=["trading_agent*"]),
    python_requires=">=3.9",
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    # Include package data if needed
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json", "*.sql"],
    },
    # Entry points for command-line scripts (if needed)
    entry_points={
        "console_scripts": [
            "trading-agent=src.trading_agent.agent:main",
        ],
    },
)
