# Wheel Building Guide

This project uses setuptools to build environment-specific wheels for the `trading_agent` package. Airflow containers automatically install the appropriate wheel based on their environment.

## Quick Start

### Building Wheels

Build a wheel - environment is automatically detected from git branch:

```bash
# Auto-detect environment from current branch and build
./build-wheel.sh

# Or explicitly specify environment (overrides auto-detection)
./build-wheel.sh dev
./build-wheel.sh staging
./build-wheel.sh prod
```

**Auto-detection rules:**
- `dev/*` branches → builds `trading_agent-dev-{version}.whl`
- `staging` branch → builds `trading_agent-staging-{version}.whl`
- `main`/`master` branch → builds `trading_agent-prod-{version}.whl`

**Note:** setuptools converts hyphens to underscores in package names, so the actual wheel files are:
- `trading_agent_dev-{version}-py3-none-any.whl` (for dev)
- `trading_agent_staging-{version}-py3-none-any.whl` (for staging)
- `trading_agent_prod-{version}-py3-none-any.whl` (for prod)

### Installing Wheels for Airflow

After building a wheel, copy it to the Airflow wheels directory:

```bash
# Auto-detect environment from current branch and install
.ops/.airflow/install-wheel.sh

# Or explicitly specify environment
.ops/.airflow/install-wheel.sh dev
.ops/.airflow/install-wheel.sh staging
.ops/.airflow/install-wheel.sh prod
```

This copies the wheel from `dist/` to `.ops/.airflow/wheels/` where Airflow containers can find it.

## How It Works

### Build Process

1. **Environment Detection**: The `setup.py` reads the `ENV` environment variable (dev/staging/prod)
2. **Package Naming**: Creates a package named `trading_agent-{env}` (e.g., `trading_agent-dev`)
3. **Dependencies**: Includes base `requirements.txt` plus environment-specific `requirements-{env}.txt`
4. **Wheel Output**: Builds a wheel in `dist/` directory

### Airflow Integration

1. **Wheel Directory**: Airflow containers mount `.ops/.airflow/wheels/` at `/opt/airflow/wheels`
2. **Auto-Installation**: On container startup, Airflow:
   - Reads `AIRFLOW_ENV` environment variable (dev/staging/prod)
   - Finds the latest matching wheel: `trading_agent-{AIRFLOW_ENV}-*.whl`
   - Installs it using `pip install`
3. **No Source Mounting**: Source code is no longer mounted; only wheels are installed

### Environment Mapping

- **airflow-dev** (port 8082) → `trading_agent-dev-*.whl`
- **airflow-test** (port 8083) → `trading_agent-staging-*.whl`
- **airflow-prod** (port 8084) → `trading_agent-prod-*.whl`

## Workflow

### Development Workflow

1. Make code changes
2. Build wheel: `./build-wheel.sh dev`
3. Install for Airflow: `.ops/.airflow/install-wheel.sh dev`
4. Restart Airflow container: `docker restart airflow-dev`

### Staging/Production Workflow

1. Build wheel: `./build-wheel.sh staging` (or `prod`)
2. Install for Airflow: `.ops/.airflow/install-wheel.sh staging` (or `prod`)
3. Restart Airflow container: `docker restart airflow-test` (or `airflow-prod`)

## Files

- **`setup.py`**: Setuptools configuration for building wheels
- **`build-wheel.sh`**: Script to build environment-specific wheels
- **`.ops/.airflow/install-wheel.sh`**: Script to copy wheels to Airflow wheels directory
- **`.ops/.airflow/wheels/`**: Directory where Airflow looks for wheels
- **`dist/`**: Directory where built wheels are stored

## Notes

- Wheels include all dependencies from `requirements.txt` and environment-specific requirements
- The version comes from `src/trading_agent/__init__.py` (`__version__`)
- Airflow containers automatically select the correct wheel based on `AIRFLOW_ENV`
- If no matching wheel is found, Airflow will log a warning but continue to start
