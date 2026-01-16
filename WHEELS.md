# Wheel Building Guide

This project uses setuptools to build environment-specific wheels for the `trading_agent` package. Airflow containers automatically install the appropriate wheel based on their environment.

## Quick Start

### Building Wheels

Build a wheel for a specific environment:

```bash
# Build for dev environment
./build-wheel.sh dev

# Build for staging environment
./build-wheel.sh staging

# Build for prod environment
./build-wheel.sh prod
```

The wheel will be created in `dist/` directory with the naming pattern:
- `trading_agent-dev-{version}-py3-none-any.whl`
- `trading_agent-staging-{version}-py3-none-any.whl`
- `trading_agent-prod-{version}-py3-none-any.whl`

### Installing Wheels for Airflow

After building a wheel, copy it to the Airflow wheels directory:

```bash
# Install dev wheel for Airflow
.ops/.airflow/install-wheel.sh dev

# Install staging wheel for Airflow
.ops/.airflow/install-wheel.sh staging

# Install prod wheel for Airflow
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
