# Docker Configuration

This folder contains Docker Compose configurations for infrastructure and application services.

## Files

- **`docker-compose.infra-platform.yml`**: Infrastructure services Docker Compose configuration
  - Defines all infrastructure services: Grafana, Prometheus, MLflow, Airflow (dev/test/prod), PostgreSQL, Redis, Jenkins
  - Configures networking and volumes
  - Sets up service dependencies
  - **Use this for infrastructure deployment**

- **`docker-compose.yml`**: Application services Docker Compose configuration
  - For application-specific services (future use)
  - Currently minimal, ready for application services
  - **Use this for application-specific services**

- **`docker-compose.override.yml.example`**: Example override file
  - Template for custom configurations
  - Copy to `docker-compose.override.yml` to use

- **`DOCKER_SETUP.md`**: Detailed Docker setup documentation
  - Service configuration
  - Troubleshooting guide
  - Production considerations

- **`DOCKER_START.md`**: Quick guide for starting Docker
  - How to start Docker daemon
  - Verification steps
  - Common issues

## Quick Start

### Start Services

```bash
# From .ops/.docker directory (recommended - starts both infra and app services)
./start-docker-monitoring.sh

# Or manually start infrastructure services
docker-compose -f docker-compose.infra-platform.yml up -d

# Start application services (if any)
docker-compose -f docker-compose.yml up -d
```

### Stop Services

```bash
# From .ops/.docker directory (stops both infra and app services)
./stop-docker-monitoring.sh

# Or manually stop services
docker-compose -f docker-compose.infra-platform.yml down
docker-compose -f docker-compose.yml down
```

## Services

### Grafana
- **Port**: 3000
- **URL**: http://localhost:3000
- **Credentials**: admin/admin
- **Volumes**: 
  - `../grafana/provisioning` → Auto-configures datasources and dashboards
  - `../grafana_dashboards` → Dashboard JSON files

### Prometheus
- **Port**: 9090
- **URL**: http://localhost:9090
- **Config**: `../prometheus/prometheus.yml`
- **Data**: Stored in Docker volume `prometheus-data`

### MLflow
- **Port**: 5000
- **URL**: http://localhost:5000
- **Backend**: SQLite (default) or PostgreSQL (configurable)
- **Data**: Stored in Docker volume `mlflow-data`

### Airflow
- **Port**: 8080
- **URL**: http://localhost:8080
- **Home (in container)**: `/opt/airflow`
- **DAGs folder (in container)**: `/opt/airflow/dags` (mapped from `.ops/.airflow/dags`)
- **Command**: `airflow db migrate && airflow standalone`

See `.ops/.airflow/QUICK_START.md` for Airflow environment variables, admin user, and authentication details.

## Custom Configuration

### Using Override File

1. Copy the example:
   ```bash
   cp docker-compose.override.yml.example docker-compose.override.yml
   ```

2. Edit `docker-compose.override.yml` with your settings

3. Restart services:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Common Customizations

**Change Grafana password:**
```yaml
# In docker-compose.infra-platform.yml
services:
  grafana:
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=your_password
```

**Use PostgreSQL for MLflow:**
```yaml
# In docker-compose.infra-platform.yml
services:
  mlflow:
    environment:
      - MLFLOW_BACKEND_STORE_URI=postgresql://user:pass@postgres:5432/mlflow
    depends_on:
      - postgres
  
  postgres:
    image: postgres:15
    environment:
      - POSTGRES_USER=mlflow
      - POSTGRES_PASSWORD=mlflow
      - POSTGRES_DB=mlflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
```

## Path References

All paths in `docker-compose.infra-platform.yml` are relative to the `.ops/.docker/` folder:
- `../.prometheus/` → Points to `.ops/.prometheus/`
- `../.grafana/` → Points to `.ops/.grafana/`
- `../.grafana/dashboards/` → Points to `.ops/.grafana/dashboards/`
- `../../src` → Points to project `src/` directory (mounted in Airflow containers)

## Commands

All commands should be run from the `.ops/.docker/` directory:

```bash
cd .ops/.docker

# Start infrastructure services
docker-compose -f docker-compose.infra-platform.yml up -d

# Stop infrastructure services
docker-compose -f docker-compose.infra-platform.yml down

# View logs
docker-compose -f docker-compose.infra-platform.yml logs -f

# Restart a service
docker-compose -f docker-compose.infra-platform.yml restart grafana

# View service status
docker-compose -f docker-compose.infra-platform.yml ps

# Remove all data (fresh start)
docker-compose -f docker-compose.infra-platform.yml down -v
```

## Troubleshooting

See `DOCKER_SETUP.md` for detailed troubleshooting guide.

Common issues:
- Port conflicts: Change ports in `docker-compose.infra-platform.yml`
- Permission errors: Check Docker Desktop is running
- Volume mount errors: Verify paths are correct relative to `.ops/.docker/` folder

## Infrastructure vs Application Services

This directory contains two separate Docker Compose files:

1. **`docker-compose.infra-platform.yml`**: All infrastructure services
   - Airflow (dev, test, prod instances)
   - Grafana, Prometheus
   - PostgreSQL, Redis
   - Jenkins, MLflow, Feast
   - Managed by infrastructure pipeline (`Jenkinsfile.infra-platform`)

2. **`docker-compose.yml`**: Application services
   - For application-specific containers (future use)
   - Managed by application pipeline (`Jenkinsfile`)

This separation allows:
- Independent deployment of infrastructure and application
- Separate CI/CD pipelines
- Future migration of `.ops/` to a separate repository

