# Docker Configuration

This folder contains all Docker-related files for running the ML workflow monitoring services (Grafana, Prometheus, MLflow, and Airflow).

## Files

- **`docker-compose.yml`**: Main Docker Compose configuration
  - Defines services: Grafana, Prometheus, MLflow, Airflow
  - Configures networking and volumes
  - Sets up service dependencies

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
# From .ops/.docker directory
./start-docker-monitoring.sh

# Or manually
docker-compose up -d
```

### Stop Services

```bash
# From .ops/.docker directory
./stop-docker-monitoring.sh

# Or manually
docker-compose down
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
services:
  grafana:
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=your_password
```

**Use PostgreSQL for MLflow:**
```yaml
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

All paths in `docker-compose.yml` are relative to the `.ops/.docker/` folder:
- `../.prometheus/` → Points to `.ops/.prometheus/`
- `../.grafana/` → Points to `.ops/.grafana/`
- `../.grafana/dashboards/` → Points to `.ops/.grafana/dashboards/`

## Commands

All commands should be run from the `.docker/` directory:

```bash
cd docker

# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Restart a service
docker-compose restart grafana

# View service status
docker-compose ps

# Remove all data (fresh start)
docker-compose down -v
```

## Troubleshooting

See `DOCKER_SETUP.md` for detailed troubleshooting guide.

Common issues:
- Port conflicts: Change ports in `docker-compose.yml`
- Permission errors: Check Docker Desktop is running
- Volume mount errors: Verify paths are correct relative to `docker/` folder

