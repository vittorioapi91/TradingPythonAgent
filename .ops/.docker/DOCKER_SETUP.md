# Docker Setup for ML Workflow Monitoring

This guide explains how to run Grafana, Prometheus, MLflow, Airflow, PostgreSQL, and Feast using Docker for the macro cycle HMM model.

## Quick Start

### 1. Start All Services

```bash
cd .ops/.docker
docker-compose up -d
```

This will start:
- **Grafana** on http://localhost:3000 (admin/2014)
- **Prometheus** on http://localhost:9090
- **MLflow** on http://localhost:55000
- **Airflow** on http://localhost:8080
- **PostgreSQL** on localhost:55432 (container `postgres`)
- **Feast** CLI container (long-running, for feature management)

### 2. Access Services

- **Grafana**: http://localhost:3000
  - Username: `admin`
  - Password: `2014`

- **Prometheus**: http://localhost:9090

- **MLflow**: http://localhost:55000

### 3. Configure Grafana

Grafana is automatically configured with:
- Prometheus datasource (pre-configured)
- HMM monitoring dashboards (auto-imported)

Dashboards are located in the `Macro Cycle HMM` folder.

## Services

### Grafana

Grafana is configured with:
- **Provisioned datasource**: Prometheus (http://prometheus:9090)
- **Auto-imported dashboards**: From `grafana_dashboards/` directory
- **Persistent storage**: Data stored in Docker volume `grafana-data`

**Customization:**
- Edit `grafana/provisioning/datasources/prometheus.yml` for datasource config
- Edit `grafana/provisioning/dashboards/dashboard.yml` for dashboard provisioning
- Add custom dashboards to `grafana_dashboards/` directory

### Prometheus

Prometheus is configured to scrape:
- **Prometheus itself**: localhost:9090
- **HMM model metrics**: host.docker.internal:8000 (when training script runs)
- **MLflow metrics**: mlflow:5000

**Configuration:**
- Main config: `prometheus.yml`
- Data retention: 30 days
- Storage: Docker volume `prometheus-data`

**For Kubernetes:**
Uncomment the Kubernetes service discovery section in `prometheus.yml` and remove the static config.

### MLflow

MLflow tracking server with:
- **Backend store**: SQLite (default) or PostgreSQL (configured)
- **Artifact store**: Local filesystem
- **Port**: 5000

**To use PostgreSQL:**
1. Uncomment PostgreSQL service in `docker-compose.override.yml.example`
2. Copy to `docker-compose.override.yml`
3. Update MLflow environment variables

### Airflow

Airflow is included as an optional orchestration service:

- **Port**: 8080  
- **URL**: http://localhost:8080  
- **Home (in container)**: `/opt/airflow`  
- **DAGs folder in container**: `/opt/airflow/dags`  
- **DAGs mapped from**: `.ops/.airflow/dags`  
- **Command**: `airflow db migrate && airflow standalone` (runs webserver + scheduler)

To access Airflow:

1. Ensure environment variables in `.ops/.airflow/QUICK_START.md` are set (especially `AIRFLOW_HOME` and `AIRFLOW__CORE__DAGS_FOLDER`) if you also run Airflow outside Docker.
2. Start services with `docker-compose up -d`.
3. Open `http://localhost:8080` in your browser.

See `.ops/.airflow/QUICK_START.md` for detailed Airflow setup and authentication.

### PostgreSQL

PostgreSQL is used for the macro databases (`fred`, `bis`, `bls`, `eurostat`, `imf`, etc.):

- **Port**: 55432 on the host (mapped to 5432 in the container)
- **Container name**: `postgres`
- **Default user**: `tradingAgent`
- **Password**: from host `POSTGRES_PASSWORD` (or `tradingAgent` if not set)
- **Data**: stored in Docker volume `postgres-data`

From the host, connect to the Docker Postgres instance using:

- `host=localhost`
- `port=55432`
- `user=tradingAgent`
- `password=$POSTGRES_PASSWORD`

You still need to create the individual databases inside Postgres (once):

```bash
docker exec -it postgres psql -U tradingAgent -c "CREATE DATABASE fred;"
docker exec -it postgres psql -U tradingAgent -c "CREATE DATABASE bis;"
docker exec -it postgres psql -U tradingAgent -c "CREATE DATABASE bls;"
docker exec -it postgres psql -U tradingAgent -c "CREATE DATABASE eurostat;"
docker exec -it postgres psql -U tradingAgent -c "CREATE DATABASE imf;"
```

After that, the existing ingestion scripts will write into these databases as before.

### Feast

Feast is run via a lightweight CLI container:

- **Image**: `python:3.11-slim` with `feast[local]` installed  
- **Repo path in container**: `/workspace/.ops/.feast/feast_repo`  
- **Online store**: Local SQLite (as configured in `feature_store.yaml`)

On container start, it runs:

```bash
feast apply && tail -f /dev/null
```

You can exec into the container for ad-hoc Feast commands:

```bash
docker exec -it feast bash
cd /workspace/.ops/.feast/feast_repo
feast materialize-incremental $(date +%Y-%m-%d)
```

> **Note:** Kubeflow and KServe remain Kubernetes-native components. Use the manifests in `.ops/.kubernetes/` and the Kubeflow/KServe READMEs for deploying them to a Kubernetes cluster; they are not run directly via `docker-compose`.

## Custom Configuration

### Override Docker Compose

Create `docker-compose.override.yml` for custom settings:

```bash
cp docker-compose.override.yml.example docker-compose.override.yml
# Edit docker-compose.override.yml with your settings
```

### Change Grafana Password

Edit `docker-compose.yml` or create override:

```yaml
services:
  grafana:
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=your_secure_password
```

### Add Custom Dashboards

1. Create JSON dashboard files in `grafana_dashboards/`
2. Restart Grafana: `docker-compose restart grafana`
3. Dashboards will be auto-imported

### Configure Prometheus Scraping

Edit `prometheus.yml` to add new scrape targets:

```yaml
scrape_configs:
  - job_name: 'my-service'
    static_configs:
      - targets: ['host.docker.internal:8080']
```

## Running Training Script with Docker Services

When running the training script, ensure it can reach Prometheus:

```bash
# Set environment variables
export POSTGRES_PASSWORD='your_password'
export MLFLOW_TRACKING_URI='http://localhost:5000'
export PROMETHEUS_PORT=8000

# Run training (metrics will be scraped by Prometheus)
python training_script.py \
    --series-ids GDP UNRATE CPIAUCSL \
    --prometheus-port 8000
```

Prometheus will scrape metrics from `host.docker.internal:8000` when the training script is running.

## Docker Commands

### Start services
```bash
docker-compose up -d
```

### Stop services
```bash
docker-compose down
```

### View logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f grafana
docker-compose logs -f prometheus
docker-compose logs -f mlflow
```

### Restart a service
```bash
docker-compose restart grafana
```

### Remove all data (fresh start)
```bash
docker-compose down -v
```

### Update services
```bash
docker-compose pull
docker-compose up -d
```

## Troubleshooting

### Grafana can't connect to Prometheus

1. Check Prometheus is running: `docker-compose ps`
2. Check Prometheus URL: http://localhost:9090
3. Verify datasource in Grafana: Configuration → Data Sources → Prometheus
4. Test connection in Grafana datasource settings

### Prometheus can't scrape metrics

1. Ensure training script is running with `--prometheus-port 8000`
2. Check metrics endpoint: http://localhost:8000/metrics
3. For Docker Desktop, use `host.docker.internal:8000`
4. For Linux, use host IP or add network mode

### Dashboards not appearing

1. Check dashboard files in `grafana_dashboards/` are valid JSON
2. Check Grafana logs: `docker-compose logs grafana`
3. Verify provisioning config: `grafana/provisioning/dashboards/dashboard.yml`
4. Manually import: Dashboards → Import → Upload JSON

### Port conflicts

If ports are already in use, modify `docker-compose.yml`:

```yaml
services:
  grafana:
    ports:
      - "3001:3000"  # Use port 3001 instead
```

## Network Configuration

Services communicate on the `monitoring` Docker network:
- Grafana → Prometheus: `http://prometheus:9090`
- Prometheus → HMM metrics: `host.docker.internal:8000` (from host)
- External access: Use published ports (3000, 9090, 5000)

## Data Persistence

All data is stored in Docker volumes:
- `grafana-data`: Grafana dashboards, users, settings
- `prometheus-data`: Prometheus time-series data
- `mlflow-data`: MLflow artifacts and database

To backup:
```bash
docker run --rm -v docker_grafana-data:/data -v $(pwd):/backup alpine tar czf /backup/grafana-backup.tar.gz /data
```

To restore:
```bash
docker run --rm -v docker_grafana-data:/data -v $(pwd):/backup alpine tar xzf /backup/grafana-backup.tar.gz -C /
```

## Production Considerations

For production:
1. Use strong passwords (set in environment variables)
2. Enable authentication for all services
3. Use PostgreSQL for MLflow backend
4. Use S3/minio for MLflow artifacts
5. Set up SSL/TLS certificates
6. Configure proper resource limits
7. Set up backup strategy
8. Use secrets management for passwords
9. Enable Prometheus alerting
10. Set up log aggregation

