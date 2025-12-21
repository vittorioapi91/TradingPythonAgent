# Kubernetes Monitoring Stack (Cloud-Ready)

This folder contains Kubernetes manifests that mirror the Docker Compose stack in `.ops/.docker/docker-compose.yml`.

It is designed to be:
- **Cloud-ready**: can be applied to any Kubernetes cluster (GKE, EKS, AKS, etc.)
- **Docker-friendly**: can be run locally using a Docker-backed cluster such as **kind** or **k3d**.

## Components

The monitoring stack includes:

- **Prometheus** (`prometheus` Deployment + Service)
- **Grafana** (`grafana` Deployment + Service)
- **MLflow** (`mlflow` Deployment + Service)
- **Airflow** (`airflow` Deployment + Service)
- **PostgreSQL** (`postgres` Deployment + Service)
- **Feast**: managed via CLI or jobs using the `.ops/.feast` repo
- **KServe / Kubeflow**: separate manifests (see existing `kserve-inference-service.yaml` and Kubeflow docs)

All resources are created in the `trading-monitoring` namespace.

## Files

- `monitoring-stack.yaml`  
  Deploys:
  - Namespace `trading-monitoring`
  - Prometheus Deployment + Service + ConfigMap
  - Grafana Deployment + Service
  - MLflow Deployment + Service
  - Airflow Deployment + Service
  - PostgreSQL Deployment + Service + Secret

- `prometheus-service.yaml`, `prometheus-scrape-config.yaml`  
  Additional Prometheus configuration for scraping the model metrics (used previously; you can keep or consolidate with `monitoring-stack.yaml`).

- `kserve-inference-service.yaml`  
  Example **KServe** InferenceService for serving the macro HMM model.

## Running Kubernetes *in* Docker (kind) for Kubeflow / KServe

In this project, Kubernetes is used primarily for **Kubeflow Pipelines** and
**KServe**. Monitoring (Grafana, Prometheus, MLflow, Airflow, Postgres) is
handled by Docker Compose in `.ops/.docker`.

To bring up a local kind cluster and install the Kubernetes Dashboard, run:

```bash
bash .ops/.kubernetes/start-kubernetes.sh
```

This script will:

- Ensure `kubectl` and `kind` are installed.
- Create (or reuse) a kind cluster named `trading-cluster`.
- Set the current context to `kind-trading-cluster`.
- Install or update the Kubernetes Dashboard.
- Create an `admin-user` ServiceAccount with `cluster-admin` privileges.
- Print commands so you can:
  - Generate a dashboard login token.
  - Start `kubectl proxy`.
  - Open the dashboard URL in your browser.

You can re-run the script any time; it is idempotent.

## Cloud-Ready Notes

- For cloud clusters (GKE/EKS/AKS):
  - `LoadBalancer` Services will get cloud load balancers automatically.
  - Replace `emptyDir` volumes with proper `PersistentVolumeClaim`s for production.
  - Externalize secrets (e.g., PostgreSQL password) into a secrets manager or external Secret manifests.

- PostgreSQL:
  - Currently uses `emptyDir` for simplicity. For production, define a `StorageClass` and PVC.

- Airflow:
  - Uses SQLite for metadata in this example. For production, point `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` to a managed Postgres instance or the `postgres` service.

## Relationship to Docker Compose

- **Docker Compose** (`.ops/.docker/docker-compose.yml`):
  - Best for quick local dev, everything on a single Docker host.

- **Kubernetes manifests** (`.ops/.kubernetes/*.yaml`):
  - Best for cloud deployment or local kind/k3d clusters.
  - Mirrors the same services but uses Deployments + Services instead of Compose.

You can choose:
- **Local dev**: use Docker Compose.
- **Cloud / local k8s**: use these Kubernetes manifests.


