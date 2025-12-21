# HMM Model Training Job

This directory contains Kubernetes manifests to run the HMM model calibration/training as a Kubernetes Job.

## Files

- `hmm-model-training-job.yaml` - Kubernetes Job manifest for model training
- `Dockerfile.model-training` - Dockerfile to build the training container image
- `build-model-image.sh` - Script to build the Docker image locally
- `load-model-image-to-kind.sh` - Script to load the built image into kind cluster

## Prerequisites

1. **Kubernetes cluster running** (e.g., kind cluster from `start-kubernetes.sh`)
2. **Monitoring stack deployed** (PostgreSQL, MLflow services in `trading-monitoring` namespace)
3. **Docker** for building the container image

## Setup Instructions

### 1. Build Docker Image

Build the Docker image locally (only needed when code/dependencies change):

```bash
bash .ops/.kubernetes/build-model-image.sh
```

This will:
- Build a Docker image with all dependencies and source code
- Save it locally for loading into kind cluster

### 2. Load Image into Kind Cluster

Load the built image into your kind cluster:

```bash
bash .ops/.kubernetes/load-model-image-to-kind.sh
```

This will:
- Check if the image exists locally
- Load it into the kind cluster (no registry needed for local development)

**Note**: You only need to rebuild the image when your code or dependencies change. You can load the same image multiple times into the cluster for different deployments.

### 3. Deploy the Training Job

```bash
kubectl apply -f .ops/.kubernetes/hmm-model-training-job.yaml
```

### 4. Monitor Job Execution

```bash
# Check job status
kubectl get jobs -n trading-monitoring

# View job logs
kubectl logs -n trading-monitoring job/hmm-model-calibration -f

# Describe job for details
kubectl describe job -n trading-monitoring hmm-model-calibration
```

### 5. Check Job Pods

```bash
# List pods created by the job
kubectl get pods -n trading-monitoring -l app=hmm-model,component=training

# View pod logs
kubectl logs -n trading-monitoring <pod-name> -f
```

## Configuration

The job is configured via environment variables in the manifest. Key settings:

### Data Parameters
- `SERIES_IDS`: FRED series IDs (default: "GDP UNRATE CPIAUCSL")
- `START_DATE`: Start date for data (default: "2000-01-01")
- `FEATURE_METHOD`: Feature engineering method (default: "pct_change")

### Model Parameters
- `N_REGIMES`: Number of HMM regimes (default: "4")
- `COVARIANCE_TYPE`: Covariance type (default: "full")
- `RANDOM_STATE`: Random seed (default: "42")

### Service Connections
- `DB_HOST`: PostgreSQL service (default: "postgres.trading-monitoring.svc.cluster.local")
- `MLFLOW_TRACKING_URI`: MLflow service (default: "http://mlflow.trading-monitoring.svc.cluster.local:5000")
- Database password is read from `postgres-secret` Secret

## Customizing the Job

### Modify Parameters

Edit `hmm-model-training-job.yaml` and change environment variable values:

```yaml
env:
  - name: SERIES_IDS
    value: "GDP UNRATE CPIAUCSL FEDFUNDS"  # Add more series
  - name: N_REGIMES
    value: "6"  # Try different number of regimes
```

### Add AIC-Based State Selection

To use AIC-based state selection, modify the args in the Job:

```yaml
args:
  - --select-by-aic
  - --min-regimes
  - "2"
  - --max-regimes
  - "8"
  # ... other args
```

### Schedule as CronJob

To run training periodically, convert to a CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: hmm-model-calibration-cron
  namespace: trading-monitoring
spec:
  schedule: "0 2 * * 0"  # Run every Sunday at 2 AM
  jobTemplate:
    # ... use the same template from the Job
```

## Troubleshooting

### Job Fails to Start

1. **Check if image exists locally:**
   ```bash
   docker images | grep hmm-model-training
   ```

2. **Build image if missing:**
   ```bash
   bash .ops/.kubernetes/build-model-image.sh
   ```

3. **Load image into cluster:**
   ```bash
   bash .ops/.kubernetes/load-model-image-to-kind.sh
   ```

2. **Check pod events:**
   ```bash
   kubectl describe pod -n trading-monitoring <pod-name>
   ```

### Connection Issues

1. **Verify services are running:**
   ```bash
   kubectl get svc -n trading-monitoring
   kubectl get pods -n trading-monitoring
   ```

2. **Test database connection:**
   ```bash
   kubectl run -it --rm debug --image=postgres:15 --restart=Never -n trading-monitoring -- \
     psql -h postgres.trading-monitoring.svc.cluster.local -U tradingAgent -d fred
   ```

3. **Test MLflow connection:**
   ```bash
   kubectl run -it --rm debug --image=curlimages/curl --restart=Never -n trading-monitoring -- \
     curl http://mlflow.trading-monitoring.svc.cluster.local:5000
   ```

### Resource Issues

If the job is OOMKilled or CPU throttled, adjust resources in the manifest:

```yaml
resources:
  requests:
    cpu: "1000m"  # Increase if needed
    memory: "4Gi"  # Increase if needed
  limits:
    cpu: "4000m"
    memory: "8Gi"
```

## Cleanup

```bash
# Delete the job (keeps completed pods for debugging per ttlSecondsAfterFinished)
kubectl delete job -n trading-monitoring hmm-model-calibration

# Force delete pods if needed
kubectl delete pods -n trading-monitoring -l app=hmm-model,component=training
```

