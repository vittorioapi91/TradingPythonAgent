## Kubernetes + Kubeflow + KServe QUICK START

This project uses:

- **Docker Compose** (in `.ops/.docker`) for Grafana, Prometheus, MLflow, Airflow, Postgres, Redis, Feast.
- **Kubernetes (kind)** for **Kubeflow Pipelines** and **KServe** only.

This guide summarises the operations we performed to install and configure:
the Kubernetes cluster, Dashboard, cert-manager, KServe and Kubeflow Pipelines.

---

### 1. Install CLI tools

On macOS (with Homebrew):

```bash
brew install kubectl
brew install kind
```

Verify:

```bash
kubectl version --client
kind version
```

---

### 2. Create a Kubernetes-in-Docker cluster + Dashboard

From the project root (`/Users/Snake91/CursorProjects/TradingPythonAgent`):

```bash
bash .ops/.kubernetes/start-kubernetes.sh
```

This script:

- Checks that `kubectl` and `kind` are available.
- Creates (or reuses) a kind cluster named `trading-cluster`.
- Sets the current context to `kind-trading-cluster`.
- Installs/updates the **Kubernetes Dashboard** (`v2.7.0`).
- Creates an `admin-user` ServiceAccount bound to `cluster-admin`.
- Prints the exact commands to:
  - Generate a dashboard token:
    ```bash
    kubectl -n kubernetes-dashboard create token admin-user
    ```
  - Start the API proxy:
    ```bash
    kubectl proxy
    ```
  - Open the Dashboard:
    ```text
    http://127.0.0.1:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
    ```

You can re-run the script any time; it is idempotent.

---

### 3. Install cert-manager (required by KServe)

KServe relies on cert-manager CRDs and a webhook for TLS certificates.
Install cert-manager into its own namespace:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
```

Wait until all cert-manager pods are `Running`:

```bash
kubectl get pods -n cert-manager
```

Expected pods:

- `cert-manager`
- `cert-manager-cainjector`
- `cert-manager-webhook`

If any are not `Running`, describe them:

```bash
kubectl describe pod -n cert-manager cert-manager-webhook
kubectl logs -n cert-manager cert-manager-webhook
```

---

### 4. Install KServe

With cert-manager running, install KServe:

```bash
kubectl apply -f https://github.com/kserve/kserve/releases/download/v0.11.0/kserve.yaml
```

Check status:

```bash
kubectl get pods -n kserve
kubectl get crd | grep serving.kserve.io
```

If you previously saw errors like:

- `no matches for kind "Certificate" in version "cert-manager.io/v1"`
- `failed to call webhook "webhook.cert-manager.io": connect: connection refused`

they are resolved once cert-manager is fully up and KServe is re-applied.

---

### 5. Deploy KServe InferenceService from this repo

The project includes an example KServe **InferenceService** manifest:

- `.ops/.kserve/kserve-inference-service.yaml`

Apply it:

```bash
cd /Users/Snake91/CursorProjects/TradingPythonAgent
kubectl apply -f .ops/.kserve/kserve-inference-service.yaml
```

Then:

```bash
kubectl get inferenceservices -A
kubectl get pods -n <namespace-from-yaml>
```

Once the InferenceService is `Ready`, you can port-forward its Service to
call the model locally (see comments inside `kserve-inference-service.yaml`).

---

### 6. Install Kubeflow Pipelines (standalone)

Kubeflow Pipelines is deployed in two stages:

1. **Cluster-scoped resources** (CRDs, namespace, etc.):

   ```bash
   kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources?ref=master"
   ```

   This:
   - Creates the `kubeflow` namespace.
   - Installs the `Application` CRD (`app.k8s.io/v1beta1`).

2. **Dev environment components**:

   ```bash
   kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/dev?ref=master"
   ```

Check pod status:

```bash
kubectl get pods -n kubeflow
```

Initially pods will be in `ContainerCreating` while images are pulled.
After a few minutes they should become `Running`.

---

### 7. Access Kubeflow Pipelines UI

Once the `ml-pipeline-ui` Service is up:

```bash
kubectl get svc -n kubeflow ml-pipeline-ui
```

Port-forward to your local machine:

```bash
kubectl port-forward -n kubeflow svc/ml-pipeline-ui 8081:80
```

Then open:

```text
http://localhost:8081
```

This UI is what the `kubeflow_pipeline.py` code in this project can talk to.

---

### 8. Reminder: Monitoring stays on Docker Compose

To keep responsibilities clear:

- **Docker Compose** (`.ops/.docker/docker-compose.yml`):
  - Grafana, Prometheus, MLflow, Airflow, Postgres, Redis, Feast.
  - Start/stop with:
    ```bash
    cd .ops/.docker
    ./start-docker-monitoring.sh    # start
    ./stop-docker-monitoring.sh     # stop
    ```

- **Kubernetes / kind**:
  - Kubeflow Pipelines and KServe only.
  - Managed via `kubectl` and `.ops/.kubernetes/start-kubernetes.sh`.


