# Kubernetes Dashboard Access

This directory contains a dedicated kubeconfig file for easy access to the Kubernetes dashboard.

## Quick Access

### Option 1: Use the setup script (Recommended)

```bash
# Run the setup script which configures everything
bash .ops/.kubernetes/start-kubernetes.sh
```

This will:
- Ensure the cluster exists
- Install/update the Kubernetes Dashboard with skip-login enabled
- Create admin user and permissions
- Start kubectl proxy
- Set up kubeconfig

### Option 2: Set KUBECONFIG manually

```bash
export KUBECONFIG=$(pwd)/.ops/.kubernetes/kubeconfig-dashboard.yaml
```

### Option 3: Use with kubectl commands directly

```bash
kubectl --kubeconfig=.ops/.kubernetes/kubeconfig-dashboard.yaml <command>
```

## Accessing the Dashboard

Once KUBECONFIG is set:

1. **Start kubectl proxy** (if not already running):
   ```bash
   kubectl proxy --port=8001
   ```

2. **Get dashboard login token**:
   ```bash
   kubectl -n kubernetes-dashboard create token admin-user
   ```

3. **Open dashboard URL in browser**:
   ```
   http://127.0.0.1:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
   ```

## Files

- `kubeconfig-dashboard.yaml` - Dedicated kubeconfig file for dashboard access (auto-generated)
- `start-kubernetes.sh` - Bootstrap script to create cluster, install dashboard, and configure everything

## Notes

- **The kubeconfig is automatically regenerated**: The `start-kubernetes.sh` script automatically updates the kubeconfig file from the current cluster state. This handles cases where the cluster was recreated (kind clusters can get new ports/certificates when recreated).
- **Skip-login is enabled**: The dashboard is configured to allow skipping the login page for local development convenience.
- The kubeconfig file is specific to the `kind-trading-cluster` context
- Make sure the cluster is running before using the dashboard
- The `admin-user` service account has cluster-admin privileges
- The proxy must be running on port 8001 for dashboard access

### Why regeneration is needed

When a kind cluster is deleted and recreated:
- The API server port can change (e.g., from `:58184` to `:58200`)
- Certificates are regenerated
- The kubeconfig needs to reflect these changes

The script handles this automatically, so you don't need to worry about it!

