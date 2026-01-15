# Setting Up Infra-Platform Pipeline in Jenkins

This guide explains how to create a separate Jenkins multibranch pipeline for infrastructure changes.

## Overview

The project has two separate pipelines:

1. **TradingPythonAgent** (Application Pipeline)
   - Uses `Jenkinsfile`
   - Triggers on all code changes
   - Builds and validates trading_agent application code

2. **infra-platform** (Infrastructure Pipeline)
   - Uses `Jenkinsfile.infra-platform`
   - Should trigger only on `.ops/` directory changes
   - Validates and builds infrastructure components

## Setup Steps

### 1. Create New Multibranch Pipeline Job

1. In Jenkins, click **"New Item"**
2. Enter name: **`infra-platform`**
3. Select **"Multibranch Pipeline"**
4. Click **"OK"**

### 2. Configure Branch Sources

1. In the job configuration, go to **"Branch Sources"**
2. Click **"Add source"** → Select **"Git"**
3. Configure:
   - **Project Repository**: `https://github.com/vittorioapi91/TradingPythonAgent.git`
   - **Credentials**: Add your GitHub credentials if needed
   - **Behaviors**:
     - Click **"Add"** → **"Filter by name (with wildcards)"**
     - **Include**: `main`, `staging`, `dev/*`
     - **Exclude**: Leave empty
   - **Property strategy**: Select **"All branches get the same properties"**

### 3. Configure Build Configuration

1. In **"Build Configuration"** section:
   - **Mode**: Select **"by Jenkinsfile"**
   - **Script Path**: Enter **`Jenkinsfile.infra-platform`** (not `Jenkinsfile`)

### 4. Configure Build Triggers (Optional but Recommended)

1. In **"Build Triggers"** section:
   - Enable **"Build whenever a SNAPSHOT dependency is built"** (if you want it to build after main pipeline)
   - Or use **"Poll SCM"** with schedule: `H/15 * * * *` (every 15 minutes) to check for changes

### 5. Configure Path Filter (Important)

To ensure the pipeline only runs when `.ops/` changes:

1. In **"Behaviors"** section (under Branch Sources):
   - Click **"Add"** → **"Filter by name (with wildcards)"**
   - **Include**: `main`, `staging`, `dev/*`
   - Click **"Add"** → **"Path filter"**
   - **Include**: `.ops/**`
   - **Exclude**: Leave empty

   **Note**: Path filters in Jenkins multibranch pipelines work at the branch level. For more precise control, you may need to use a scripted pipeline or webhook triggers.

### 6. Alternative: Use Webhook Triggers

For more precise control, configure GitHub webhooks:

1. In GitHub repository settings → **Webhooks**
2. Add webhook pointing to Jenkins
3. Configure Jenkins to trigger `infra-platform` pipeline only when `.ops/` files change

### 7. Save and Scan

1. Click **"Save"**
2. Jenkins will automatically scan for branches
3. You should see branches: `main`, `staging`, and any `dev/*` branches
4. Each branch will use `Jenkinsfile.infra-platform` for its pipeline

## Verification

After setup, you should see:

- **TradingPythonAgent** pipeline with branches (uses `Jenkinsfile`)
- **infra-platform** pipeline with branches (uses `Jenkinsfile.infra-platform`)

## Troubleshooting

### Pipeline doesn't appear
- Check that `Jenkinsfile.infra-platform` exists in the repository
- Verify branch source configuration
- Check Jenkins logs: **Manage Jenkins** → **System Log**

### Pipeline runs on all changes, not just `.ops/`
- Path filters in multibranch pipelines may not work as expected
- Consider using webhook triggers or scripted pipeline logic
- Alternative: Use a separate repository for `.ops/` (future plan)

### Pipeline uses wrong Jenkinsfile
- Verify **"Script Path"** is set to `Jenkinsfile.infra-platform`
- Re-scan branches after changing configuration

## Future: Separate Repository

As mentioned in the project plan, `.ops/` will eventually be moved to a separate repository. When that happens:

1. Create a new Jenkins job pointing to the infrastructure repository
2. Use `Jenkinsfile.infra-platform` from that repository
3. Configure separate branch sources for dev/staging/main

## Related Files

- `Jenkinsfile.infra-platform`: Infrastructure pipeline definition
- `Jenkinsfile`: Application pipeline definition
- `.ops/.docker/docker-compose.infra-platform.yml`: Infrastructure services
