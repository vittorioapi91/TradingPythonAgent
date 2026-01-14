# Jenkins CI/CD Setup

This directory contains scripts and configuration for deploying Jenkins with all plugins, credentials, and pipeline configuration.

## Quick Start

### Deploy Jenkins

```bash
# Deploy Jenkins with current configuration
./deploy-jenkins.sh

# Deploy with fresh install (removes existing data)
./deploy-jenkins.sh --fresh

# Create backup before deployment
./deploy-jenkins.sh --backup
```

The deployment script will:
1. Build the custom Jenkins Docker image (if needed)
2. Verify/initialize Jenkins data directory
3. Start Jenkins container
4. Wait for Jenkins to be ready
5. Display deployment summary

### Backup Jenkins Configuration

```bash
# Create a backup of current Jenkins state
./backup-jenkins.sh

# Restore from backup
./deploy-jenkins.sh --restore backups/jenkins_backup_YYYYMMDD_HHMMSS.tar.gz
```

## Jenkins Configuration

The current Jenkins setup includes:
- **106 plugins** (including Jira, GitHub Branch Source, Pipeline, etc.)
- **Multibranch Pipeline** for TradingPythonAgent
- **GitHub integration** with automatic branch discovery
- **JIRA integration** for issue validation
- **User authentication** configured
- **Periodic branch scanning** (every 5 minutes)

## Manual Setup

This guide explains how to set up Jenkins to automatically build and deploy the HMM model training Docker image to your Kubernetes cluster on every commit.

**Note**: Jenkins can run either:
- **Standalone** (recommended for local/kind clusters): Docker container or host service - see `setup-docker.sh`
- **Inside Kubernetes**: As a pod - see `jenkins-deployment.yaml` (optional)

Both approaches connect to your Kubernetes cluster to deploy jobs. For kind clusters, standalone Docker is usually simpler.

## Overview

The Jenkins pipeline:
1. Builds the Docker image on each commit
2. Tags it with build number and git commit SHA
3. Loads it into the kind cluster (`trading-cluster`)
4. Updates/deploys the Kubernetes Job

## Prerequisites

### 1. Jenkins Installation

#### Option A: Docker (Recommended for Local Development)

Use the setup script:

```bash
# Quick setup with all necessary configurations
bash .ops/.jenkins/setup-docker.sh
```

Or manually:

```bash
# Run Jenkins in Docker
docker run -d \
  --name jenkins \
  -p 8080:8080 \
  -p 50000:50000 \
  -v jenkins_home:/var/jenkins_home \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $(which kind):/usr/local/bin/kind \
  -v $(which kubectl):/usr/local/bin/kubectl \
  jenkins/jenkins:lts

# Get initial admin pa
ssword
docker exec jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

#### Option B: Homebrew (macOS)

```bash
brew install jenkins-lts
brew services start jenkins-lts
```

Access Jenkins at: `http://localhost:8080`

#### Option C: Inside Kubernetes (Optional)

If you prefer to run Jenkins as a Kubernetes pod:

```bash
kubectl apply -f .ops/.jenkins/jenkins-deployment.yaml
kubectl port-forward svc/jenkins 8080:8080 -n jenkins
```

**Note**: For kind clusters, Option A (Docker) is usually simpler and more reliable.

### 2. Jenkins Plugins

Install these plugins (Manage Jenkins → Plugins):
- **Docker Pipeline** - For Docker integration
- **Git** - For Git SCM
- **Kubernetes CLI** - For kubectl commands (optional)
- **Pipeline** - For Jenkinsfile support

### 3. Jenkins Configuration

#### Configure Docker Access

If Jenkins runs in Docker, it needs access to Docker on the host:

```bash
# Add Jenkins user to docker group (if Jenkins runs on host)
sudo usermod -aG docker jenkins

# Or use Docker socket mount (if Jenkins runs in Docker - see docker run command above)
```

#### Configure kubectl and kind Access

Jenkins needs access to kubectl and kind:

1. **If Jenkins runs on host**: Already available in PATH
2. **If Jenkins runs in Docker**: Mount binaries (see docker run command) or install in Jenkins container

#### Set KUBECONFIG (if needed)

If your kubeconfig is in a non-standard location:

1. Go to: **Manage Jenkins → Configure System → Global properties**
2. Add environment variable:
   - Name: `KUBECONFIG`
   - Value: `/path/to/your/kubeconfig` (or mount it in Docker container)

## Pipeline Setup

### Method 1: Pipeline Job (Recommended)

1. **Create New Item**:
   - Click "New Item"
   - Enter name: `hmm-model-training-pipeline`
   - Select "Pipeline"
   - Click OK

2. **Configure Pipeline**:
   - **Definition**: Pipeline script from SCM
   - **SCM**: Git
   - **Repository URL**: Your repository URL
   - **Credentials**: Add if repository is private
   - **Branches**: `*/main` or `*/master` (or `*` for all branches)
   - **Script Path**: `Jenkinsfile`

3. **Build Triggers**:
   - Check "GitHub hook trigger for GITScm polling" (if using GitHub webhooks)
   - Or check "Poll SCM" and set schedule: `H/5 * * * *` (every 5 minutes)

4. **Save** and click "Build Now"

### Method 2: Multibranch Pipeline

For automatic branch discovery:

1. **Create New Item**:
   - Select "Multibranch Pipeline"
   - Name: `hmm-model-training`

2. **Configure**:
   - **Branch Sources**: Add Git source
   - **Build Configuration**: Mode: "by Jenkinsfile"
   - **Script Path**: `Jenkinsfile`

## Git Webhooks (Optional)

For automatic builds on push:

### GitHub

1. Repository → Settings → Webhooks → Add webhook
2. **Payload URL**: `http://your-jenkins-url/github-webhook/`
3. **Content type**: `application/json`
4. **Events**: "Just the push event"

### GitLab

1. Repository → Settings → Webhooks
2. **URL**: `http://your-jenkins-url/project/hmm-model-training-pipeline`
3. **Trigger**: "Push events"

## Pipeline Details

### Image Tagging

Images are tagged with:
- **Build-specific**: `hmm-model-training:BUILD_NUMBER-COMMIT_SHA`
- **Latest**: `hmm-model-training:latest`

Example: `hmm-model-training:42-a1b2c3d`

### Kind Cluster Access

The pipeline uses `kind load docker-image` to load images into the cluster. This requires:
- `kind` binary available to Jenkins
- Access to the `trading-cluster` kind cluster
- Docker daemon access

### Kubernetes Job Update

The pipeline:
1. Updates the job's container image (if job exists)
2. Applies the job manifest (creates if doesn't exist)

## Troubleshooting

### Jenkins Can't Access Docker

**Error**: `docker: command not found` or permission denied

**Solution**:
```bash
# If Jenkins runs on host
sudo usermod -aG docker jenkins
sudo systemctl restart jenkins

# If Jenkins runs in Docker - ensure socket is mounted
# Check docker run command includes: -v /var/run/docker.sock:/var/run/docker.sock
```

### Jenkins Can't Access kubectl/kind

**Error**: `kubectl: command not found` or `kind: command not found`

**Solution**:
```bash
# Install in Jenkins container
docker exec -it jenkins bash
# Inside container:
#   curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
#   install kubectl /usr/local/bin/
#   # Similar for kind
```

Or mount binaries (recommended - see docker run command in prerequisites).

### Image Not Found in Cluster

**Error**: `ImagePullBackOff` or `ErrImagePull`

**Solution**:
- Verify image was loaded: `kind get images --name trading-cluster`
- Check image tag matches job specification
- Ensure `imagePullPolicy: Never` in job manifest (for kind clusters)

### Job Not Updating

**Error**: Job still uses old image

**Solution**:
```bash
# Manually delete and recreate job
kubectl delete job hmm-model-calibration -n trading-monitoring
kubectl apply -f .ops/.kubernetes/hmm-model-training-job.yaml
```

Or the pipeline will create it on the next run.

## Advanced Configuration

### Custom Build Arguments

Modify the Jenkinsfile to add build arguments:

```groovy
sh """
    docker build \
        --build-arg PYTHON_VERSION=3.11 \
        -f .ops/.kubernetes/Dockerfile.model-training \
        -t ${IMAGE_NAME}:${IMAGE_TAG} \
        .
"""
```

### Multi-Stage Builds

For faster builds, use Docker build cache:

```groovy
sh """
    docker build \
        --cache-from ${IMAGE_NAME}:latest \
        -f .ops/.kubernetes/Dockerfile.model-training \
        -t ${IMAGE_NAME}:${IMAGE_TAG} \
        .
"""
```

### Notification on Failure

Add email/Slack notifications in `post` section:

```groovy
post {
    failure {
        emailext(
            subject: "Build Failed: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
            body: "Check console output: ${env.BUILD_URL}",
            to: "your-email@example.com"
        )
    }
}
```

### Parallel Testing (Optional)

Add test stage before deployment:

```groovy
stage('Test') {
    parallel {
        stage('Unit Tests') {
            steps {
                sh 'pytest tests/unit/'
            }
        }
        stage('Integration Tests') {
            steps {
                sh 'pytest tests/integration/'
            }
        }
    }
}
```

## Monitoring

### View Pipeline Status

- Jenkins Dashboard → Your Pipeline → Latest Build
- Blue Ocean UI (install Blue Ocean plugin for better visualization)

### View Build Logs

- Click on build number → Console Output
- Or use: `docker logs jenkins` (if running in Docker)

### Kubernetes Job Logs

After deployment, view job logs:

```bash
kubectl logs -n trading-monitoring job/hmm-model-calibration -f
```

## Security Best Practices

1. **Credentials Management**:
   - Use Jenkins Credentials for sensitive data
   - Never hardcode passwords/secrets in Jenkinsfile

2. **Docker Security**:
   - Run Jenkins container with non-root user
   - Use Docker-in-Docker (DinD) if needed (with caution)

3. **Kubernetes RBAC**:
   - Create dedicated service account for Jenkins
   - Use minimal required permissions

4. **Image Scanning**:
   - Add image vulnerability scanning stage
   - Use tools like Trivy or Clair

## Example Workflow

```bash
# 1. Make changes to model code
vim src/trading_agent/model/training_script.py

# 2. Commit and push
git add .
git commit -m "Update model parameters"
git push origin main

# 3. Jenkins automatically:
#    - Detects the push (via webhook or polling)
#    - Builds Docker image
#    - Loads into kind cluster
#    - Updates Kubernetes job

# 4. Monitor in Jenkins UI
#    - Check build status
#    - View console output

# 5. Verify in Kubernetes
kubectl get jobs -n trading-monitoring
kubectl logs -n trading-monitoring job/hmm-model-calibration -f
```

## Next Steps

1. Install Jenkins (Docker or Homebrew)
2. Install required plugins
3. Create pipeline job using Jenkinsfile
4. Configure build triggers (webhook or polling)
5. Test with a commit
6. Monitor first build
7. Verify deployment in Kubernetes

## Additional Resources

- [Jenkins Pipeline Documentation](https://www.jenkins.io/doc/book/pipeline/)
- [Docker Pipeline Plugin](https://plugins.jenkins.io/docker-workflow/)
- [Kubernetes Plugin](https://plugins.jenkins.io/kubernetes/)

