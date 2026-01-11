# Building Custom Jenkins Image

This document describes how to build the custom Jenkins Docker image with kubectl, kind, and Docker CLI pre-installed.

## Why a Custom Image?

The Jenkins pipeline requires several tools:
- **Docker CLI**: For building Docker images
- **kubectl**: For Kubernetes operations (deploying jobs, managing resources)
- **kind**: For loading Docker images into the local kind cluster
- **Python 3**: For running Python scripts in the pipeline
- **Python dependencies**: All packages from `requirements.txt` for running the trading agent code

Instead of installing these tools every time the Jenkins container starts (which is very slow), we pre-build a custom Jenkins image with all tools and dependencies installed.

## Building the Image

### Prerequisites

- Docker Desktop (or Docker Engine) running
- Access to the internet (to download tools)

### Build Command

The build should be run from `.ops/.docker` directory (which contains `requirements.txt`):

```bash
cd .ops/.docker
docker build -f Dockerfile.jenkins -t jenkins-custom:lts .
```

**Note**: `requirements.txt` needs to be copied to `.ops/.docker/requirements.txt` for the Docker build context. If you update `requirements.txt` in the project root, copy it:

```bash
cp requirements.txt .ops/.docker/requirements.txt
```

This builds a custom Jenkins image named `jenkins-custom:lts` based on `jenkins/jenkins:lts` with:
- Docker CLI v27.4.1
- kubectl v1.28.0
- kind v0.30.0
- Python 3 (system Python)
- All Python dependencies from `requirements.txt` (pandas, numpy, torch, mlflow, psycopg2, etc.)

### Verification

After building, verify the image was created:

```bash
docker images | grep jenkins-custom
```

You should see:
```
jenkins-custom   lts    <image-id>   <time>   <size>
```

### Testing the Image

You can test that all tools are installed:

```bash
docker run --rm jenkins-custom:lts docker --version
docker run --rm jenkins-custom:lts kubectl version --client
docker run --rm jenkins-custom:lts kind version
docker run --rm jenkins-custom:lts python3 --version
docker run --rm jenkins-custom:lts pip3 list | grep pandas
```

## Using the Custom Image

The custom image is used in `docker-compose.yml`. Update the Jenkins service to use:

```yaml
jenkins:
  image: jenkins-custom:lts  # Instead of jenkins/jenkins:lts
```

Then restart Jenkins:

```bash
cd .ops/.docker
docker-compose stop jenkins
docker-compose up -d jenkins
```

## Updating the Image

If you need to update any of the tools or Python dependencies, modify `Dockerfile.jenkins` and/or `requirements.txt` and rebuild:

1. Edit `.ops/.docker/Dockerfile.jenkins` (for tools) or `requirements.txt` (for Python packages)
2. If you updated `requirements.txt` in the project root, copy it: `cp requirements.txt .ops/.docker/requirements.txt`
3. Update version numbers, download URLs, or package versions
4. Rebuild from `.ops/.docker`: `docker build -f Dockerfile.jenkins -t jenkins-custom:lts .`
5. Restart Jenkins: `docker-compose restart jenkins`

## Image Size

The custom image is larger than the base Jenkins image because it includes:
- Docker CLI binary (~40 MB)
- kubectl binary (~50 MB)
- kind binary (~6 MB)
- Python 3 and system packages (~50 MB)
- Python dependencies from requirements.txt (~500-1000 MB depending on packages)

Total additional size: ~600-1100 MB (compressed size will be smaller due to Docker layer caching)

**Note**: The Python dependencies (especially PyTorch, MLflow, etc.) are the largest contributors to image size. This is acceptable for a development/build environment where all dependencies need to be available.

## Kubeconfig Setup

Even with the custom image, the kubeconfig still needs to be configured at runtime because:
1. The kind cluster port may change
2. The kubeconfig path is user-specific
3. TLS certificate configuration needs to be set for `host.docker.internal`

The `docker-compose.yml` entrypoint handles kubeconfig updates on container startup, but the tools themselves are already installed in the image.

