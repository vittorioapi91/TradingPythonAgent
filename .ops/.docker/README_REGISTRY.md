# Local Docker Registry Setup

This directory contains configuration and scripts for managing a local Docker registry that stores base images for reuse across builds.

## Overview

The local registry (`localhost:5000`) stores base Docker images that are reused across pipeline runs. This eliminates the need to rebuild base images on every pipeline execution, significantly speeding up builds.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Local Docker Registry (localhost:5000)                 │
│  - Stores base images (hmm-model-training-base:base)    │
│  - Stores incremental images (hmm-model-training:*)      │
└─────────────────────────────────────────────────────────┘
                    ↑                    ↓
                    │                    │
        ┌───────────┴──────────┐        │
        │                       │        │
   Jenkins Pipeline      push-base-images.sh
   (pushes after build)  (initial setup)
        │                       │
        └───────────┬───────────┘
                    ↓
        ┌───────────────────────┐
        │  Kind Cluster         │
        │  (pulls from registry)│
        └───────────────────────┘
```

## Setup

### 1. Start the Registry

```bash
cd .ops/.docker
docker-compose -f docker-compose.registry.yml up -d
```

The registry will be available at `http://localhost:5000`.

### 2. Build and Push Base Images

```bash
# Build and push base images to registry
.ops/.docker/push-base-images.sh

# Or force rebuild:
.ops/.docker/push-base-images.sh --rebuild
```

This script:
- Builds the base image (`hmm-model-training-base:base`) if it doesn't exist locally
- Tags it for the registry
- Pushes it to `localhost:5000/hmm-model-training-base:base`

### 3. Configure Kind Cluster

```bash
# Configure kind cluster to use local registry
.ops/.docker/configure-kind-registry.sh [cluster-name]

# Default cluster name: trading-cluster
```

This script:
- Connects the registry container to the kind network
- Configures containerd in the kind cluster to use the local registry
- Allows pulling images from `localhost:5000` without TLS verification

## How It Works

### Pipeline Flow

1. **Check Registry**: Pipeline checks if the image exists in the registry
2. **Skip Build if Exists**: If image exists and is up-to-date, skip build
3. **Build if Needed**: Only build if image doesn't exist or needs update
4. **Push to Registry**: After building, push to registry for future use
5. **Kind Pulls from Registry**: Kind cluster pulls images from registry (faster than `kind load`)

### Base Images

Base images (`hmm-model-training-base:base`) contain:
- System dependencies (gcc, g++, postgresql-client)
- Python dependencies (from `requirements.txt`)

These change infrequently, so they're built once and reused.

### Incremental Images

Incremental images (`hmm-model-training:*`) contain:
- Base image (FROM base)
- Source code (`src/` directory)

These are rebuilt when source code changes, but the base layer is reused from the registry.

## Benefits

1. **Faster Builds**: Base images are reused, only source code layer is rebuilt
2. **Reduced Network**: Kind pulls from local registry instead of loading large images
3. **Consistent Images**: All environments use the same base images
4. **Offline Development**: Works without internet (after initial setup)

## Maintenance

### View Images in Registry

```bash
# List repositories
curl http://localhost:5000/v2/_catalog

# List tags for a repository
curl http://localhost:5000/v2/hmm-model-training-base/tags/list
```

### Clean Up Old Images

The registry supports deletion. You can delete old tags manually or use the registry API.

### Update Base Images

When `requirements.txt` or `Dockerfile.model-training.base` changes:

```bash
# Rebuild and push base image
.ops/.docker/push-base-images.sh --rebuild
```

## Troubleshooting

### Registry Not Accessible

```bash
# Check if registry is running
docker ps | grep docker-registry

# Check registry logs
docker logs docker-registry

# Restart registry
docker-compose -f .ops/.docker/docker-compose.registry.yml restart
```

### Kind Can't Pull from Registry

```bash
# Reconfigure kind cluster
.ops/.docker/configure-kind-registry.sh

# Verify registry is accessible from kind
kubectl run --rm -i --restart=Never --image=curlimages/curl:latest test-registry --context kind-trading-cluster -- curl http://localhost:5000/v2/
```

### Images Not Found in Registry

```bash
# Rebuild and push base images
.ops/.docker/push-base-images.sh --rebuild
```

## Files

- `docker-compose.registry.yml`: Registry service definition
- `push-base-images.sh`: Script to build and push base images
- `configure-kind-registry.sh`: Script to configure kind cluster
- `check-rebuild-needed.sh`: Helper script to check if rebuild is needed
