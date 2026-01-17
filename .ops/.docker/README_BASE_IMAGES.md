# Docker Base Images

## Overview

This project uses a two-tier Docker image strategy:
1. **Base Images**: Persistent images with dependencies pre-installed (never deleted)
2. **Incremental Images**: Fast builds that add code changes on top of base images

## Base Images

### 1. Jenkins Base Image (`jenkins-custom:base`)

**Dockerfile:** `.ops/.docker/Dockerfile.jenkins.base`

**Contains:**
- Jenkins LTS base
- All system tools (curl, python3, gcc, g++, postgresql-client)
- Docker CLI + buildx + compose
- kubectl and kind
- Python dependencies from `requirements.txt`

**Tag:** `jenkins-custom:base`

**Built when:**
- **Manually** (not in pipeline): Run `.ops/.docker/build-base-images.sh jenkins`
- Or manually: `docker build -t jenkins-custom:base -f .ops/.docker/Dockerfile.jenkins.base .ops/.docker/`
- Rebuild when `.ops/.docker/Dockerfile.jenkins.base` or `requirements.txt` changes

### 2. Trading Agent Base Image (`hmm-model-training-base:base`)

**Dockerfile:** `.ops/.kubernetes/Dockerfile.model-training.base`

**Contains:**
- Python 3.11-slim base
- System dependencies (gcc, g++, postgresql-client)
- Python dependencies from `requirements.txt`

**Tag:** `hmm-model-training-base:base`

**Built when:**
- **Manually** (not in pipeline): Run `.ops/.docker/build-base-images.sh trading-agent`
- Or manually: `docker build -t hmm-model-training-base:base -f .ops/.kubernetes/Dockerfile.model-training.base .`
- Rebuild when `.ops/.kubernetes/Dockerfile.model-training.base` or `requirements.txt` changes

## Incremental Images

### 1. Jenkins Incremental (`jenkins-custom:lts`)

**Dockerfile:** `.ops/.docker/Dockerfile.jenkins`

**FROM:** `jenkins-custom:base`

**Changes:** Currently identical to base (pass-through for future customizations)

**Built:** Every infra-platform pipeline run (fast since base is pre-built and cached)

### 2. Trading Agent Incremental (`hmm-model-training:XXX`)

**Dockerfile:** `.ops/.kubernetes/Dockerfile.model-training`

**FROM:** `hmm-model-training-base:base`

**Changes:** Only copies `src/` code (incremental layer)

**Built:** Every trading agent pipeline run (fast since base is pre-built and cached)

**Tags:** `hmm-model-training-dev:dev-XX-XXX`, `hmm-model-training:XX-XXX`, etc.

## Benefits

1. **Faster Builds**: Base images cached, incremental builds only add changed code
2. **Disk Space**: Base images shared, incremental builds are small
3. **Reliability**: Base images never deleted, ensuring builds always have dependencies
4. **Efficiency**: Dependencies installed once in base, reused in all builds

## Protection

Base images tagged with `:base` are **NEVER deleted** by:
- Jenkins workspace cleanup (only cleans workspace files, not images)
- Jenkins post-build cleanup (explicitly excludes `:base` tagged images)
- Manual cleanup scripts

## Building Base Images

**Base images must be built manually before running pipelines.** They are NOT built automatically in pipelines.

### Quick Build (Recommended)

```bash
# Build both base images
.ops/.docker/build-base-images.sh

# Or build individually
.ops/.docker/build-base-images.sh jenkins
.ops/.docker/build-base-images.sh trading-agent
```

### Manual Build

```bash
# Build Jenkins base
docker build -t jenkins-custom:base -f .ops/.docker/Dockerfile.jenkins.base .ops/.docker/

# Build trading agent base
docker build -t hmm-model-training-base:base -f .ops/.kubernetes/Dockerfile.model-training.base .
```

### Verification

```bash
# List base images
docker images | grep ':base'

# Check if base images exist (pipelines will fail if missing)
docker images --format '{{.Repository}}:{{.Tag}}' | grep ':base$'
```

### When to Rebuild Base Images

Rebuild base images when:
- `requirements.txt` changes (Python dependencies updated)
- Base Dockerfile changes (system packages, tools updated)
- Switching between different dependency versions

**Note:** After rebuilding base images, incremental builds will automatically use the new base on next pipeline run.
