#!/bin/bash

# Startup script to launch all trading agent services
# This script is designed to be run at Mac startup via LaunchAgent

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Log file
LOG_FILE="$HOME/.trading-agent-services.log"
PROJECT_ROOT="/Users/Snake91/CursorProjects/TradingPythonAgent"
DOCKER_COMPOSE_DIR="$PROJECT_ROOT/.ops/.docker"
KUBECTL_CONTEXT="kind-trading-cluster"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "=== Starting Trading Agent Services ==="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    log "${RED}ERROR: Docker is not running. Please start Docker Desktop.${NC}"
    exit 1
fi

# Start Docker Compose services
log "${YELLOW}Starting Docker Compose services...${NC}"
cd "$DOCKER_COMPOSE_DIR"
if docker-compose up -d >> "$LOG_FILE" 2>&1; then
    log "${GREEN}✓ Docker Compose services started${NC}"
else
    log "${RED}ERROR: Failed to start Docker Compose services${NC}"
    exit 1
fi

# Wait a bit for services to initialize
sleep 5

# Check if Kubernetes cluster exists and is running
log "${YELLOW}Checking Kubernetes cluster...${NC}"
if kind get clusters | grep -q "trading-cluster"; then
    log "${GREEN}✓ Kubernetes cluster 'trading-cluster' exists${NC}"
    
    # Check if cluster is accessible
    if kubectl cluster-info --context "$KUBECTL_CONTEXT" > /dev/null 2>&1; then
        log "${GREEN}✓ Kubernetes cluster is accessible${NC}"
    else
        log "${YELLOW}Warning: Kubernetes cluster exists but may not be fully ready${NC}"
    fi
else
    log "${YELLOW}Warning: Kubernetes cluster 'trading-cluster' not found${NC}"
    log "${YELLOW}You may need to create it manually: kind create cluster --name trading-cluster${NC}"
fi

# Wait for Jenkins pod to be ready (if deployed)
log "${YELLOW}Checking Jenkins deployment...${NC}"
if kubectl get pods -n jenkins --context "$KUBECTL_CONTEXT" > /dev/null 2>&1; then
    log "${GREEN}✓ Jenkins namespace exists${NC}"
    
    # Wait for Jenkins pod to be ready (with timeout)
    if kubectl wait --for=condition=ready pod -l app=jenkins -n jenkins --context "$KUBECTL_CONTEXT" --timeout=60s > /dev/null 2>&1; then
        log "${GREEN}✓ Jenkins pod is ready${NC}"
    else
        log "${YELLOW}Warning: Jenkins pod may not be ready yet${NC}"
    fi
else
    log "${YELLOW}Jenkins not deployed yet. Deploy with: kubectl apply -f $PROJECT_ROOT/.ops/.jenkins/jenkins-deployment.yaml${NC}"
fi

# Set up port forwarding for Jenkins (background)
log "${YELLOW}Setting up port forwarding for Jenkins...${NC}"
if pgrep -f "kubectl port-forward.*jenkins.*8081" > /dev/null; then
    log "${YELLOW}Jenkins port forwarding already running${NC}"
else
    kubectl port-forward -n jenkins service/jenkins 8081:8080 --context "$KUBECTL_CONTEXT" >> "$LOG_FILE" 2>&1 &
    log "${GREEN}✓ Jenkins port forwarding started on port 8081${NC}"
fi

# Set up port forwarding for Kubernetes Dashboard (background)
log "${YELLOW}Setting up port forwarding for Kubernetes Dashboard...${NC}"
if pgrep -f "kubectl port-forward.*kubernetes-dashboard.*8001" > /dev/null; then
    log "${YELLOW}Kubernetes Dashboard port forwarding already running${NC}"
else
    kubectl port-forward -n kubernetes-dashboard service/kubernetes-dashboard 8001:443 --context "$KUBECTL_CONTEXT" >> "$LOG_FILE" 2>&1 &
    log "${GREEN}✓ Kubernetes Dashboard port forwarding started on port 8001${NC}"
fi

# Wait a bit for dashboard to be ready, then generate and copy token
log "${YELLOW}Generating Kubernetes Dashboard token...${NC}"
sleep 3
if kubectl get serviceaccount dashboard-admin -n kubernetes-dashboard --context "$KUBECTL_CONTEXT" > /dev/null 2>&1; then
    DASHBOARD_TOKEN=$(kubectl create token dashboard-admin -n kubernetes-dashboard --context "$KUBECTL_CONTEXT" --duration=8760h 2>/dev/null)
    if [ -n "$DASHBOARD_TOKEN" ]; then
        if echo "$DASHBOARD_TOKEN" | pbcopy 2>/dev/null; then
            log "${GREEN}✓ Kubernetes Dashboard token generated and copied to clipboard${NC}"
            log "${GREEN}  Token is ready to paste (Cmd+V) when accessing https://localhost:8001${NC}"
        else
            log "${YELLOW}Warning: Token generated but could not copy to clipboard (pbcopy failed)${NC}"
            log "${YELLOW}  You can get the token manually with:${NC}"
            log "${YELLOW}  kubectl create token dashboard-admin -n kubernetes-dashboard --context $KUBECTL_CONTEXT${NC}"
        fi
    else
        log "${YELLOW}Warning: Could not generate dashboard token${NC}"
    fi
else
    log "${YELLOW}Warning: dashboard-admin service account not found. Token generation skipped.${NC}"
fi

# Summary
log "${GREEN}=== Service Startup Complete ===${NC}"
log "${GREEN}Services available at:${NC}"
log "  - Airflow: http://localhost:8080"
log "  - Jenkins: http://localhost:8081"
log "  - Grafana: http://localhost:3000"
log "  - MLflow: http://localhost:55000"
log "  - Prometheus: http://localhost:9090"
log "  - Kubernetes Dashboard: https://localhost:8001"
log "  - PostgreSQL: localhost:55432"
log "  - Redis: localhost:6379"
log ""
log "Log file: $LOG_FILE"

