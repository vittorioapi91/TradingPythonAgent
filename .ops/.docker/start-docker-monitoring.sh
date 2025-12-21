#!/bin/bash
# Quick start script for Grafana, Prometheus, MLflow, Airflow, and PostgreSQL

set -e

echo "🚀 Starting ML Workflow Monitoring Services (Docker Compose)..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Navigate to docker directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Start services
echo "📦 Starting Docker Compose services..."
docker-compose -f docker-compose.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 10

# Check service status
echo ""
echo "📊 Service Status:"
docker-compose -f docker-compose.yml ps

echo ""
echo "✅ Services started successfully!"
echo ""
echo "🌐 Access your services:"
echo "   Grafana:    http://localhost:3000   (admin/2014)"
echo "   Prometheus: http://localhost:9090"
echo "   MLflow:     http://localhost:55000"
echo "   Airflow:    http://localhost:8080"
echo "   Postgres:   host=localhost port=55432 user=tradingAgent"
echo ""
echo "📝 To view logs: cd .ops/.docker && docker-compose logs -f"
echo "🛑 To stop:     cd .ops/.docker && ./stop-docker-monitoring.sh"
echo ""


