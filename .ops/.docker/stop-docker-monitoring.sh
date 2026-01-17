#!/bin/bash
# Stop script for Docker-based monitoring services

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "🛑 Stopping Docker monitoring services..."
# Stop application services first (if any)
if [ -f "docker-compose.yml" ]; then
    docker-compose -f docker-compose.yml down
fi

# Stop infra-platform services
docker-compose -f docker-compose.infra-platform.yml down

echo "✅ Services stopped"


