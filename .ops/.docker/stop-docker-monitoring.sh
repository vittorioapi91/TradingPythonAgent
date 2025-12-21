#!/bin/bash
# Stop script for Docker-based monitoring services

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "🛑 Stopping Docker monitoring services..."
docker-compose -f docker-compose.yml down

echo "✅ Services stopped"


