#!/usr/bin/env bash

set -e

export DOCKER_BUILDKIT=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKERFILE_PATH="$SCRIPT_DIR/Dockerfile"

cd "$SCRIPT_DIR/dev-env"

if [ "$1" = "stop" ] || [ "$1" = "down" ]; then
    echo "Stopping all containers..."
    docker-compose -f docker-compose-single-node.yaml down
    docker-compose down
    echo "All containers stopped."
elif [ "$1" = "single" ] || [ "$1" = "1" ]; then
    docker build -t gateway:latest -f "$DOCKERFILE_PATH" "$SCRIPT_DIR"
    echo "Starting in single node mode..."
    docker-compose -f docker-compose-single-node.yaml up
else
    echo "Starting in multi-node mode..."
    docker build -t gateway:latest -f "$DOCKERFILE_PATH" "$SCRIPT_DIR"
    docker-compose up
fi
