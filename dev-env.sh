#!/usr/bin/env bash
cd dev-env

if [ "$1" = "stop" ]; then
    echo "Stopping all containers..."
    docker-compose -f docker-compose-single-node.yaml down
    docker-compose down
    echo "All containers stopped."
elif [ "$1" = "single" ] || [ "$1" = "1" ]; then
    docker build -t gateway:latest .
    echo "Starting in single node mode..."
    docker-compose -f docker-compose-single-node.yaml up
else
    echo "Starting in multi-node mode..."
    docker build -t gateway:latest .
    docker-compose up
fi
