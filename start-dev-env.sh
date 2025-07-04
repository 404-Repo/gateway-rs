#!/usr/bin/env bash

docker build -t gateway:latest .
cd dev-env

if [ "$1" = "single" ] || [ "$1" = "1" ]; then
    echo "Starting in single node mode..."
    docker-compose -f docker-compose-single-node.yaml up
else
    echo "Starting in multi-node mode..."
    docker-compose up
fi
