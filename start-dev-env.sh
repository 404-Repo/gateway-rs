#!/usr/bin/env bash

docker build -t gateway:latest .
cd dev-env
docker-compose up
