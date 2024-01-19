#!/usr/bin/env bash

docker build -f ./microservices/ms-pipeline-manager/Dockerfile -t spex.pipeline.manager:latest .
docker tag spex.pipeline.manager:latest ghcr.io/genentech/spex.pipeline.manager:latest
docker push ghcr.io/genentech/spex.pipeline.manager:latest