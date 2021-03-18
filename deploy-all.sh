#!/bin/bash
set -e

# Builds and deploys all images for the Azure environments

ORG=${ORG:-hsl}

for TAG in latest dev stage production; do
  DOCKER_IMAGE="bulttiregistry.azurecr.io/${ORG}/hfp-loader:${TAG}"

  docker build -t "$DOCKER_IMAGE" .
  docker push "$DOCKER_IMAGE"
done
