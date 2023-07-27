#!/bin/bash
set -e

# Build the Docker image
podman build -t concurrent-pg-tester .

# Create a new container without starting it
container=$(podman create concurrent-pg-tester)

# Copy the executable from the container to the current folder
podman cp $container:/app/concurrent-pg-tester ./concurrent-pg-tester-stretch

# Remove the container
podman rm $container

echo "Done"
