# The revenue example's workers: the worker base image plus this project.
#   docker build -t revenue-worker:dev pipelines/examples/revenue
# (with a local base image: --build-arg WORKER_IMAGE=hubwagen-worker:dev)
ARG WORKER_IMAGE=ghcr.io/alpaaaca/hubwagen-worker:latest
FROM ${WORKER_IMAGE}
COPY --chown=worker . /app
