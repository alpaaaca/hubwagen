# The base image for hubwagen workers: Python and the hubwagen SDK. A
# project's image adds its code (and its dependencies) and runs the worker:
#
#   FROM ghcr.io/alpaaaca/hubwagen-worker:latest
#   COPY . /app
#   RUN pip install --no-cache-dir -r requirements.txt   # if it has any
#
# Build from sdk/python:  docker build -t hubwagen-worker:dev sdk/python
# The worker reads HUBWAGEN_SERVER, HUBWAGEN_DOCKS and HUBWAGEN_LIFTS, and
# imports the modules listed in /app/pyproject.toml ([tool.hubwagen]).
FROM docker.io/library/python:3.12-slim
COPY . /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk \
 && useradd --uid 10001 --create-home worker
# Containers share no files with the server: data goes through its URLs.
ENV PYTHONUNBUFFERED=1 HUBWAGEN_DATA=remote
WORKDIR /app
USER worker
CMD ["python3", "-m", "hubwagen.worker"]
