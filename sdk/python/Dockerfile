# The base image for moksha workers: Python and the moksha SDK. A
# project's image adds its code (and its dependencies) and runs the worker:
#
#   FROM ghcr.io/alpaaaca/moksha-worker:latest
#   COPY . /app
#   RUN pip install --no-cache-dir -r requirements.txt   # if it has any
#
# Build from sdk/python:  docker build -t moksha-worker:dev sdk/python
# The worker reads MOKSHA_SERVER, MOKSHA_DOCKS and MOKSHA_LIFTS, and
# imports the modules listed in /app/pyproject.toml ([tool.moksha]).
FROM docker.io/library/python:3.12-slim
COPY . /tmp/sdk
RUN pip install --no-cache-dir /tmp/sdk && rm -rf /tmp/sdk \
 && useradd --uid 10001 --create-home worker
# Containers share no files with the server: data goes through its URLs.
ENV PYTHONUNBUFFERED=1 MOKSHA_DATA=remote
WORKDIR /app
USER worker
CMD ["python3", "-m", "moksha.worker"]
