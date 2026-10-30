# Container images and a local deployment. CONTAINER is docker if it is
# installed, else podman; override with `make images CONTAINER=podman`.
CONTAINER ?= $(shell command -v docker >/dev/null 2>&1 && echo docker || echo podman)
COMPOSE   ?= $(CONTAINER) compose
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
TAG       ?= dev

.PHONY: images server-image worker-image example-image up down

images: server-image worker-image example-image ## build all three images

server-image: ## hubwagen:$(TAG): the server, with the web UI
	$(CONTAINER) build -t hubwagen:$(TAG) --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
	  --build-arg BUILD_DATE=$$(date -u +%Y-%m-%dT%H:%M:%SZ) .

worker-image: ## hubwagen-worker:$(TAG): Python and the SDK, the base of project images
	$(CONTAINER) build -t hubwagen-worker:$(TAG) sdk/python

example-image: worker-image ## revenue-worker:$(TAG): the revenue example's workers
	$(CONTAINER) build -t revenue-worker:$(TAG) --build-arg WORKER_IMAGE=hubwagen-worker:$(TAG) pipelines/examples/revenue

up: ## run the server and the example's workers (deploy/compose.yaml)
	$(COMPOSE) -f deploy/compose.yaml up

down: ## stop them; add -v to the compose command to drop the data volume
	$(COMPOSE) -f deploy/compose.yaml down
