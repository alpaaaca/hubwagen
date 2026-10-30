# Deploying hubwagen with containers

Three images, a compose file for one machine and a Helm chart for
Kubernetes. Settings are in [config.md](config.md); worker pools in
[worker-pools.md](worker-pools.md).

## Images

| Image | Built from | What |
|---|---|---|
| `hubwagen` | [Dockerfile](../Dockerfile) (repository root) | The server, with the web UI built in: a static binary on distroless, as a non-root user. `hubwagen server` with [deploy/hubwagen.yaml](../deploy/hubwagen.yaml): listens on `0.0.0.0:7070`, data in the `/data` volume, the bucket from `HUBWAGEN_BUCKET` (empty: `/data/bucket`). |
| `hubwagen-worker` | [sdk/python/Dockerfile](../sdk/python/Dockerfile) | Python 3.12 and the SDK: the base of a project's workers. Runs `python -m hubwagen.worker` in `/app` as a non-root user, data through the server (`HUBWAGEN_DATA=remote`). |
| your project's | e.g. [pipelines/examples/revenue/Dockerfile](../pipelines/examples/revenue/Dockerfile) | `FROM hubwagen-worker`, then your code in `/app` (its `pyproject.toml` lists the modules) and its dependencies. |

```bash
make images                  # hubwagen:dev, hubwagen-worker:dev, revenue-worker:dev
make images CONTAINER=podman # docker is the default when installed
```

A project's image:

```dockerfile
FROM ghcr.io/alpaaaca/hubwagen-worker:latest   # or hubwagen-worker:dev, built locally
COPY --chown=worker requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --chown=worker . /app
```

A new image is new code: the build it registers says which steps' code
changed, and those run again; the rest is reused.

## One machine: compose

[deploy/compose.yaml](../deploy/compose.yaml) runs the server and the
revenue example's workers (two by default):

```bash
make images
make up                      # docker compose (or podman compose) -f deploy/compose.yaml up
hubwagen start daily --param day=2026-10-01 --wait
open http://127.0.0.1:7070
make down
```

- The API is published on `127.0.0.1` only: it has no authentication yet.
- Data stays in the `data` volume (`docker compose -f deploy/compose.yaml
  down -v` removes it), or in a bucket: set `HUBWAGEN_BUCKET` (and
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) in the environment or an
  `.env` file next to the compose file.
- More workers: `REVENUE_WORKERS=4 make up` (Docker Compose; `podman-compose`
  ignores `deploy.replicas` and runs one). For your project, copy the
  `revenue` service with your image.

Compose runs a fixed number of workers. For workers that come and go
with the work, run the server outside a container with a `container`
pool ([config.md](config.md#container-pools-one-container-per-worker)),
or use Kubernetes.

## Kubernetes: the Helm chart

[deploy/helm/hubwagen](../deploy/helm/hubwagen) installs the server and a
Deployment per worker pool, whose replicas the server sets from the work
waiting (`launcher: kubernetes`, [config.md](config.md#kubernetes-pools-a-deployments-replicas)):

```yaml
# values.yaml
image:
  repository: ghcr.io/alpaaaca/hubwagen
server:
  bucket:
    url: s3://acme-hubwagen/prod
    region: eu-central-1
    existingSecret: hubwagen-s3     # keys access_key_id, secret_access_key; or a pod role
pools:
  - name: revenue
    image: ghcr.io/acme/revenue:2026-10-30
    min: 0
    max: 5
    lifts: 2
  - name: gpu
    image: ghcr.io/acme/train:2026-10-30
    max: 2
    docks: [gpu]
    lifts: 1
    resources: {limits: {nvidia.com/gpu: 1}}
```

```bash
helm install hubwagen deploy/helm/hubwagen -n hubwagen --create-namespace -f values.yaml
kubectl -n hubwagen port-forward svc/hubwagen 7070
hubwagen start daily --param day=2026-10-01 --wait
```

What it creates:

- **The server**: one replica (a single leader on SQLite) with `Recreate`,
  so two never share the data; `/data` on a PVC; `/healthz` as its
  probes; a ClusterIP Service.
- **A ConfigMap** with `hubwagen.yaml`: the bucket, and a `workers:` group
  per pool with `launcher: kubernetes`. A change restarts the server.
- **A Role** that lets the server's service account patch
  `deployments/scale` of the pools' Deployments, and nothing else.
- **A Deployment per pool** without `replicas`: the server sets them
  between `min` and `max`, and `helm upgrade` leaves them be. Its pods get
  `HUBWAGEN_SERVER`, the pool's docks and lifts, and no service account
  token. Give `terminationGracePeriodSeconds` (default 600) more time than
  your steps take: Kubernetes picks which pod to remove when it scales
  down, and a stopped worker finishes what it runs.

The API has no authentication yet: keep the Service a ClusterIP and reach
it with a port-forward, or behind something that authenticates.

## Not here yet

- Published images: there is no CI yet that pushes `ghcr.io/alpaaaca/hubwagen`
  and `hubwagen-worker`; build and push them yourself (`make images`, then
  tag and push), and point `image.repository` and the pools at yours.
- Failover: one server; a restart picks up where it left off
  ([failover.md](failover.md)).
