# Deploying moksha with containers

Three images, a compose file for one machine and a Helm chart for
Kubernetes. Settings are in [config.md](config.md); worker pools in
[worker-pools.md](worker-pools.md).

## Binaries

Publishing a GitHub release `v1.2.3` also runs
[.github/workflows/release.yml](../.github/workflows/release.yml):
[GoReleaser](../.goreleaser.yaml) builds `moksha` with the web UI for
macOS and Linux on amd64 and arm64 and attaches
`moksha_1.2.3_<os>_<arch>.tar.gz` and `checksums.txt` to the release,
keeping its notes. To build them for an existing tag again: Actions →
release → Run workflow. To try it locally (needs Node), into `dist/`:

```bash
go run github.com/goreleaser/goreleaser/v2@v2.20.0 release --snapshot --clean
```

No Windows build: worker supervision uses Unix process groups and
signals.

## Images

| Image | Built from | What |
|---|---|---|
| `moksha` | [Dockerfile](../Dockerfile) (repository root) | The server, with the web UI built in: a static binary on distroless, as a non-root user. `moksha server` with [deploy/moksha.yaml](../deploy/moksha.yaml): listens on `0.0.0.0:7070`, data in the `/data` volume, the bucket from `MOKSHA_BUCKET` (empty: `/data/bucket`). |
| `moksha-worker` | [sdk/python/Dockerfile](../sdk/python/Dockerfile) | Python 3.12 and the SDK: the base of a project's workers. Runs `python -m moksha.worker` in `/app` as a non-root user, data through the server (`MOKSHA_DATA=remote`). |
| your project's | your project's `Dockerfile` | `FROM moksha-worker`, then your code in `/app` (its `pyproject.toml` lists the modules) and its dependencies. |

```bash
make images                  # moksha:dev, moksha-worker:dev
make images CONTAINER=podman # docker is the default when installed
```

### Published images

[.github/workflows/images.yml](../.github/workflows/images.yml) builds the
server and worker base images for `linux/amd64` and `linux/arm64` and
pushes them to `ghcr.io/alpaaaca/moksha` and
`ghcr.io/alpaaaca/moksha-worker`, only when asked, never on every
commit:

| When | Tags |
|---|---|
| a pull request gets the label `build` (and each new commit while it has it) | `pr-<number>`, `sha-<commit>` |
| a GitHub release `v1.2.3` is published | `1.2.3`, `1.2`, `latest` |
| by hand: Actions → images → Run workflow, on a branch | `<branch>`, `sha-<commit>` |

To try a pull request's images: label it `build`, then
`helm upgrade … --set image.tag=pr-123`. Remove the label to stop
rebuilding. Pull requests from forks are never built (they can't push).

The Helm chart's default `image.tag` is its `appVersion` (the published
chart's is the release's; in a checkout, `0.2.1`), which exists once that
release is published; until then use a `pr-…`
image or run the workflow by hand. The server image cross-compiles (`--platform=$BUILDPLATFORM`),
so arm64 needs no emulation; the worker image's arm64 build runs `pip`
under QEMU. GHCR makes a new package private: make both public in their
package settings once, or give the cluster a pull secret. The same goes
for the chart (`charts/moksha`), pushed by the release workflow.

A project's image:

```dockerfile
FROM ghcr.io/alpaaaca/moksha-worker:latest   # or moksha-worker:dev, built locally
COPY --chown=worker requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY --chown=worker . /app
```

A new image is new code: the build it registers says which steps' code
changed, and those run again; the rest is reused.

## One machine: compose

[deploy/compose.yaml](../deploy/compose.yaml) runs the server, with a
commented-out `workers` service for your project's image (two by default):

```bash
make images
make up                      # docker compose (or podman compose) -f deploy/compose.yaml up
moksha start my_pipeline --wait  # once your workers are up
open http://127.0.0.1:7070
make down
```

- The API is published on `127.0.0.1` only: it has no authentication yet.
- Data stays in the `data` volume (`docker compose -f deploy/compose.yaml
  down -v` removes it), or in a bucket: set `MOKSHA_BUCKET` (and
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) in the environment or an
  `.env` file next to the compose file.
- Your workers: uncomment the `workers` service and set `WORKER_IMAGE`;
  more of them: `WORKERS=4 make up` (Docker Compose; `podman-compose`
  ignores `deploy.replicas` and runs one).

Compose runs a fixed number of workers. For workers that come and go
with the work, run the server outside a container and `moksha deploy`
your project's image to it
([config.md](config.md#deployed-images-one-container-per-worker)), or use
Kubernetes.

## Kubernetes: the Helm chart

The chart, [deploy/helm/moksha](../deploy/helm/moksha), is published with
each release as `oci://ghcr.io/alpaaaca/charts/moksha` (version `1.2.3`
for release `v1.2.3`, its images the release's). It installs the server, and the
server runs your projects: `moksha deploy --push` hands it a project's
image by digest, and it runs each pool as a Deployment it creates,
scales from the work waiting and deletes when a new version replaces it
(`projects.launcher: kubernetes`,
[config.md](config.md#projects-on-kubernetes-a-deployment-per-pool)).
Nothing per project goes in the values:

```yaml
# values.yaml
image:
  repository: ghcr.io/alpaaaca/moksha
server:
  bucket:
    url: s3://acme-moksha/prod
    region: eu-central-1
    existingSecret: moksha-s3     # keys access_key_id, secret_access_key; or a pod role
projects:
  imagePullSecrets: [ghcr]        # if projects' images are private
```

```bash
helm install moksha oci://ghcr.io/alpaaaca/charts/moksha --version 0.2.1 \
  -n moksha --create-namespace -f values.yaml
# or from a checkout: helm install moksha deploy/helm/moksha …
kubectl -n moksha port-forward svc/moksha 7070
```

Then, in a project's directory (a `moksha.yaml` with `kind: Project` and
a `build:` whose `image` is in a registry the cluster pulls from):

```bash
moksha deploy --push             # build, push, deploy by digest
kubectl -n moksha get deployments -l app.kubernetes.io/managed-by=moksha
moksha start daily --wait
```

`moksha deploy` again with changed code is a new version: its
Deployments start at each pool's `min`, and the old version's are
deleted, their pods finishing what they run. A project's
`{secret: revenue-db}` is key `value` of the Secret `revenue-db` in the
release's namespace.

What it creates:

- **The server**: one replica (a single leader on SQLite) with `Recreate`,
  so two never share the data; `/data` on a PVC; `/healthz` as its
  probes; a ClusterIP Service. Without a bucket, data versions are on the
  PVC and workers move them through the server.
- **A ConfigMap** with `moksha.yaml`: the bucket, `projects.launcher:
  kubernetes`, the Service as pods reach it
  (`http://<release>.<namespace>.svc:7070`) and the instance that labels
  the server's Deployments (`projects.instance`, default the release's
  full name, so two releases in a namespace stay apart). A change
  restarts the server.
- **A Role** for the server's service account: get, list, create and
  delete Deployments in the namespace, and set their scale. Nothing else.
- **No worker Deployments**: those are the server's, not Helm's, so
  `helm uninstall` leaves them. `moksha delete <project>` first, or
  `kubectl delete deployments -l app.kubernetes.io/managed-by=moksha,moksha.dev/instance=<instance>`
  after.

The API has no authentication yet: keep the Service a ClusterIP and reach
it with a port-forward, or behind something that authenticates.

## Not here yet

- Failover: one server; a restart picks up where it left off
  ([failover.md](failover.md)).
