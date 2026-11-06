# Instance configuration

How a moksha instance is configured: one YAML file, `moksha.yaml`, with
flags on top. Code: `internal/config`.

## What belongs here

The config file describes the **instance**: where the server keeps its data,
what it listens on and, in dev, which worker processes to start next to it.
It never describes what a pipeline does; that is code, versioned by builds.

## Today

```yaml
apiVersion: moksha.dev/v1alpha1
kind: Config

data_dir: .moksha              # state.db, bucket/, lock; default .moksha
# bucket:                        # where data versions go; default <data_dir>/bucket
#   url: s3://acme-moksha/prod
#   access_key_id_file: /run/secrets/s3_key_id
#   secret_access_key_file: /run/secrets/s3_secret
server:
  listen: 127.0.0.1:7070         # the API; default 127.0.0.1:7070

# pools:                         # pools something else starts (below)
#   - {name: gpu, launcher: external, min: 0, max: 4, queues: [gpu]}
```

The config describes the instance. It has no workers of your code: those
come from your project's file (`kind: Project`, [projects.md](projects.md)),
which `moksha dev` deploys from its directory and `moksha deploy` hands
to a server:

```yaml
# moksha.yaml in your project, next to pyproject.toml
apiVersion: moksha.dev/v1alpha1
kind: Project
name: revenue
pools:
  - name: default                # workers revenue-default-v<version>-<n>
    min: 1                       # always running
    max: 3                       # more while work waits
    steps: 2                     # steps one worker runs at once; default 4
  - name: gpu
    queues: [gpu]                # default [default]
    min: 0                       # none while nothing waits
    max: 2
    idle_after: 10m              # stop one with nothing to run this long; default 5m
```

- **Relative paths** (`data_dir`, `workers[].dir`, the bucket's credential
  files) are relative to the config file, so a project's `moksha.yaml` works from any directory.
- **Where the file comes from**: `--config <path>`, else `$MOKSHA_CONFIG`,
  else `./moksha.yaml` if it exists, else the defaults. A file named by
  `--config` or `$MOKSHA_CONFIG` must exist.
- **A project file instead**: when that file is a project's (`kind:
  Project`), `moksha dev` uses the defaults, its data in `.moksha` next
  to the file, and deploys the project to itself in place; with a server
  config, it deploys a `moksha.project.yaml` in the current directory, if
  there is one ([projects.md](projects.md)).
- **Flags win**: `moksha dev --data-dir … --listen …` override the file,
  but only when passed.
- **Strict**: unknown fields, wrong types and duplicate keys are errors.
  Every problem is reported at once, with its location:
  `moksha.yaml: pools[1]: min 3 is more than max 2`.
- **Variables**: values may come from the environment, for what differs per
  machine (see below).

### Variables

`${NAME}` is replaced with the environment variable `NAME`, and
`${NAME:-default}` with its default when `NAME` is unset or empty, before
the file is parsed. So one committed file serves every machine, with a
`.env` or the service manager setting the values:

```yaml
server:
  listen: ${MOKSHA_LISTEN:-127.0.0.1:7070}
bucket:
  url: ${MOKSHA_BUCKET:-}          # unset: no bucket section, <data_dir>/bucket
  endpoint: ${MOKSHA_S3_ENDPOINT:-}
kubernetes:
  namespace: ${MOKSHA_NAMESPACE}   # must be set
pools:
  - name: revenue
    launcher: kubernetes
    deployment: revenue-workers
    max: ${WORKERS:-3}             # still a number
```

- **`${NAME}` must be set**, even if empty; an unset one is an error with
  its line (`moksha.yaml: line 7: ${MOKSHA_NAMESPACE} is not set`), all of
  them at once.
- **A bucket section of empty values is no bucket**, so
  `${MOKSHA_BUCKET:-}` makes the bucket optional.
- **Only `${…}`**: a bare `$NAME` is left alone, so a worker command such
  as `[sh, -c, "echo $HOME"]` means what it says; `$${` stands for a literal
  `${`. Lines that are only a comment aren't expanded.
- **Textual**: the value is put in as is, so line numbers stay right. Quote
  the field (`"${NAME}"`) if a value may hold YAML characters such as `: `
  or ` #`. A value can't hold a line break.
- **Not for secrets**: credentials still come from `*_file` paths or the
  usual `AWS_*` variables, never values in the config, expanded or not.

### Workers

A worker is any process that speaks the worker API; moksha doesn't care
what language it is in. The Python worker is `python -m moksha.worker`:
it imports the modules its project lists under `[tool.moksha]` in
`pyproject.toml`, registers the pipelines they define, and the server
sends it only those. A project's pools say how many to run; the server
starts them for each deployed project (below), as processes in its
directory or containers of its image. Every worker it starts gets these
variables, which a project's `env` can't set:

| Variable | Value |
|---|---|
| `MOKSHA_SERVER` | the API's URL, e.g. `http://127.0.0.1:7070` (an unspecified listen host such as `0.0.0.0` becomes `127.0.0.1`) |
| `MOKSHA_WORKER_ID` | `<project>-<pool>-v<version>-<n>`; the event log records it on every dispatch |
| `MOKSHA_SUPERVISOR_PID` | the moksha process that started it; a worker stops once its parent is no longer this process |
| `MOKSHA_QUEUES` | the pool's `queues`, comma-separated: the Python worker's default for `--queue` |
| `MOKSHA_STEPS` | the pool's `steps`: the Python worker's default for `--steps` |

The Python worker reads all of them. `MOKSHA_SUPERVISOR_PID` covers a
supervisor that dies without stopping its workers (`kill -9`, a crash).
The workers run in their own process groups, so nothing else would stop
them, and the next `moksha dev` would start a second set. The Python
worker checks every second and then stops as on Ctrl-C: running steps
finish and report.

`moksha dev` and `moksha server` start the workers after the API is up.
Dev prefixes their output with their ID (`revenue-default-v1-2 | …`); the
server puts it in its log ([below](#running-the-server-as-a-service)).
One that exits is restarted, with backoff from 1s doubling to 30s. On
Ctrl-C (or SIGTERM) they send the workers SIGTERM and wait: running steps
finish, and a poll in flight ends within its wait (20s by default). Then
the server stops. A second Ctrl-C kills the workers at once. Each worker
runs in its own process group, so the terminal's Ctrl-C reaches only
moksha.

- `--no-workers` starts the server alone: it starts no workers and
  scales no pools, and dev deploys nothing.
- A worker someone else starts (another machine, a service manager)
  needs only `MOKSHA_SERVER`: `MOKSHA_SERVER=http://10.0.0.5:7070 python
  -m moksha.worker` in the project's directory.

#### Pools: scaled by the work that waits

A pool runs between `min` and `max` workers: `moksha server` and `moksha
dev` scale it from the work queued on its `queues`. Every two seconds:

- **Up**: when runs or steps on its queues wait because no worker polls
  them or all are busy, it starts workers, one per `steps` waiting steps
  (one for a waiting run), up to `max`. It starts no more until those
  have polled (or a minute passed), so a burst of work doesn't start
  `max` at once. Meanwhile a waiting run says "a worker is starting in
  pool revenue-gpu-v1".
- **Down**: a worker above `min` that waited for work, with nothing to
  run, for `idle_after` is stopped (SIGTERM: like any stop, it finishes
  what it runs), highest number first.
- **Not**: for work no worker's code defines, or held by a queue's limit:
  more workers wouldn't run it.
- **Pools sharing a queue**: the first with room takes its work.

`min: 0` runs nothing while nothing waits: deploying registered the
project's code, so its pipelines take runs with no worker running.
Scaling is the leader's memory, not state: a restarted server starts
each pool at its `min` and scales from what waits. The pools and their
last decision are in `ListPools`, the web UI (on a waiting run) and the
server's log (`worker pool revenue-gpu-v1: started revenue-gpu-v1-1: 3
steps wait on gpu`). Design: [worker-pools.md](worker-pools.md).

#### Deployed projects

Projects deployed with `moksha deploy` ([projects.md](projects.md)) bring
their own pools, named `<project>-<pool>-v<version>`; the server runs them
next to the `pools:` of its config, as containers of the project's
image or processes in its directory. How it runs the containers:

```yaml
projects:
  runtime: podman          # default: docker if on PATH, else podman
  server_url: http://host.containers.internal:7070   # default: the listen address, its loopback host as the containers reach it
  auth_file: /etc/moksha/registry/config.json      # pull credentials, see below
  secrets_dir: /etc/moksha/secrets                 # see below
```

- **Images by digest**: a deploy to a server elsewhere (or `moksha deploy
  --push`) pushes the project's image with the deploying machine's login
  and deploys it as `<image>@sha256:…`; the server pulls exactly that.
  For a private registry it needs pull credentials: `auth_file`, a
  `docker`/`podman login` credentials file (`config.json`, or podman's
  `auth.json`): Podman gets `--authfile`, Docker `DOCKER_CONFIG` (the
  file must be named `config.json`). Without one, the runtime's own login
  on the server's machine applies. `auth_file` works on `launcher:
  container` groups too.
- **Secrets**: a project's `env: {DB_URL: {secret: revenue-db}}` sends only
  the name. The server reads `<secrets_dir>/revenue-db` (one value per
  file, trailing newlines dropped) when it starts a worker and puts it in
  the worker's environment. Values never reach events, state, the API or
  a command line. A deploy naming a secret the server doesn't have is
  refused; a secret removed later stops new workers from starting
  (logged; tried again on the next pass). Keep the directory readable by
  the server only.

At start the server removes containers an earlier moksha on the same
data directory left running, for its pools and its projects alike.

#### External pools: KEDA, an HPA or anything that polls

A pool in the server's config (`pools:`) is one moksha starts nothing for.
With `launcher: external`, its workers run elsewhere (a Kubernetes
Deployment, another machine), and the server only works out how many it
wants. That count is
`GET /v1/pools/<name>` on the API, as JSON:

```yaml
pools:
  - name: revenue
    launcher: external       # no command or env: they live with the workers
    min: 0
    max: 10
    queues: [default]
    steps: 2
    idle_after: 5m
```

```json
{"name": "revenue", "launcher": "external", "queues": ["default"], "min": 0, "max": 10,
 "desired": 3, "workers": 1, "decision": "wants 3 workers (was 1): 5 steps wait on default",
 "decided_at": "2026-10-30T17:20:04Z"}
```

`desired` is the workers polling its queues that ran something within
`idle_after`, plus one per `steps` steps (or one for a run) waiting there
for a worker, between `min` and `max`. Workers that the server started
for a project don't count, and each worker counts for the first external pool whose
queues it polls. `GET /v1/pools` lists every pool.

KEDA reads it with its `metrics-api` trigger. With a target of 1 per pod,
the Deployment runs `desired` replicas, from zero:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: moksha-revenue
spec:
  scaleTargetRef:
    name: revenue-workers          # the Deployment running python -m moksha.worker
  minReplicaCount: 0
  maxReplicaCount: 10
  pollingInterval: 5
  cooldownPeriod: 60
  triggers:
    - type: metrics-api
      metadata:
        url: "http://moksha.moksha.svc:7070/v1/pools/revenue"
        valueLocation: "desired"
        targetValue: "1"
        activationTargetValue: "0"
```

The workers' Deployment sets `MOKSHA_SERVER`, and `--queue`/`--steps` (or
`MOKSHA_QUEUES`/`MOKSHA_STEPS`) to match the pool. Kubernetes picks
which pod to remove when it scales down, maybe a busy one: give the pods a
`terminationGracePeriodSeconds` longer than your steps, so a stopped
worker finishes them (it stops polling on SIGTERM).

#### Deployed images: one container per worker

A project deployed with an image (`build.image`, [projects.md](projects.md))
runs each worker as a container of it, with Docker or Podman
(`projects.runtime`), scaled like processes:

- **Each worker is `<runtime> run --rm --name moksha-<id>`** in the
  foreground, so its output is in moksha's, SIGTERM reaches the worker,
  and a crashed one is restarted. `<runtime> rm -f moksha-<id>` runs before
  each start and after each exit.
- **Leftovers**: containers are labelled with their pool and the
  server's data directory (`moksha.pool`, `moksha.data_dir`). A
  moksha killed with SIGKILL leaves its containers running and still
  polling; when it starts again it removes them (those of its own data
  directory only) before starting the pool.
- **The server's URL** inside the container is `server_url`, by default
  the listen address with a loopback host replaced by
  `host.docker.internal` (Docker) or `host.containers.internal`
  (Podman). Docker Desktop and Podman (macOS) reach the host's loopback that way;
  on Linux with Docker, the server must listen where the bridge reaches it
  (`0.0.0.0`, and keep it off public networks while there is no
  authentication).
- **Data goes through the server** (`MOKSHA_DATA=remote`): containers
  share no files with it and need no bucket credentials.
- **Variables** (`env`, `MOKSHA_SERVER`, `MOKSHA_WORKER_ID`,
  `MOKSHA_QUEUES`, `MOKSHA_STEPS`) go in with `-e NAME`, so their values
  aren't on the command line.

#### Projects on Kubernetes: a Deployment per pool

With `projects.launcher: kubernetes`, a server in a cluster runs each
deployed project's pools as Deployments it creates, scales and deletes
itself. `moksha deploy` pushes the project's image and deploys it by
digest; nothing else to install per project:

```yaml
kubernetes:
  namespace: moksha                # default: the service account's
  instance: moksha                 # labels this server's Deployments; default moksha
  image_pull_secrets: [ghcr]       # Secrets for pulling projects' images, if private
projects:
  launcher: kubernetes
  server_url: http://moksha.moksha.svc:7070   # the server's Service, as pods reach it
```

- **A Deployment per pool and version**, `moksha-<project>-<pool>-v<version>`,
  labelled `app.kubernetes.io/managed-by: moksha` and `moksha.dev/instance`.
  It is created at the pool's `min` and its replicas are set from the work
  waiting, as for any pool. A new version is a new Deployment; the old
  one's is deleted, so its pods get SIGTERM and finish what they run
  within `terminationGracePeriodSeconds` (600). Deleting the project
  deletes its Deployments. At start the server deletes Deployments of
  its instance that no project needs any more.
- **Pods**: the image, its own command, `MOKSHA_SERVER` (`server_url`),
  `MOKSHA_WORKER_ID` (the pod's name), the pool's queues and steps,
  `MOKSHA_DATA=remote` (data goes through the server), the project's `env`,
  and no service account token.
- **Secrets**: a project's `env: {DB_URL: {secret: revenue-db}}` reads
  key `value` of the Kubernetes Secret `revenue-db` in the namespace
  (`kubectl create secret generic revenue-db --from-literal=value=…`). The
  server doesn't read Secrets, so a deploy isn't checked for them: a
  missing one shows on the pods (`CreateContainerConfigError`).
- **Images by digest only**: a deploy of code in place is refused, and
  so is an image by tag. Through `kubectl port-forward` the server looks
  local to `moksha deploy`, so use `moksha deploy --push`: it pushes the
  image and deploys it by digest.
- **Its service account** needs:

  ```yaml
  apiVersion: rbac.authorization.k8s.io/v1
  kind: Role
  metadata: {name: moksha-projects, namespace: moksha}
  rules:
    - apiGroups: [apps]
      resources: [deployments]
      verbs: [get, list, create, delete]
    - apiGroups: [apps]
      resources: [deployments/scale]
      verbs: [get, patch]
  ```

#### Kubernetes pools: a Deployment's replicas

With `launcher: kubernetes` in `pools:`, for a Deployment someone else
manages (the Helm chart's pools, until it moves to `projects.launcher:
kubernetes`), the server sets the replicas of a Deployment
that runs the workers, to the same desired count an external pool
publishes. No KEDA needed:

```yaml
kubernetes:
  # api_url: http://127.0.0.1:8001   # kubectl proxy, from outside the cluster;
  #                                  # default: the pod's service account
  namespace: moksha                # default: the service account's
pools:
  - name: revenue
    launcher: kubernetes
    deployment: revenue-workers      # runs python -m moksha.worker
    min: 0
    max: 10
    steps: 2
```

- **It patches** `deployments/<name>/scale` when the count changes (and
  sets `min` at start), retrying on the next pass if the API refuses.
  In a pod it uses its service account, which needs:

  ```yaml
  apiVersion: rbac.authorization.k8s.io/v1
  kind: Role
  metadata: {name: moksha-scaler, namespace: moksha}
  rules:
    - apiGroups: [apps]
      resources: [deployments/scale]
      verbs: [get, patch]
  ```

- **The Deployment** sets `MOKSHA_SERVER` (the server's Service) and the
  group's queues and steps (`MOKSHA_QUEUES`, `MOKSHA_STEPS`). As with KEDA,
  Kubernetes picks the pod it removes: give the pods a
  `terminationGracePeriodSeconds` longer than your steps.
- **No client library**: plain HTTP to the API server, so the binary stays
  pure Go. A server that can't reach the cluster (no service account, no
  `api_url`) doesn't start.

This doesn't break "the control plane never runs user code". The leader
still runs none: workers are separate processes or pods, and dev's
supervisor is a convenience for one machine. `moksha server` supervises them the same
way, so a server that starts with queued runs starts the workers to run
them. Workers on other machines, or run by the service manager itself (a
Deployment), run with `--no-workers` on the server. Proposed next: scale
them by demand ([worker-pools.md](worker-pools.md)).

### The bucket

Data versions (their files and manifests) go to the bucket. Without a
`bucket:` section it is the directory `<data_dir>/bucket`; with one, an
S3 bucket or an S3-compatible store (MinIO, Cloudflare R2, Google Cloud
Storage's XML API):

```yaml
bucket:
  url: s3://acme-moksha/prod              # bucket and optional prefix
  endpoint: https://minio.internal:9000     # S3-compatible stores; omit for AWS
  public_endpoint: https://s3.acme.example  # what workers reach the store as, if not endpoint
  region: eu-central-1                      # default: asked of the store
  path_style: true                          # bucket in the path (MinIO)
  access_key_id_file: /run/secrets/s3_key_id
  secret_access_key_file: /run/secrets/s3_secret
  transfers: presigned                      # or server
```

- **Credentials** are files, never values in the config (trailing
  newlines don't count). Without them the usual AWS sources apply:
  `AWS_ACCESS_KEY_ID` and friends, `~/.aws/credentials`, the instance or
  pod role.
- **Only the server uses the bucket.** Workers get URLs from the server for
  what a step writes and reads: presigned URLs of the store
  (`transfers: presigned`, the default), or URLs of the server itself,
  which streams the bytes (`transfers: server`, for workers that can't
  reach the store). With the directory bucket, workers on the server's
  machine read and write it directly, and others go through the server. So
  workers need no shared filesystem and no storage credentials.
- **Conditional writes are required**: committing a data version creates
  its manifest only if it doesn't exist (`If-None-Match: *`). The server
  checks this at start and refuses a store without it.
- **One bucket per data directory**: the catalog in `state.db` points to
  manifests in the bucket, so the server refuses to start a data directory
  with another bucket than the one it was first used with.

Design and the reasoning behind it: [remote-storage.md](remote-storage.md).

### Running the server as a service

`moksha server` is `moksha dev` for a service manager (systemd,
launchd, a container) to run: the same data directory, API, web UI and
workers, from the same config and flags, but no runs printed. Its output
is its log, on stderr; `--log-format json` writes JSON lines for a log
collector, and `--log-level` works as for every command. What the workers
print goes into the log too, one record per line with a `worker` field
(`revenue-default-v1-2`); what steps print is kept by the server (the web UI shows it) and
not echoed.

```bash
moksha server --config /etc/moksha/moksha.yaml --log-format json
```

With workers something else runs (a separate unit, another machine):

```bash
moksha server --config /etc/moksha/moksha.yaml --log-format json --no-workers
MOKSHA_SERVER=http://10.0.0.5:7070 python -m moksha.worker   # in the project's directory
```

- **Health**: `GET /healthz` answers `200 ok` while the server runs and
  `503 stopped` once it has stopped.
- **Stopping**: SIGTERM or Ctrl-C stops the workers first (running steps
  finish and report, polls end within their wait; a second signal kills
  them), then closes the API and stops the server. Work that workers
  elsewhere are running carries on and is picked up on the next start, as
  with dev. Give the service manager a stop timeout longer than your
  steps' (systemd: `TimeoutStopSec=`). If the server stops on its own (it
  couldn't write its state), it stops its workers and exits with an error,
  for the service manager to restart it.
- **One per data directory**: the lock that stops a second `moksha dev`
  stops a second server too, and dev and server can't share a directory.
- **No authentication yet**: anyone who can reach the API can start,
  cancel and read runs. The server warns when `server.listen` isn't a
  loopback address; keep it on one, or reachable only by people and
  workers you trust. Tokens come later (`server.auth`).

A systemd unit for the server and its workers:

```ini
[Service]
ExecStart=/usr/local/bin/moksha server --config /etc/moksha/moksha.yaml --log-format json
Restart=on-failure
KillSignal=SIGTERM
KillMode=mixed      # SIGTERM to moksha only: it stops the workers in order
TimeoutStopSec=10min
```

### Trying failures locally

With `moksha dev` in a project whose pool runs three workers:

- **A worker dies**: `kill -9` one (dev prints PIDs). It is restarted.
  After the heartbeat timeout (a minute in dev), its running step attempt is
  `lost` and retried elsewhere if the step has a retry policy.
- **The leader restarts**: run `moksha dev --no-workers` and, in the
  project's directory, `MOKSHA_SERVER=http://127.0.0.1:7070 python -m
  moksha.worker` in two terminals, then Ctrl-C and restart dev. The worker
  retries until it's back. A step that finishes while the leader is down keeps
  its result, and its attempt keeps sending heartbeats, until the new leader
  takes the result. It isn't rerun.

## Later

Sketched for later milestones, not implemented:

- **`profile`**: `dev | kubernetes`. It is read only by
  `internal/profile`, which turns the config into concrete implementations.
  A `standalone` profile isn't needed: `moksha server` is the single-node
  server.
- **`bucket`**: `gs://` natively; today Google Cloud Storage works through
  its S3-compatible XML API.
- **Sections**: `server.auth` (tokens for workers and users), `replication`
  (Litestream), `secrets`, `log`, and limits the server enforces (maximum step
  timeout). Retention is declared per dataset in code
  (`mk.Dataset(keep=...)`); the expiry interval is the core's
  `RetentionEvery` (an hour), a config field later.
- **Environment variables** as overrides without touching the file: an
  explicit list (`MOKSHA_DATA_DIR`, `MOKSHA_LOG_LEVEL`, …), not a
  generic mapping of every field. Values from the environment inside the
  file are `${NAME}` (above).
- **Commands**: `moksha config check` and `moksha config show`, which
  shows where each value came from.
- **Secrets**: the file never holds secret values, only `*_file` paths to
  them, so it can be committed or shipped as a ConfigMap.

Why YAML: Kubernetes ConfigMaps and Helm values are YAML, and
`gopkg.in/yaml.v3` with `KnownFields` gives strict decoding. INI has no
types or nesting, and TOML would add a second format for little gain.
