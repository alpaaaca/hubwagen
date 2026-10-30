# Instance configuration

How a hubwagen instance is configured: one YAML file, `hubwagen.yaml`, with
flags on top. Code: `internal/config`.

## What belongs here

The config file describes the **instance**: where the server keeps its data,
what it listens on and, in dev, which worker processes to start next to it.
It never describes what a haul does; that is code, versioned by builds.

## Today

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Config

data_dir: .hubwagen              # state.db, bucket/, lock; default .hubwagen
# bucket:                        # where data versions go; default <data_dir>/bucket
#   url: s3://acme-hubwagen/prod
#   access_key_id_file: /run/secrets/s3_key_id
#   secret_access_key_file: /run/secrets/s3_secret
server:
  listen: 127.0.0.1:7070         # the API; default 127.0.0.1:7070

workers:                         # started by hubwagen dev, server and workers
  - name: py                     # IDs are <name>-<n>: py-1, py-2, py-3
    replicas: 3                  # default 1; 0 turns the group off
    command: [python3, -m, hubwagen.worker, --lifts, "2"]   # modules: pyproject.toml
    dir: .                       # working directory; default the file's
    env:                         # added to hubwagen's own environment
      PYTHONPATH: src
```

- **Relative paths** (`data_dir`, `workers[].dir`, the bucket's credential
  files) are relative to the config file, so a project's `hubwagen.yaml` works from any directory.
- **Where the file comes from**: `--config <path>`, else `$HUBWAGEN_CONFIG`,
  else `./hubwagen.yaml` if it exists, else the defaults. A file named by
  `--config` or `$HUBWAGEN_CONFIG` must exist.
- **Flags win**: `hubwagen dev --data-dir … --listen …` override the file,
  but only when passed.
- **Strict**: unknown fields, wrong types and duplicate keys are errors.
  Every problem is reported at once, with its location:
  `hubwagen.yaml: workers[1].replicas: must be 0 or more`.
- **Variables**: values may come from the environment, for what differs per
  machine (see below).

### Variables

`${NAME}` is replaced with the environment variable `NAME`, and
`${NAME:-default}` with its default when `NAME` is unset or empty, before
the file is parsed. So one committed file serves every machine, with a
`.env` or the service manager setting the values:

```yaml
server:
  listen: ${HUBWAGEN_LISTEN:-127.0.0.1:7070}
bucket:
  url: ${HUBWAGEN_BUCKET:-}          # unset: no bucket section, <data_dir>/bucket
  endpoint: ${HUBWAGEN_S3_ENDPOINT:-}
workers:
  - name: py
    replicas: ${WORKERS:-3}          # still a number
    command: [python3, -m, hubwagen.worker]
    env:
      PYTHONPATH: ${HUBWAGEN_SDK}    # must be set
```

- **`${NAME}` must be set**, even if empty; an unset one is an error with
  its line (`hubwagen.yaml: line 12: ${HUBWAGEN_SDK} is not set`), all of
  them at once.
- **A bucket section of empty values is no bucket**, so
  `${HUBWAGEN_BUCKET:-}` makes the bucket optional.
- **Only `${…}`**: a bare `$NAME` is left alone, so a worker command such
  as `[sh, -c, "echo $HOME"]` means what it says; `$${` stands for a literal
  `${`. Lines that are only a comment aren't expanded.
- **Textual**: the value is put in as is, so line numbers stay right. Quote
  the field (`"${NAME}"`) if a value may hold YAML characters such as `: `
  or ` #`. A value can't hold a line break.
- **Not for secrets**: credentials still come from `*_file` paths or the
  usual `AWS_*` variables, never values in the config, expanded or not.

### Workers

A worker is any process that speaks the worker API; hubwagen doesn't care
what language it is in. The config describes processes, not pipelines.
Which hauls a worker runs is up to its code: the Python worker imports the
modules its project lists under `[tool.hubwagen]` in `pyproject.toml`. It
registers the hauls they define, and the server sends it only those. One
group per project (its own `dir` and virtualenv) is enough for any number
of pipelines in it, and several projects can share the server. Every process gets three variables, which its `env`
can't set:

| Variable | Value |
|---|---|
| `HUBWAGEN_SERVER` | the API's URL, e.g. `http://127.0.0.1:7070` (an unspecified listen host such as `0.0.0.0` becomes `127.0.0.1`) |
| `HUBWAGEN_WORKER_ID` | `<name>-<n>`; the logbook records it on every dispatch |
| `HUBWAGEN_SUPERVISOR_PID` | the hubwagen process that started it; a worker stops once its parent is no longer this process |

The Python worker (`python -m hubwagen.worker`) reads all three. The last
one covers a supervisor that dies without stopping its workers (`kill -9`, a
crash). The workers run in their own process groups, so nothing else would
stop them, and the next `hubwagen dev` would start a second set. The Python
worker checks every second and then stops as on Ctrl-C: running lifts
finish and report.

`hubwagen dev` starts the workers after the API is up. It prefixes their
output with their ID (`py-2 | …`) and restarts one that exits, with backoff
from 1s doubling to 30s. On Ctrl-C it sends the workers SIGTERM and waits:
running lifts finish, and a poll in flight ends within its wait (20s by
default). Then it stops the server. A second Ctrl-C kills the workers at
once. Each worker runs in its own process group, so the terminal's Ctrl-C
reaches only hubwagen.

- `hubwagen server` starts and restarts them the same way, their output
  in its log ([below](#running-the-server-as-a-service)).
- `hubwagen dev --no-workers` and `hubwagen server --no-workers` start the
  server alone.
- `hubwagen workers` starts the same workers without a server. They connect
  to `--server` (default: from `server.listen`). Use it to restart the
  server while the workers keep running.

This doesn't break "the control plane never runs user code". The leader
still runs none: workers are separate processes, and dev's supervisor is a
convenience for one machine. `hubwagen server` supervises them the same
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
  url: s3://acme-hubwagen/prod              # bucket and optional prefix
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

`hubwagen server` is `hubwagen dev` for a service manager (systemd,
launchd, a container) to run: the same data directory, API, web UI and
workers, from the same config and flags, but no runs printed. Its output
is its log, on stderr; `--log-format json` writes JSON lines for a log
collector, and `--log-level` works as for every command. What the workers
print goes into the log too, one record per line with a `worker` field
(`py-2`); what steps print is kept by the server (the web UI shows it) and
not echoed.

```bash
hubwagen server --config /etc/hubwagen/hubwagen.yaml --log-format json
```

With workers elsewhere (another machine, a separate unit or Deployment):

```bash
hubwagen server --config /etc/hubwagen/hubwagen.yaml --log-format json --no-workers
hubwagen workers --config /etc/hubwagen/hubwagen.yaml --server http://10.0.0.5:7070
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
- **One per data directory**: the lock that stops a second `hubwagen dev`
  stops a second server too, and dev and server can't share a directory.
- **No authentication yet**: anyone who can reach the API can start,
  cancel and read runs. The server warns when `server.listen` isn't a
  loopback address; keep it on one, or reachable only by people and
  workers you trust. Tokens come later (`server.auth`).

A systemd unit for the server and its workers:

```ini
[Service]
ExecStart=/usr/local/bin/hubwagen server --config /etc/hubwagen/hubwagen.yaml --log-format json
Restart=on-failure
KillSignal=SIGTERM
KillMode=mixed      # SIGTERM to hubwagen only: it stops the workers in order
TimeoutStopSec=10min
```

### Trying failures locally

With one leader and three workers from one file:

- **A worker dies**: `kill -9` one (dev prints PIDs). It is restarted.
  After the heartbeat timeout (a minute in dev), its running lift attempt is
  `lost` and retried elsewhere if the lift has a retry policy.
- **The leader restarts**: run `hubwagen dev --no-workers` and
  `hubwagen workers` in two terminals, then Ctrl-C and restart dev. Workers
  retry until it's back. A lift that finishes while the leader is down keeps
  its result, and its attempt keeps sending heartbeats, until the new leader
  takes the result. It isn't rerun.

## Later

Sketched for later milestones, not implemented:

- **`profile`**: `dev | kubernetes`. It is read only by
  `internal/profile`, which turns the config into concrete implementations.
  A `standalone` profile isn't needed: `hubwagen server` is the single-node
  server.
- **`bucket`**: `gs://` natively; today Google Cloud Storage works through
  its S3-compatible XML API.
- **Sections**: `server.auth` (tokens for workers and users), `replication`
  (Litestream), `secrets`, `log`, and limits the server enforces (maximum lift
  timeout). Retention is declared per dataset in code
  (`hw.Dataset(keep=...)`); the expiry interval is the core's
  `RetentionEvery` (an hour), a config field later.
- **Environment variables** as overrides without touching the file: an
  explicit list (`HUBWAGEN_DATA_DIR`, `HUBWAGEN_LOG_LEVEL`, …), not a
  generic mapping of every field. Values from the environment inside the
  file are `${NAME}` (above).
- **Commands**: `hubwagen config check` and `hubwagen config show`, which
  shows where each value came from.
- **Secrets**: the file never holds secret values, only `*_file` paths to
  them, so it can be committed or shipped as a ConfigMap.

Why YAML: Kubernetes ConfigMaps and Helm values are YAML, and
`gopkg.in/yaml.v3` with `KnownFields` gives strict decoding. INI has no
types or nesting, and TOML would add a second format for little gain.
