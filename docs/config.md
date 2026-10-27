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
server:
  listen: 127.0.0.1:7070         # the API; default 127.0.0.1:7070

workers:                         # started by hubwagen dev and hubwagen workers
  - name: py                     # IDs are <name>-<n>: py-1, py-2, py-3
    replicas: 3                  # default 1; 0 turns the group off
    command: [python3, -m, hubwagen.worker, app.hauls, --lifts, "2"]
    dir: .                       # working directory; default the file's
    env:                         # added to hubwagen's own environment
      PYTHONPATH: src
```

- **Relative paths** (`data_dir`, `workers[].dir`) are relative to the
  config file, so a project's `hubwagen.yaml` works from any directory.
- **Where the file comes from**: `--config <path>`, else `$HUBWAGEN_CONFIG`,
  else `./hubwagen.yaml` if it exists, else the defaults. A file named by
  `--config` or `$HUBWAGEN_CONFIG` must exist.
- **Flags win**: `hubwagen dev --data-dir … --listen …` override the file,
  but only when passed.
- **Strict**: unknown fields, wrong types and duplicate keys are errors.
  Every problem is reported at once, with its location:
  `hubwagen.yaml: workers[1].replicas: must be 0 or more`.

### Workers

A worker is any process that speaks the worker API; hubwagen doesn't care
what language it is in. Every process gets two variables, which its `env`
can't set:

| Variable | Value |
|---|---|
| `HUBWAGEN_SERVER` | the API's URL, e.g. `http://127.0.0.1:7070` (an unspecified listen host such as `0.0.0.0` becomes `127.0.0.1`) |
| `HUBWAGEN_WORKER_ID` | `<name>-<n>`; the logbook records it on every dispatch |

The Python worker (`python -m hubwagen.worker`) reads both.

`hubwagen dev` starts the workers after the API is up. It prefixes their
output with their ID (`py-2 | …`) and restarts one that exits, with backoff
from 1s doubling to 30s. On Ctrl-C it sends the workers SIGTERM and waits:
running lifts finish, and a poll in flight ends within its wait (20s by
default). Then it stops the server. A second Ctrl-C kills the workers at
once. Each worker runs in its own process group, so the terminal's Ctrl-C
reaches only hubwagen.

- `hubwagen dev --no-workers` starts the server alone.
- `hubwagen workers` starts the same workers without a server. They connect
  to `--server` (default: from `server.listen`). Use it to restart the
  server while the workers keep running.

This doesn't break "the control plane never runs user code". The leader
still runs none: workers are separate processes, and dev's supervisor is a
convenience for one machine. In `standalone` and `kubernetes`, workers are
started by whatever runs services there (systemd, a Deployment), and the
`workers:` section doesn't apply.

### Trying failures locally

With one leader and three workers from one file:

- **A worker dies**: `kill -9` one (dev prints PIDs). It is restarted.
  After the heartbeat timeout (a minute in dev), its running lift attempt is
  `lost` and retried elsewhere if the lift has a retry policy.
- **The leader restarts**: run `hubwagen dev --no-workers` and
  `hubwagen workers` in two terminals, then Ctrl-C and restart dev. Workers
  retry until it's back; lifts that were running report to the new leader
  and aren't rerun.

## Later

Sketched for later milestones, not implemented:

- **`profile`**: `dev | standalone | kubernetes`. It is read only by
  `internal/profile`, which turns the config into concrete implementations.
- **`bucket`**: `s3://…`; today the bucket is always `file://<data_dir>/bucket`.
- **Sections**: `server.auth` (tokens for workers and users), `replication`
  (Litestream), `secrets`, `log`, and limits the server enforces (maximum lift
  timeout, retention).
- **Environment variables**: an explicit list (`HUBWAGEN_DATA_DIR`,
  `HUBWAGEN_LOG_LEVEL`, …), not a generic mapping of every field.
- **Commands**: `hubwagen config check` and `hubwagen config show`, which
  shows where each value came from.
- **Secrets**: the file never holds secret values, only `*_file` paths to
  them, so it can be committed or shipped as a ConfigMap.

Why YAML: Kubernetes ConfigMaps and Helm values are YAML, and
`gopkg.in/yaml.v3` with `KnownFields` gives strict decoding. INI has no
types or nesting, and TOML would add a second format for little gain.
