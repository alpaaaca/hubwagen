# Pipeline format

Reference for pipeline definitions and what `hubwagen compile` turns them into.
Implemented in `internal/spec`; the tests there (`compile_test.go`) are the
executable version of the rules below.

## Authoring format

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Pipeline
metadata:
  name: daily_orders               # [a-z0-9_-]{1,63}, unique per tenant
  tenant: analytics                # must exist on the target control plane
  description: "Load and model yesterday's orders"
  labels: {team: analytics}

spec:
  schedule:                        # optional; omit for manual/asset-only pipelines
    cron: "0 2 * * *"              # standard 5-field cron
    timezone: Europe/Berlin        # IANA name; default UTC
    interval: daily                # hourly|daily|weekly|monthly|cron (= previous tick → this tick)
    catchup: true                  # create runs for missed intervals (default true)
    start: 2026-01-01              # earliest interval to schedule

  triggers:                        # optional; asset-driven runs
    - asset: raw.orders            # tenant-local asset name
      partition: "{{ .Asset.Partition }}"   # becomes the run's partition key

  params:                          # declared params, with defaults; manual runs may override
    full_refresh: {type: bool, default: false}

  concurrency:
    max_active_runs: 1
    on_overlap: queue              # queue|skip|cancel_running

  defaults:
    retry: {limit: 2, backoff: exponential, initial: 30s, max: 10m}
    timeout: 30m
    runtime: {kind: agent, pool: py312}

  tasks:
    - id: extract
      command: ["python", "-m", "jobs.extract"]
      args: ["--from={{ .Interval.Start }}", "--to={{ .Interval.End }}"]
      env: {LOG_LEVEL: info}
      secrets:                     # Kubernetes Secret refs in the tenant namespace
        - {name: warehouse-creds, key: password, env: WAREHOUSE_PASSWORD}
      outputs:
        - name: orders_raw
          asset: raw.orders        # optional: commit also updates this asset partition
          partition: "{{ .Interval.Start | date \"2006-01-02\" }}"

    - id: transform
      depends_on: [extract]
      runtime:
        kind: job                  # heavy task → its own Kubernetes Job
        image: ghcr.io/acme/dbt@sha256:3f1c…   # required for kind: job
        resources: {cpu: "2", memory: 4Gi}
      command: ["dbt", "build"]
      args: ["--vars", "{run_date: '{{ .Interval.Start | date \"2006-01-02\" }}'}"]
      inputs:
        - name: orders
          from: extract.orders_raw # <task-id>.<output-name>
      timeout: 2h
      on_failure: stop             # stop|continue|skip_dependents (v1 semantics)

    - id: notify
      depends_on: [transform]
      trigger_rule: all_done       # all_success (default)|all_done|one_failed
      command: ["notify"]
      runtime: {kind: agent, pool: tools}
```

### Field rules

| Field | Rule |
|---|---|
| `tasks[].id` | Unique within the pipeline, `[a-z0-9_]{1,63}` |
| `depends_on` | Must reference existing task IDs; the graph must be acyclic |
| `command` | Required. The image's own entrypoint is not used: the task wrapper becomes the entrypoint (see [task-contract.md](task-contract.md)) |
| `runtime.kind: agent` | Requires `pool`; the pool's image is used; `image` is not allowed |
| `runtime.kind: job` | Requires `image`; tags are resolved to digests at compile time (dev mode allows tags) |
| `inputs[].from` | Must reference an output of an upstream task (a direct or transitive dependency) |
| `retry.limit` | 0–10 |
| `timeout` | Go duration, 1s–48h; default from `defaults.timeout`, else 1h |
| Templates | Only the variables and functions below; anything else fails compilation |

### Template variables

| Variable | Meaning |
|---|---|
| `.Interval.Start`, `.Interval.End` | Run's data interval, RFC 3339 in the pipeline timezone (empty for ad-hoc runs) |
| `.Run.ID`, `.Run.Key`, `.Run.Generation` | Run identity |
| `.Run.Date` | `YYYY-MM-DD` the run was created, in the pipeline timezone (not a data interval) |
| `.Pipeline`, `.Tenant` | Names |
| `.Params.<name>` | Declared params |
| `.Asset.Name`, `.Asset.Partition` | Only in asset-triggered pipelines |
| `.Inputs.<name>`, `.Outputs.<name>` | Object-storage URIs (also available as env vars) |

Functions: `date "<go layout>"`, `quote`, `default`, `lower`, `upper`, and the comparison
built-ins `eq ne lt le gt ge and or not len printf`. Not allowed: `{{ range }}`, `{{ with }}`,
`{{ define }}`/`{{ template }}`, variables (`$x`), bare `{{ . }}`, `index`, `call`.

### Runtimes per profile

The spec describes what a task needs; the server decides how (spec FR-038):

| Spec | `kubernetes` | `standalone` / `dev` |
|---|---|---|
| `runtime: {kind: job, image, resources}` | Kubernetes Job in the tenant namespace | Container on an agent in the server's `runtimes.job.pool`, via Docker/Podman |
| `runtime: {kind: agent, pool: p}` | Agent pool `p` (Deployment of the pool image) | Agents started with `--pool p`; host processes, or the pool's image if configured |
| `secrets:` | Kubernetes Secret refs | `file` or `env` secret provider |

Image digests are resolved at compile time in every profile except `dev`. For
air-gapped `standalone`, `hubwagen compile --registry-mirror` resolves against a
local registry, or `--no-resolve` accepts digests already written in the YAML.

### v1 compatibility (dev mode only)

A file with top-level `name` and `steps` (v1 format) compiles as:

| v1 | v1alpha1 |
|---|---|
| `steps[]` | `tasks[]` |
| `executor: shell`, `command: "…"` | `runtime: {kind: agent, pool: local}`, `command: ["/bin/sh", "-c", "…"]` |
| `{{ .RunDate }}` | `{{ .Run.Date }}` (v1's RunDate was the run's own date, not a data interval) |
| `{{ .RunID }}`, `{{ .DAGName }}` | `{{ .Run.ID }}`, `{{ .Pipeline }}` |
| `{{ .Env.X }}` | rejected — use `env:` or `secrets:` |
| `executor: http` / `sql` | rejected with a migration hint |
| `retry.interval_sec`, `backoff: true` | `retry.initial`, `backoff: exponential` (a step's retry block replaces the DAG's entirely, as in v1) |
| `schedule`, `timezone` | `schedule: {cron, timezone, interval: cron, catchup: false}` |
| `concurrency.on_overlap` unset | `skip` (v1's default) |
| no `timeout_sec` | `timeout: 48h` (v1 steps had no timeout) |
| DAG-level `env` | merged into every task's `env`, task values winning |
| `executor: noop` | `command: ["true"]` |
| tenant | `default` |

## Compiled spec

The compiler normalises the definition (defaults applied, templates parsed and
checked, image digests resolved, dependencies sorted, tasks in topological order)
into `spec.Spec` (`internal/spec/spec.go`), serialised as canonical JSON with
snake_case field names. Durations become integer seconds (`timeout_seconds`,
`retry.initial_seconds`). Specs compiled from v1 files carry
`"source_format": "v1"`, which servers outside dev mode reject.

The API carries the spec as these canonical bytes rather than as a nested
protobuf message, so the version hash never depends on a protobuf JSON mapping.

```
api_version, source_format?, tenant, name, description?, labels?
schedule?     {cron, timezone, interval, catchup, start?}        start: RFC 3339 UTC
triggers[]?   {asset, partition}
params[]?     {name, type, default?}                             sorted by name
concurrency   {max_active_runs, on_overlap}
tasks[]       in topological order (ties by id)
  {id, depends_on[]?, trigger_rule, on_failure,
   runtime {kind, pool? | image?, resources? {cpu?, memory?}},
   command[], args[]?, env{}?, secrets[]? {name, key, env},
   inputs[]? {name, from_task, from_output},
   outputs[]? {name, asset?, partition?},
   retry {limit, backoff, initial_seconds, max_seconds},
   timeout_seconds}
```

`?` marks fields omitted when empty.

**Version hash**: `sha256(JCS(spec))`, hex-encoded, where JCS is RFC 8785
canonical JSON (integers only — the spec has no floats). `compile --out` writes
exactly these bytes to `<out>/<tenant>/<pipeline>/<hash>.json`, so
`sha256sum` of the file equals its version ID. Field order, YAML formatting and comments therefore don't
affect the hash; any semantic change does.

## CLI

```bash
hubwagen compile pipelines/ --out build/specs/       # validate + compile; network only for registry lookups
hubwagen compile pipelines/ --check                  # CI lint mode: exit non-zero on any error, write nothing
hubwagen compile pipelines/ --no-resolve             # keep image tags (dev, offline)
hubwagen compile pipelines/ --registry-mirror registry.internal:5000   # resolve digests via a mirror
```

Directories are searched recursively for `*.yaml`/`*.yml`. If any file fails,
nothing is written. Two files defining the same `tenant/name` is an error.

Planned (needs the API, see [roadmap.md](roadmap.md)): `hubwagen publish
build/specs/ --server …` uploads specs and registers them; idempotent per hash.
