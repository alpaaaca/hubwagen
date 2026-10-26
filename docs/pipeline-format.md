# Pipeline format

> **v1.** This format is replaced by hauls written in code (see
> [architecture.md](architecture.md)) and is removed with `internal/spec` in
> M3c. See [v2-migration.md](v2-migration.md).

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
      on_failure: stop             # stop|continue|skip_dependents

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

The spec describes what a task needs; the server decides how:

| Spec | `kubernetes` | `standalone` / `dev` |
|---|---|---|
| `runtime: {kind: job, image, resources}` | Kubernetes Job in the tenant namespace | Container on an agent in the server's `runtimes.job.pool`, via Docker/Podman |
| `runtime: {kind: agent, pool: p}` | Agent pool `p` (Deployment of the pool image) | Agents started with `--pool p`; host processes, or the pool's image if configured |
| `secrets:` | Kubernetes Secret refs | `file` or `env` secret provider |

Image digests are resolved at compile time in every profile except `dev`. For
air-gapped `standalone`, `hubwagen compile --registry-mirror` resolves against a
local registry, or `--no-resolve` accepts digests already written in the YAML.

### Execution semantics

Implemented by the scheduler core (`internal/core`); its tests are the
executable version of this section.

**Readiness.** A task is evaluated as soon as *its own* dependencies change —
there are no waves. Its `trigger_rule` decides:

| `trigger_rule` | Runs when | Otherwise |
|---|---|---|
| `all_success` (default) | every dependency succeeded (a failure with `on_failure: continue` counts as success) | `upstream_failed` as soon as one dependency failed or was cancelled; `skipped` if one was skipped |
| `all_done` | every dependency finished, whatever the outcome | waits |
| `one_failed` | the first dependency fails | `skipped` once all finished without a failure |

**`on_failure`** (when a task has finally failed, after retries):

| Value | Rest of the run | Run status |
|---|---|---|
| `stop` (default) | every unfinished task is cancelled; running attempts get a cancel | `failed` |
| `skip_dependents` | other branches continue; dependents follow their trigger rule | `failed` |
| `continue` | other branches continue; dependents treat the task as succeeded | unaffected |

A run is `succeeded` when every task finished and none failed (skipped tasks
don't count), `failed` otherwise, and `cancelled` if it was cancelled.

**Retries.** `retry.limit` is the number of retries (so `limit: 2` means up to
3 attempts). The delay before retry *n* is `initial` for `fixed`, or
`initial × 2^(n-1)` capped at `max` for `exponential`. Failures that would
repeat identically are not retried: `exec_error` (command not found or not
executable), `render_error` (a template failed, for example `.Interval` in a
run without an interval), `no_runtime` (the deployment has no runtime for the
task's kind). Lost attempts (agent gone, leader restart) and timeouts are
retried.

**Timeouts** run from the moment the attempt starts, not from dispatch. On
expiry the attempt is recorded as failed (`timeout`), the runtime is told to
kill it, and the retry policy applies; any later report from that attempt is
ignored.

**Concurrency.** At most `max_active_runs` runs of a pipeline run at once.
When a trigger arrives with all slots taken, `on_overlap` decides: `queue`
(wait in order), `skip` (no run is created; the trigger reports why), or
`cancel_running` (cancel the active runs and queue the new one).

**Run keys and idempotency.** Every run has a key; triggering an existing key
returns the existing run instead of creating another:

| Trigger | Run key |
|---|---|
| with a data interval | `<tenant>/<pipeline>/<start>_<end>` (UTC, `20060102T150405Z`) |
| with an idempotency key | `<tenant>/<pipeline>/key:<key>` |
| neither (ad-hoc) | `<tenant>/<pipeline>/run:<run-id>` (always new) |

Re-running a key whose latest run has finished creates generation `g+1` under
the same key; outputs of different generations never overwrite each other.

**Versions.** A run keeps the pipeline version it was created with, even if a
new version is published while it is queued or running.

**Schedules.** Each scheduled pipeline has a cursor that is saved together
with the runs it creates, so no interval is scheduled twice and anything
missed while hubwagen was down is caught up on restart.

- The first schedule starts at `start` (catching up from there) or, without
  `start`, at publish time: the first run is for the interval that is in
  progress when you publish.
- Calendar intervals (`hourly`, `daily`, `weekly`, `monthly`) are generated
  back to back; the cron expression only decides *when* they become due. A
  daily run fires at the first tick after its day has ended.
- Around DST changes cron skips local times that don't exist and repeats ones
  that occur twice. Calendar intervals are unaffected: each day still gets
  exactly one run. With `interval: cron`, a skipped tick makes one interval
  longer and a repeated tick makes a one-hour interval.
- `catchup: true` (default) creates a run for every missed interval, at most
  100 per batch; `catchup: false` creates one for the latest only.
- Every tick is recorded as a `CronTicked` event, including ones that created
  no run (`on_overlap: skip`, or an interval before `start`).
- A paused pipeline creates no scheduled runs (manual triggers still work).
  On resume, missed intervals follow the `catchup` setting.
- Publishing a version that changes or removes the schedule takes effect
  immediately; the cursor is kept.

## Compiled spec

The compiler normalises the definition (defaults applied, templates parsed and
checked, image digests resolved, dependencies sorted, tasks in topological order)
into `spec.Spec` (`internal/spec/spec.go`), serialised as canonical JSON with
snake_case field names. Durations become integer seconds (`timeout_seconds`,
`retry.initial_seconds`).

The API carries the spec as these canonical bytes rather than as a nested
protobuf message, so the version hash never depends on a protobuf JSON mapping.

```
api_version, tenant, name, description?, labels?
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
