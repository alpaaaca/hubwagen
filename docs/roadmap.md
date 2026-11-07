# Roadmap

What is built, what is next, and what is still undecided. Design rationale is
in [architecture.md](architecture.md); what v2 keeps and drops from the v1
engine is in [v2-migration.md](v2-migration.md). Keep this file current:
update a milestone's status in the same PR that changes it. Each
milestone's PR-sized steps are GitHub issues under a milestone of the same
name; the links below point to them.

## Direction

v1 (M1–M2) built a compiled-YAML orchestrator with container tasks. v2 turns
moksha into a durable execution engine for data: runs as code, steps
memoized by fingerprint, data versions in a catalog derived from event logs.

The order is chosen to prove the differentiator early. Durable runs alone
are what Temporal already offers; the reason to use moksha is a rerun or
backfill that skips unchanged work. So the first goal is the smallest system
that shows that on one machine: the Go core first (tested with a fake
worker), then data versions and memoization, then a minimal Python SDK. Everything
that only matters at scale waits.

## Status

| Milestone | Status |
|---|---|
| M1: Spec compiler (v1) | **Done** |
| M2: v1 engine in dev mode | **Done** |
| M3a: Run engine in the core | **Done** |
| M3b: Data versions, catalog, memoization | **Done** |
| M3c: Worker API + minimal Python SDK | **Done** |
| M4: Full run surface (fan-out, signals, triggers, testing) | **Done** |
| M5: Catalog in the UI, invalidation, staleness | **In progress** |
| M6: Projects everywhere, native on Kubernetes | **In progress** |
| Remote storage: object-store buckets, remote workers | **In progress** |
| Authoring API: values, dataframes, `reuse=` | **In progress** |
| Later | — |

M3a → M3b → M3c are sequential; the end of M3c is the first demo. M4 and M5
can run in parallel. M6 comes before M5's last step (the catalog in the
web UI): teams adopt moksha by running it on Kubernetes, so deploying and
operating it there should be the native way, and that starts with one way
to run workers.

## M1–M2: v1 engine (done)

Compiler (`internal/spec`), event store with term fencing (`internal/state`),
single-goroutine core with retries, timeouts, cancellation and DST-safe cron
(`internal/core`), `file://` bucket with atomic create-if-absent
(`internal/blob`), the task contract with commit markers and restart recovery
(`internal/taskexec`), in-process runtime (`internal/runtime/localrt`),
`moksha dev`/`run` with the dashboard. The per-step history is in git. [v2-migration.md](v2-migration.md) says which parts v2 reused; the rest was
removed in M3c and is in git history (commit 33ca1ba).

## M3a: Run engine in the core

Goal: a run with several steps, a timer and a retry survives a leader
restart, driven entirely from Go tests with the fake clock and a fake worker.
No Python yet.

1. ✅ **Event log events and projections** (`internal/state`): run, run task,
   step, step attempt and timer tables; events carry a run ID, so a run's
   event log is its events in order. Events `RunStarted`,
   `RunTaskQueued`/`Dispatched`/`Completed` (commands)/`Failed`,
   `RunStuck`, `RunFinished`, `StepScheduled`,
   `StepAttemptDispatched`/`Started`/`Finished`, `StepRetryScheduled`,
   `StepFinished` (result), `TimerStarted`/`Fired`/`Cancelled`; payloads are
   defined by the core. Migrations next to the v1 tables, which are dropped
   in M3c. ([#12](https://github.com/alpaaaca/moksha/issues/12))
2. ✅ **Run state machine** (`internal/core`): replay-until-blocked protocol on
   the server side — schedule a run task when a result arrives, apply the
   returned commands, dispatch steps, arm timers. Retries and timeouts for
   steps reuse the v1 logic. Restore on leader start. ([#14](https://github.com/alpaaaca/moksha/issues/14), [#15](https://github.com/alpaaaca/moksha/issues/15), [#17](https://github.com/alpaaaca/moksha/issues/17))
3. ✅ **Queues**: in-memory queues per queue, long-poll semantics behind an
   interface the API will expose; the fake worker in tests consumes it. ([#13](https://github.com/alpaaaca/moksha/issues/13))
4. ✅ **Sequence check on the server**: a run task's commands must extend the
   event log, never contradict it; a contradiction marks the run `stuck`
   until `ResumeRun` and a consistent replay. ([#16](https://github.com/alpaaaca/moksha/issues/16))

Done when: an end-to-end test in `tests/` runs a three-step run with a sleep,
kills and restarts the leader mid-run, and the run completes with each step
run once.

## M3b: Data versions, catalog, memoization

Goal: running the same run twice reuses every step, in a Go test.

1. ✅ **Data versions and datasets**: catalog tables projected from `StepFinished`
   and commit events; data version states `pending` (recorded, not yet promoted),
   `committed`, `expired`, `invalid`; latest data version per partition; upstream
   and downstream lineage. ([#18](https://github.com/alpaaaca/moksha/issues/18))
2. ✅ **Commit protocol** on `file://` (`internal/dataversion`): staging per
   attempt, data version manifests promoted with `PutIfAbsent` off the core loop,
   promote redone on restart from the manifest in the event log, staging of
   failed attempts discarded. ([#19](https://github.com/alpaaaca/moksha/issues/19))
3. ✅ **Fingerprints**: computed by the server from what the worker registers
   (`RegisterBuild`: code version per step) and what the command carries
   (inputs, canonical args, output); content hash for `file://` inputs;
   opt-in memoization for steps with an output (`memo: fingerprint` or
   `ttl:…`); `StepReused` events with the matched data version; rerun reasons
   (`not memoized`, `no previous run`, `code changed (a → b)`,
   `input changed`, `params changed`, `expired`, `unknown code version`). ([#20](https://github.com/alpaaaca/moksha/issues/20))
4. ✅ **Single-flight**: a memoized step whose fingerprint is already running
   waits for it (`StepWaiting`) and reuses its data version; if the run fails, the
   first waiter runs itself. Flights are in memory and rebuilt on restore
   (committed fingerprints are the catalog's own index). ([#21](https://github.com/alpaaaca/moksha/issues/21))

Done when: a test runs a run of memoized steps, reruns it with one step's
code version changed, and only that step and its downstream run (a step
without `memo=True` runs both times); two concurrent runs with the same
fingerprint run the step once.

## M3c: Worker API + minimal Python SDK

Goal: the first demo — `moksha replay` on a real Python example shows
reuse.

1. ✅ **Worker API** (Connect, HTTP/JSON-callable): `RegisterBuild`,
   `PollRunTask`, `CompleteRunTask`, `PollStepTask`, `CompleteStepTask`,
   `Heartbeat` (leases; tells workers what to stop). `moksha dev` serves it
   (`--listen`, default 127.0.0.1:7070), with `RunService` (`StartRun`,
   `GetRun`) next to it; `moksha start` and `moksha replay` call it. ([#22](https://github.com/alpaaaca/moksha/issues/22), [#23](https://github.com/alpaaaca/moksha/issues/23))
2. **Python SDK** (`sdk/python`): ✅ `@pipeline`, `@step`, `ctx.step`,
   `step_async`/`gather`, `ctx.sleep`, `ctx.state`, `ctx.latest_version`
   (answered and recorded by the server), `ctx.now`, `ctx.random`; replay
   until blocked; client-side sequence check with file and line; the worker
   (`python -m moksha.worker`, fixed hash seed, heartbeats); writing a
   step's output files, and reading input data versions through their manifests
   (`DataVersion.files()`, `path()`). ✅ Code versions from the code a step
   reaches and the packages it uses, no lockfile term (spike S2); pipeline code
   versions in the build ID; runtime guards in pipeline code with
   `sys.monitoring`, which make a run stuck (#25). ([#24](https://github.com/alpaaaca/moksha/issues/24), [#25](https://github.com/alpaaaca/moksha/issues/25), [#26](https://github.com/alpaaaca/moksha/issues/26))
   ✅ `moksha.yaml` (server settings and worker processes; `moksha dev`
   starts and restarts them, `moksha workers` runs them alone), see
   [config.md](config.md).
   ✅ Routing by build: builds register their runs; workers get only the
   pipelines and steps their build defines; `StartRun` refuses unknown runs.
   The worker reads its modules from `[tool.moksha]` in `pyproject.toml`.
   ✅ Memoized results: steps without an output can be memoized (their
   result is reused); a data version's content hash covers the step's result.
   ✅ Dataframes: steps return pandas/Polars frames and Arrow tables (written
   as Parquet into a data version, in the step's implicit dataset `step:<name>`
   when it has no output) and take them as arguments, loaded by annotation.
3. ✅ **Example** in `pipelines/examples/revenue/` (since moved to
   [moksha-demo](https://github.com/alpaaaca/moksha-demo); the tests run
   their copy in `sdk/python/tests/projects/revenue/`): a small Python project with
   an extract → transform → report run that writes CSV, and the demo test
   (`sdk/python/tests/test_example.py`): `moksha dev` + a worker, replays
   over a date range, a code change and a data change, each reporting what
   was reused and what ran and why. ([#27](https://github.com/alpaaaca/moksha/issues/27))
4. ✅ **Remove v1**: YAML compiler, pipeline loader, v1 state machines and
   tables, the task wrapper and local runtime, the dashboard (back in M5),
   `compile` and `run`. The calendar math moved to `internal/schedule` for
   M4 (see [v2-migration.md](v2-migration.md)). ([#28](https://github.com/alpaaaca/moksha/issues/28))

Done when: `moksha dev` + a local worker run the example, and a replay over
a date range reports reused and rerun steps with reasons.

## M4: Full run surface

Goal: the runs in [architecture.md](architecture.md) (a scheduled daily
batch with an approval gate, incremental ingestion, dynamic fan-out) run as
written. Steps, in order; each is one PR:

1. ✅ **Signals**: `ctx.wait_signal(name, timeout=)` returns the payload or
   raises `SignalTimeout`; `SignalRun` on `RunService` and
   `moksha signal <run> <name> [--data JSON]`. Signals sent before the
   run waits are kept, and each is delivered to one wait, oldest first. ([#56](https://github.com/alpaaaca/moksha/issues/56))
2. ✅ **Version markers**: `ctx.version(name, max=)`, recorded the first time,
   so old event logs replay the old branch and new runs take the new one. ([#57](https://github.com/alpaaaca/moksha/issues/57))
3. ✅ **Fan-out limits**: `gather(..., max_parallel=)` and a per-run limit on
   running steps, held by the server. ([#58](https://github.com/alpaaaca/moksha/issues/58))
4. ✅ **Child runs and `continue_as_new`**: `ctx.run(...)` (start and wait
   for the result), `ctx.start_run(...)` (fire and forget), and
   `ctx.continue_as_new(**params)` for runs that loop. ([#59](https://github.com/alpaaaca/moksha/issues/59))
5. ✅ **Schedules**: `@mk.pipeline(schedule="0 3 * * *", catch_up="latest")` on
   `internal/schedule`; builds register schedules, the leader keeps a
   cursor per schedule and starts what is due (`skip`, `latest`, `all`). ([#60](https://github.com/alpaaaca/moksha/issues/60))
6. ✅ **Dataset subscriptions**: start a run when a data version is committed to a
   dataset or partition, with debounce and batching. ([#61](https://github.com/alpaaaca/moksha/issues/61))
7. ✅ **`moksha lint`**: AST checks for clock, randomness, set iteration,
   I/O and environment variables, `except:`/`except BaseException`, and steps
   called directly in pipeline code. It is `python -m moksha.lint`, in the
   SDK, since it reads Python. Checking SQL table references that bypass
   `input()` waits for SQL steps. ([#63](https://github.com/alpaaaca/moksha/issues/63))
8. ✅ **`moksha test`**: `moksha.testing.run(run, params, steps=...,
   signals=..., children=...)` plays the server in memory against scripted
   results; `python -m moksha.testing --replay ID` or `--replay-recent N`
   replays recorded event logs (`GetEventLog`, `ListRuns`) against the local
   code. Both in the SDK, since they run Python. ([#64](https://github.com/alpaaaca/moksha/issues/64))

Done when: the architecture's daily-revenue run (schedule, quality gate
with an approval signal, fan-out with a limit) runs end to end on
`moksha dev`, and `moksha test --replay` catches a non-deterministic
change to it. ✅ `sdk/python/tests/test_m4.py` (the schedule's firing is
tested with a fake clock in the core) and `test_testing.py`.

## M5: Catalog in the UI, invalidation, staleness

Goal: the catalog is something people use, not just a table: they can see
what exists and what it was built from, mark bad data, see what is out of
date and why, and old versions go away on their own. Steps, in order;
each is one PR:

1. ✅ **Catalog API and CLI**: a `CatalogService` (datasets, a dataset's
   partitions with their latest data versions, a data version with its manifest, its
   inputs and what was built from it) and `moksha datasets`,
   `moksha data versions <dataset>`, `moksha data version <id>` (lineage up and
   down). Everything after this reads through it. ([#73](https://github.com/alpaaaca/moksha/issues/73))
2. ✅ **Invalidation**: `moksha data version invalidate <id> --reason …` marks the
   data version and everything built from it, transitively, invalid
   (`DataVersionInvalidated` events with the cause), after showing what it will
   mark (`--dry-run`). Invalid data versions are never reused, so a replay of the
   affected runs, which the command prints, recomputes exactly that set. ([#74](https://github.com/alpaaaca/moksha/issues/74))
3. ✅ **Staleness**: a partition's latest data version is stale when one of its
   inputs' datasets has a newer data version for that partition, or when the
   newest build's code version of the step that built it differs. The
   catalog API answers it per dataset and partition with the reason, and
   `moksha stale [dataset]` lists them with the replay that refreshes
   them. ([#75](https://github.com/alpaaaca/moksha/issues/75))
4. ✅ **Bucket layout by dataset**: a step with an output stages under
   `data/<tenant>/<dataset>/<partition>/<attempt>/` instead of the run's
   ID, so the bucket reads like the catalog; existing data versions keep their
   paths (manifests point at them). ([#76](https://github.com/alpaaaca/moksha/issues/76))
5. ✅ **Retention and expiry**: per dataset, keep the latest data version of each
   partition plus those younger than an age or among the newest N
   (`mk.Dataset(..., keep=...)`, registered with the build). A leader timer
   and `moksha gc` expire the rest as events (`DataVersionExpired`, then the
   files go), never a data version an open run reads or the latest of a
   partition; expired data versions stay in the catalog as lineage records. ([#77](https://github.com/alpaaaca/moksha/issues/77))
6. ✅ **Web UI: runs**: a web UI in TypeScript (React + Vite) under `ui/`,
   calling the Connect services from the browser with clients generated
   from `proto/` (`@connectrpc/connect-web`, through the same
   `buf generate`). `moksha dev` serves it on the API's port, embedded
   in the binary (`go:embed`), so there is still one binary and nothing
   else to run; during UI work, Vite's dev server proxies to a running
   `moksha dev`. This step: the scaffolding (build, embed, generated
   clients, tests) and the runs views, organised as pipelines (runs by
   name, titled from code: `@mk.pipeline(title=...)` or the docstring) and
   their runs: a pipeline's steps as a DAG (a step comes after the steps
   whose data versions it reads), its schedule and runs with inputs, duration,
   progress and result; a run's steps on the DAG and a timeline, why each
   ran, its children and its (folded) event log; live updates by polling;
   starting, signalling, cancelling and resuming runs. ([#78](https://github.com/alpaaaca/moksha/issues/78))
   ✅ **Run logs**, added to this step: what a step prints and logs is
   captured per attempt by the worker (stdout, stderr and `logging`, by
   thread; a failure's traceback too) and kept by the server outside the
   event log (`LogService`, files under the data dir, 1 MiB per attempt).
   A run's page shows them, all steps or one, live while it runs;
   `moksha dev` prints runs and their output instead of engine events
   (`--events` for those). Not captured: output that bypasses Python
   (subprocesses, C libraries) and pipeline code, which replays (a
   replay-safe `ctx.log` later).
   ✅ **Run history**, added to this step: the start page lists the
   pipelines with what to know about each (description, cron and
   timezone or the datasets it waits for, next run, recent runs as
   squares, last run, how many succeeded, usual duration); a pipeline's
   page shows its recent runs as a grid, a column per run with a bar for
   its duration and a row per step with a square for how that step went
   (fan-outs folded), each square opening the run on that step.
   `ListRuns` can return each run's steps (`with_steps`). The revenue
   example gets a scheduled pipeline (`nightly`, 03:00 Berlin).
   ✅ **Plain words**, added to this step: a run's page tells what
   happened in sentences instead of raw events ("extract_orders done in
   0.8 s, wrote a new version of raw.orders for 2026-10-28", "reused the
   existing version …: same inputs and same code"), from `GetHistory`
   (`internal/history`); the engine's bookkeeping and the raw events only
   with "engine details". Each step says why it ran in words (`Step.why`);
   the UI says pipeline, run, step, version and reused, with a hint for
   each. Scheduled pipelines start by hand with their latest interval
   (`Schedule.latest_params_json`).
   ✅ **Plain words in the SDK and CLI**: `@mk.pipeline`, `@mk.step`,
   `ctx.step`, `ctx.step_async`, `ctx.run`, `ctx.start_run`,
   `ctx.latest_version`, `mk.DataVersion`, `mk.StepFailed`,
   `mk.RunFailed`, `testing.run(steps=, run_steps=, versions=)`; a
   subscribed pipeline takes its new versions as `versions`. The CLI says
   pipeline, run, step and version: `moksha versions` and `moksha data`,
   the replay report in words ("1 ran (its code changed …)").
   ✅ **One vocabulary**: the engine's own names (haul, lift, pallet,
   logbook, dock) are gone from the API, proto, Go code, SDK, UI and
   docs, and the SDK's aliases for them with it: pipeline, run, step, data
   version, event log, queue everywhere. A clean break: the migrations
   start over (v1's are folded away), so a data directory from before
   doesn't open, and workers and server must be of the same version.
   ✅ **Times in UTC**: every time people see is UTC and says so: the
   web UI (times, run history and log clocks, intervals in inputs), the
   CLI, the server's and the worker's logs. Schedules default to UTC (the
   example's `nightly` too); a `timezone` still sets when a cron fires.
   ✅ **`moksha server`**, added to this milestone: the server of
   `moksha dev` on its own, for a service manager: no workers (they run
   with `moksha workers` or the service manager), no console, logs only
   (`--log-format json`), `GET /healthz`, SIGTERM to stop, an error exit
   if the core stops. The wiring moved from `internal/devmode` to
   `internal/server`. No authentication yet: it warns when it listens
   beyond loopback. Since worker pools step 1 it starts and restarts the
   config's `workers:` like dev (their output in its log), stopping them
   before itself; `--no-workers` for workers run elsewhere.
   ✅ **Why a run waits**, added to this milestone: the leader remembers,
   in memory like leases, which workers polled lately (build, queues,
   kinds; forgotten when they hang up, after a few seconds idle, or after
   a minute without a heartbeat while running work). A run in progress with
   queued work says why (`Run.waiting`): no worker polls its queue, none
   has a build that defines its pipeline or step, they are all busy, or
   the queue's limit is reached; `ListWorkers` lists the workers. The web
   UI shows it on the run (with the workers) and in run lists;
   `moksha start --wait`, `replay` and `moksha dev` print it once a
   run has waited a few seconds.
   ✅ **Worker pools** (worker-pools step 2): a `workers:` group with
   `min` and `max` is scaled by `moksha server` and `dev` from the work
   waiting on its `queues` (`steps` per worker), down after `idle_after`;
   `ListPools`, and "a worker is starting in pool …" on a waiting run.
   ✅ **External pools** (worker-pools step 3): `launcher: external`
   groups are started by something else; the server publishes how many
   workers each wants at `GET /v1/pools/<name>`, for KEDA's `metrics-api`
   trigger or an HPA.
   ✅ **Container and Kubernetes pools** (worker-pools step 4):
   `launcher: container` runs workers as `docker`/`podman run` of an
   image, scaled like processes; `launcher: kubernetes` sets a
   Deployment's replicas through the API (service account or `kubectl
   proxy`), from the same desired count. A pool with `min: 0` runs one
   worker at server start until its code is registered, so a fresh
   install needs no manual step.
   ✅ **Registries and secrets** (projects step 3): `deploy` pushes the
   image and deploys it by digest to a server elsewhere; the server pulls
   with `projects.auth_file`; `{secret:}` env is read from
   `projects.secrets_dir` when a worker starts, never stored or sent.
   ✅ **Deployments** (projects step 2): `moksha deploy` records a
   project's next version (its build registered with it) and the server
   runs its pools, replacing the old version's; `moksha projects`,
   `moksha delete`.
   ✅ **`moksha deploy --dry-run`** (projects step 1): a repo's
   project file (`kind: Project`), its build worked out in its image or in
   place, and what deploying it would change: steps that run again
   because their code changed, new ones, ones no longer in the code
   (`ListCode`).
   ✅ **Container images and a Helm chart**: images for the server and
   the worker base, `deploy/compose.yaml` for one
   machine, and `deploy/helm/moksha` (server on a PVC, a Deployment per
   pool scaled by the server); [deploy.md](deploy.md). Not yet: CI that
   publishes the images.
   ✅ **Variables in `moksha.yaml`**: `${NAME}` and `${NAME:-default}`
   from the environment, so one committed config serves every machine
   (bucket, port, paths); an unset `${NAME}` is an error with its line.
7. **Web UI: catalog**: datasets, partitions and data versions with stale and
   invalid marked, lineage up and down as a graph, a data version's files, and
   invalidating from the UI (with the same dry run first);
   schedules and subscriptions. ([#79](https://github.com/alpaaaca/moksha/issues/79))

Done when: in the revenue example, invalidating a `raw.orders` data version
marks the downstream data versions invalid, `moksha stale` and the web UI
show what to recompute, the replay it suggests recomputes exactly that,
and retention removes old versions while keeping the latest and anything
in use.

Open questions, to settle in the step that needs them:

- Dashboard: decided, a web UI in TypeScript, not a terminal UI. Its build
  isn't committed: `go generate ./internal/webui` builds it into the binary,
  and a binary built without it serves a page saying how; release
  binaries (GoReleaser, on a published release) build it in CI.
- Retention: decided, in code (`mk.Dataset(keep=mk.Keep(...))`, versioned
  with the build); a config override can come later.
- Staleness by code version: "current" is the newest build that defines
  the step; a stale data version from an old build may still be what an older
  worker would produce.

## M6: Projects everywhere, native on Kubernetes

Goal: one way to run workers, the same on a laptop and on a cluster. A
project has pools, a pool runs workers, and moksha starts and scales them:
processes on one machine, pods on Kubernetes. Today three things are
called a worker and pools are declared in two places
([projects.md](projects.md#one-worker-pools-in-one-place)). Steps, in
order; each is one PR (projects steps 4–6):

1. ✅ **`moksha dev` on deploy**: in a directory with a project file,
   dev starts the server and deploys the directory in place (process
   launcher), redeploying when the code changes; nothing when the server
   already runs it as it is. The test project
   (`sdk/python/tests/projects/revenue`) has a project file
   (`test_dev_deploys_the_project_and_its_changes`).
2. ✅ **No `moksha workers`; `workers:` becomes `pools:`**: the command
   goes; the server's config keeps only pools it doesn't start
   (`launcher: external`), under `pools:`;
   `workers:` is an error that says what to write instead. README,
   config.md and deploy.md lead with one machine, then Kubernetes.
3. ✅ **Kubernetes deploys**: `moksha deploy` to a server in a cluster
   creates, updates and drains a Deployment per pool and version;
   `{secret:}` from Kubernetes Secrets; the Helm chart installs only the
   server and its RBAC. Two PRs:
   ✅ the server's side (`projects.launcher: kubernetes`,
   `internal/launch/deployments.go`, tested against `launch/kubefake`);
   ✅ the chart (server, ConfigMap and RBAC only; tried on a kind
   cluster), and the server's `pools:` are `launcher: external` only.
4. **moksha-demo**: one project (no `server/` and `elsewhere/` split):
   `moksha dev` first, then a Kubernetes section (a kind cluster,
   `helm install`, `moksha deploy`, pods scaling from zero for a
   backfill).
   The release pushes the chart to `oci://ghcr.io/alpaaaca/charts/moksha`,
   so the demo needs no checkout of moksha. The demo's change
   (alpaaaca/moksha-demo, branch `one-project`) is for v0.2.0 and merges
   once it is released.

✅ **Rollouts take no new work to the old version**, added to this
milestone: a worker of a project's replaced build gets no run task of its
pipelines and no step the new build scheduled, while it drains; the steps
its build scheduled before stay open to it and to the new workers, and
the new version's pool counts the work as demand
([projects.md](projects.md), step 7 of the design).

Done when: the demo runs from the release binary with `moksha dev`, and
on a kind cluster with `helm install` and `moksha deploy`, with no
`workers:` anywhere and the same project file for both.

After M6, before a team shares a cluster: authentication (`server.auth`,
see Later).

## Authoring API: values, dataframes, `reuse=`

Goal: a pipeline is plain functions with `@mk.pipeline` and `@mk.step`;
steps take and return values and dataframes; reuse across runs is one
annotation that moksha points out. Design and decisions:
[api-simplification.md](api-simplification.md). Steps, each one PR:

1. ✅ **Result data versions**: steps without an output are memoized;
   the content hash covers the result.
2. ✅ **Codecs**: pandas, Polars and Arrow in and out of steps, loaded by
   annotation.
3. ✅ **`reuse=`** (decided: opt-in, not reuse by default): `reuse=True` /
   `reuse="1h"` (or a `timedelta`) in the SDK, the same policy on the
   wire; `memo=` warns (`FutureWarning`, at the decorator's line, with
   what to write) and lint flags it (MK007); the example project and docs
   say `reuse=`.
4. ✅ **`--reuse=none` and `replay --dry-run`**: recompute a run on
   purpose; see beforehand which steps of the latest earlier runs the
   current code would run again. ✅ `--reuse=none` (`StartRun.reuse`,
   kept with the run, inherited by its children and continuations; its
   steps don't join another run's flight). ✅ `--dry-run` (`PlanRuns`):
   per set of inputs, the latest earlier run with them (a completed one
   if any) and its steps: reused, reused unless a step before it returns
   something new, or run again because it has no `reuse=`, its code
   changed since (its project's deployed build, else the newest
   registered one), or it didn't complete;
   nothing starts and no code runs.
5. ✅ **The reuse hint**: every step fingerprinted (a step without
   `reuse=` is never looked up and never waits on another run's); the
   replay report and a run's page in the web UI say when such a step got
   the same result as every other run of its fingerprint
   (`Step.same_result_runs`); `reuse=False` says no on purpose and gets
   no hint.
6. **References** (`mk.table`, version tokens, the existence check) and
   **object-store checksums**: api-simplification steps 4–5.

## Remote storage

Goal: workers run anywhere, with no filesystem shared with the server, and
data versions live in an object store. Design:
[remote-storage.md](remote-storage.md). Steps, each one PR:

1. ✅ **`s3://` buckets and transfer URLs**: `internal/blob` opens `s3://`
   (AWS and S3-compatible stores; conditional writes required, checked at
   start); `moksha.yaml` `bucket:` with credential files; the worker API
   hands a running attempt URLs to upload its output and download its
   inputs (presigned, or through the server's `/v1/data/` for `file://`
   buckets and `transfers: server`); the Python worker uses them unless it
   sees the server's bucket directory (`--data auto|local|remote`). A data
   directory is tied to one bucket. Tests: `s3fake`, an S3-compatible server
   in memory; a real store with `MOKSHA_TEST_S3`.
2. **MinIO in CI**: run the `internal/blob` and `internal/api` tests
   against a MinIO container (`MOKSHA_TEST_S3`, and the API and SDK tests
   too), so signatures, checksums
   and conditional writes are checked by a real store on every PR.
3. **Large files**: presigned multipart uploads for files over a part size
   (one PUT is capped at 5 GB on S3), and ranged downloads, so Parquet
   readers can read only what they need.
4. **Input cache on workers**: keep downloaded input files across attempts
   (data versions never change), bounded by size, so a fan-out over one input
   downloads it once per worker.
5. **`gs://`**: Google Cloud Storage natively (signed URLs,
   `ifGenerationMatch=0`), for setups without HMAC keys for its XML API.

Done when: a worker on another machine runs the revenue example against a
server whose bucket is an object store, and reruns reuse unchanged steps
(step 1, tested with s3fake; a real store checks signatures from step 2).

Open questions:

- Workers with their own store credentials (reading inputs straight from
  the store, e.g. a warehouse engine scanning Parquet in place): an opt-in
  mode later, if transfer URLs turn out too slow for some steps.

## Later

Roughly in order of expected need:

- OpenLineage export.
- SQL steps (`step.sql`) and a dbt step.
- Warehouse and table-format adapters (object-store buckets: see "Remote storage").
- Pod steps: local containers in single-node mode, Kubernetes Jobs in a
  cluster (starting from the v1 task contract).
- Auth (tokens for workers and users, `server.auth`; `moksha server` warns
  until then; it also guards the transfer URLs), tenants and per-queue budgets; Litestream replication and failover
  (design: [failover.md](failover.md)).
- `moksha verify`, `keep_fresh`, data modes, build pinning (`moksha
  deploy` and projects: M6).
- Go SDK.
- Sharding, Postgres, Kubernetes Lease election.

## Spikes

| | Question | Blocks | Result |
|---|---|---|---|
| [S1](https://github.com/alpaaaca/moksha/issues/10) | Python replay prototype: how slow is full replay for a 1,000-event event log? Does patching clock/random in a running interpreter catch typical mistakes without breaking libraries imported by run modules? | M3c | **Done**: full replay is ~1 ms for 1,000 events (O(n²) over a run's life); guard with `sys.monitoring`, not patching (13/13 caught, 0 libraries broken); call sites via `sys._getframe` are free. [Findings](spikes/s1-python-replay.md) |
| [S2](https://github.com/alpaaaca/moksha/issues/11) | Code version from imports: can the SDK resolve a step's project-local imports reliably (relative imports, `importlib`, namespace packages), and how often does the lockfile term cause reruns in a real project? | M3c | **Done**: walk live objects from the step (`co_names`, globals, closures, annotations), not import statements; AST hashes per function, class and constant; per-step distribution versions instead of the lockfile, which would rerun everything on a dev-tool bump. [Findings](spikes/s2-code-version.md) |
| [S3](https://github.com/alpaaaca/moksha/issues/29) | Load: one leader with group commit at the target load (first v1 signal: ~60k events/s in batches of 128 on an M1 Mac). Re-measure with event log events. | Load target sign-off | |
| [S4](https://github.com/alpaaaca/moksha/issues/30) | Litestream as a library with a replica path per leader term (from v1's R1). | Failover ([failover.md](failover.md), option A) | |

## Open decisions

| Decision | Proposed default | Needed by |
|---|---|---|
| CLI name | Keep `moksha`; maybe add `mk` as a short alias | M3c |
| Python SDK packaging | `sdk/python` in this repo, published as `moksha` on PyPI | M3c |
| Memoization default | **Decided** (2026-11-07): opt-in per step, renamed `reuse=True` / `reuse="1h"` (`memo=` deprecated); moksha points out steps that would be reused; `--reuse=none` and `replay --dry-run` per run. Not reuse by default: a forgotten opt-out would reuse stale results silently ([api-simplification.md](api-simplification.md#4-reuse-opt-in-as-reuse-with-moksha-pointing-out-where-it-helps)) | Done |
| Code version default | Decided ([S2](spikes/s2-code-version.md)): code the step reaches + versions of the packages it uses + Python version + `deps=`, overridable with `version=` | M3b |
| dbt integration | One step per project, one data version per model | Later |
| Catalog interoperability | OpenLineage export first; Iceberg REST catalog later, if at all | Later |
| Failover | Nodes that supervise workers and can lead; bucket lease + Litestream first, embedded Raft if no-loss is needed ([failover.md](failover.md)) | Failover |
| Projects | A repo's `moksha.yaml` (`kind: Project`: build, pools, env) and `moksha deploy`: build and push an image, show what reruns, the server runs and scales its workers; `moksha delete` ([projects.md](projects.md)). Decided: pools only from the project file, no `moksha workers`, the server's `workers:` becomes `pools:` for external pools (M6) | Before production use |
| Worker pools | The leader starts and scales workers per pool (`min`/`max`, queues, capacity) from queued work; `moksha server` supervises `workers:` (step 1, done); pools of processes with `min`/`max` (step 2, done); desired counts for KEDA (step 3, done); container and Kubernetes launchers (step 4, done) ([worker-pools.md](worker-pools.md)) | Before production use |
| `standalone` profile | Decided: no separate profile; `moksha server` runs the single-node server (no auth yet) | Done |
| Load target for one leader | 100 step transitions/s sustained, 2,000 running steps | S3 |
| Licence and distribution | Apache 2.0; binaries for macOS and Linux on each GitHub release ([deploy.md](deploy.md#binaries)); PyPI for the SDK still open | Before first release |
