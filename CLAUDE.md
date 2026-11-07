# CLAUDE.md — moksha

## What this is

moksha is a durable execution engine for data: pipelines as plain code
(runs) that survive crashes and long waits, side-effecting steps (steps)
memoized by fingerprint, and every output an immutable, versioned data version in a
catalog derived from the event history. One Go server that never runs user
code, single leader on SQLite (Litestream later); workers in the user's
environment, Python SDK first.

**Today (M4 done):** `moksha dev` serves the API (`moksha server`: the
same server for a service manager, logs only, no auth yet; both start the
config's workers); Python workers
(`sdk/python`) run pipelines and steps (`moksha dev` in a project's
directory deploys it to itself and redeploys on change), with code versions from what a step's
code reaches and runtime guards in pipeline code; runs have signals, version
markers, fan-out limits, child runs, continue_as_new, schedules and
dataset subscriptions; `moksha start`/`replay` start them and report
reuse; the SDK has a linter and testing tools. The bucket is a local
directory or `s3://` (`bucket:` in moksha.yaml); only the server opens it,
and workers on other machines move data through URLs it hands out. The v1 engine (compiled YAML pipelines) was
removed in M3c; it is in git history up to commit 33ca1ba. **Next:**
authentication (tokens for workers and users), the SDK on PyPI, then the
web UI's projects (M6) and catalog (the rest of M5). Reuse is opt-in as
`reuse=`, with `--reuse=none`, `replay --dry-run` and the reuse hint
(docs/api-simplification.md step 3, built).

Read before larger changes:

- [docs/architecture.md](docs/architecture.md) — v2 design: runs, steps, data versions, memoization, commits
- [docs/roadmap.md](docs/roadmap.md) — what is done, what is next, open decisions
- [docs/v2-migration.md](docs/v2-migration.md) — what happened to each v1 package
- [docs/failover.md](docs/failover.md) — design: nodes, leader election and state handoff (not built)
- [docs/worker-pools.md](docs/worker-pools.md) — design: the leader starts and scales the workers its queued work needs (not built)
- [docs/api-simplification.md](docs/api-simplification.md) — design: steps return values and dataframes; reuse stays opt-in as `reuse=` (steps 1–3 built: reuse=, --reuse=none, replay --dry-run, the reuse hint)
- [docs/spikes/](docs/spikes/) — spike findings (S1: Python replay, S2: code versions)
- [sdk/python/README.md](sdk/python/README.md) — writing pipelines and steps, the worker
- [docs/config.md](docs/config.md) — instance configuration (`moksha.yaml`)
- [docs/deploy.md](docs/deploy.md) — container images, compose, the Helm chart
- [docs/projects.md](docs/projects.md) — design: a repo's project file and `moksha deploy` (steps 1–3 built: the dry run, deployments, registries and secrets)
- [docs/remote-storage.md](docs/remote-storage.md) — design: `s3://` buckets, transfer URLs for remote workers (step 1 built)

Planning is lightweight: a design note in `docs/` when a decision needs one,
the next milestone broken into PR-sized steps in `docs/roadmap.md`. No
spec-kit.

## Current code

| Path | What |
|---|---|
| `internal/core` | The core: one event loop; runs (replay until blocked, sequence check, stuck), steps (retries, timeouts, leases), timers, queues (long-poll), workers seen lately (why a run waits), commits, memoization, single-flight, restore, projects (deployments as events) |
| `internal/state` | Store: event log + projections (runs, steps, timers, catalog, builds), term fencing, leader-start loader |
| `internal/dataversion` | Storage adapter for data versions as files: manifests, content hash, idempotent promote, discard |
| `internal/api` | Connect services over HTTP (`WorkerService`, `RunService`, `CatalogService`, `LogService`) and the data endpoint (`/v1/data/`, transfer URLs for workers); generated code in `gen/` from `proto/` |
| `internal/history` | A run's events told in plain words (pipeline, run, step, version, reused), for the web UI's "What happened" |
| `internal/runlog` | What steps printed and logged, per attempt: files of JSON lines, capped; not state, not in the event log |
| `internal/server` | Wires store, bucket, logs, core and API (`/healthz` too) for one process; data-dir lock; `moksha server` and `moksha dev` both run it |
| `internal/devmode` | What `moksha dev` prints on top of the server (runs and their output; `--events`: event descriptions) |
| `internal/runcli` | What `start` and `replay` do: `--param` ranges, starting and waiting over `RunService`, the replay report |
| `internal/deploycli` | What `deploy` does: a project's build from its code (in its image or in place), compared with `ListCode`, then `Deploy`; the `projects` table |
| `internal/catalogcli` | What `datasets`, `versions` and `data` print, over `CatalogService` (lineage trees) |
| `internal/schedule` | Calendar and cron math (DST-correct intervals, what is due since a cursor); the core's run schedules use it |
| `internal/clock`, `internal/layout` | Real/fake clock; bucket paths (step staging under dataset and partition, data version manifests) |
| `internal/blob` | The bucket: `file://` and `s3://` (minio-go; conditional writes for atomic create-if-absent, presigned URLs); `s3fake/` is an S3-compatible server in memory for tests |
| `internal/config` | `moksha.yaml`: data dir, listen address, bucket, `pools:` started elsewhere (external); project files (`kind: Project`) |
| `internal/supervise` | Runs and restarts worker processes for `dev` and `server` (prefixed output, SIGTERM then kill); a group that grows and shrinks |
| `internal/launch` | What starts a pool's workers besides processes: a container per worker (`docker`/`podman run`), Kubernetes Deployments for deployed projects (created, scaled, deleted; plain HTTP; `kubefake/` is the API in memory for tests); the server's secrets (a file each) |
| `internal/pools` | Scales worker pools (`min`/`max`) from the work waiting on their queues (`core.Demand`), or works out the desired count of external ones (`GET /v1/pools`); reports to the core (`SetPools`) |
| `cmd/` | `dev`, `server`, `deploy` (`--dry-run`), `projects`, `delete`, `start`, `replay`, `signal`, `schedules`, `datasets`, `versions`, `data` (and `data invalidate`), `stale`, `gc`, `version` |
| `internal/webui` | Serves the web UI from the binary (embedded `dist/`, filled by `go generate ./internal/webui`; a placeholder page without it) |
| `ui/` | Web UI: React + TypeScript + Vite on the Connect services (clients generated from `proto/` into `ui/src/gen`); vitest tests |
| `sdk/python/` | Python SDK (stdlib only, Python ≥ 3.12): `@pipeline`, `@step`, replay engine, worker; pytest runs it against a real `moksha dev` |
| `sdk/python/tests/projects/revenue/` | The revenue project the end-to-end tests run (`test_example.py`, `test_remote.py`); the example people try is in [moksha-demo](https://github.com/alpaaaca/moksha-demo) |
| `tests/` | Cross-package end-to-end tests (leader restarts) |
| `Dockerfile`, `sdk/python/Dockerfile`, `deploy/` | Images (server; worker base), `deploy/compose.yaml`, the Helm chart `deploy/helm/moksha`; `make images` |

## Words

One vocabulary, from the web UI down to the Go code: a pipeline (code
that orchestrates steps), a run (one execution of a pipeline), a step (one
side-effecting call, memoized by fingerprint), a data version (an
immutable version of a dataset, a step's output), a queue (where workers
pull run and step tasks from), a run's event log (its events, what a
replay reads) and its history (that log told in sentences,
`internal/history`). In front of people, a memoized step is "reused".
The SDK, CLI, web UI, docs, API, proto and Go code all say these.

## Rules that hold everywhere

- **The control plane never runs user code.** Workers run pipelines and steps;
  the server only records, schedules and answers.
- **Write before act.** Record a state change durably before acting on it
  (dispatching, creating a Job, cancelling).
- **Single writer.** Only the leader's core goroutine writes scheduler state.
  Inside `internal/core`, mutate state only from the loop (inputs, timers via
  `c.after`, reports via `Report`); side effects go through `c.effect` so they
  run after the batch commits.
- **Keep the core portable.** No Postgres-only, Kubernetes-only or
  cloud-specific calls in the core; they go behind the interfaces listed in
  `docs/architecture.md`. `internal/profile` (to come) is the only place that
  knows the active profile.
- **Fingerprint inputs are a format.** Changing what goes into a step's
  fingerprint (`fingerprintInputs` in `internal/core/memo.go`) invalidates
  every memoized data version — treat it as a format change.
- **Never reuse what can't be read.** A step is reused only when a committed,
  readable, non-invalid data version matches its fingerprint; when unsure, rerun.
- **Pure Go, no CGO.** The binary must stay static and cross-compile
  (`modernc.org/sqlite`, not `mattn/go-sqlite3`).

## Conventions

- Raw `database/sql`, no ORM; migrations are embedded SQL files.
- IDs are ULIDs (`github.com/oklog/ulid/v2`).
- Times are UTC wherever people see them (web UI, CLI, `moksha dev`,
  server and worker logs), and say so ("UTC"); stored and sent as UTC
  timestamps. Schedules default to UTC; a schedule's `timezone` only
  decides when its cron fires and what its intervals' days are.
- Errors for users name the location (`commands[2]: …`, `app/pipelines.py:42`)
  and report all problems at once where possible.
- Structured logs via `log/slog` to stderr; command output to stdout.
- Tests: `testify` (`assert`/`require`); no mocking frameworks — use real
  implementations (`:memory:` SQLite, temp dirs, `file://` blob, real
  subprocesses, fake clock). Unit tests next to the code; cross-package
  integration tests in `tests/`. Pure logic (`internal/schedule`, `internal/core`
  with a fake clock) gets tests with no real processes or timers.
- Keep docs in step with code: update `docs/roadmap.md` status and any
  affected doc in the same PR.

## Commands

```bash
go build -o moksha .
go vet ./...
go test ./...
go test -race ./...
go generate ./internal/api    # after changing proto/: buf lint + generate (via go run, nothing to install)
(cd ui && npm run gen)        # ...and the UI's TypeScript clients
go generate ./internal/webui  # build the web UI into the binary (needs Node; dist/ isn't committed)
(cd ui && npm ci && npm test && npm run typecheck)   # UI tests
(cd ui && npm run dev)        # UI dev server, proxying the API to a running moksha dev
(cd sdk/python && uv run --no-project --with pytest --with polars --with pandas --with pyarrow --python 3.12 pytest -q)   # Python SDK, incl. against moksha dev and dataframes
MOKSHA_TEST_S3=http://key:secret@127.0.0.1:9000/bucket go test ./internal/blob   # the bucket tests also against a real S3-compatible store (MinIO)
(cd sdk/python/tests/projects/revenue && PYTHONPATH=../../../src python3 -m moksha.lint)   # lint pipeline code

./moksha dev --config path/to/moksha.yaml   # server + the config's workers; API on 127.0.0.1:7070 (moksha-demo has a project to run)
./moksha server --config path/to/moksha.yaml --log-format json   # for a service manager; runs deployed projects' workers (--no-workers: none)
./moksha replay daily --param day=2026-10-01..2026-10-03   # start runs, report what was reused
make images && make up    # the server in a container (deploy/compose.yaml); CONTAINER=podman to choose
```
