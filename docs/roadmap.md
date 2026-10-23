# Roadmap

What is built, what is next, and what is still undecided. Design rationale is in
[architecture.md](architecture.md). Keep this file current: update a
milestone's status in the same PR that changes it.

## Status

| Milestone | Status |
|---|---|
| M0: Spikes | Not started |
| M1: Spec compiler (`hubwagen compile`) | **Done** |
| M2: New engine in dev mode (`hubwagen dev`) | **Done** |
| M3: API, CLI, remote agents, dashboard as client | Next |
| M3b: Standalone profile + Docker/Podman tasks | — |
| M4: Failover on Kubernetes (Lease, Litestream, reconciliation) | — |
| M5: Kubernetes Jobs + tenants (namespaces, quotas, fair share, auth) | — |
| M6: Assets, partitions, backfills | — |
| M7: Observability (traces, metrics, log tailing) | — |
| M8: Retention/archive, hardening | — |

M3b can run in parallel with M4; M5 and M6 in parallel after M4.

## M0: Spikes

Throwaway experiments that de-risk later milestones. Most can run locally with
Podman and MinIO.

| | Question | Local? | Blocks |
|---|---|---|---|
| R1 | Can Litestream be embedded as a library and pointed at a new replica path per leader term? Restore time for 50 MB / 500 MB DBs with 1 h of WAL? Local-path replica for standalone? Warm-standby restore? | Yes (MinIO) | M3b, M4 |
| R2 | Does create-if-absent work through `gocloud.dev/blob` on S3 (MinIO, then real S3) and GCS? (`file://` is done natively in `internal/blob`) | Partly (real GCS/S3 later) | M3b multi-host, M4 |
| R3 | Does one leader sustain the load target with `modernc.org/sqlite` and group commit? | Yes, after the M2 core exists. First signal from `BenchmarkAppend` (M1 Mac, `synchronous=FULL`): ~15k events/s one per commit, ~60k/s in batches of 128 — far above the ~100 transitions/s target, but needs re-measuring on Linux server disks and with the real core | Load target sign-off |
| R4 | Can the `hubwagen` binary be injected into distroless, Alpine, Debian and non-root images (init-container copy and bind mount)? | Yes (Podman) | M3b, M5 |
| R5 | Does Podman's Docker-compatible socket (rootless too) cover create/wait/kill/inspect (OOM flag)/labels? | Yes | M3b |

## M2: New engine in dev mode

Goal: example pipelines run end-to-end with `hubwagen dev` — data intervals,
retries, timeouts, cancellation, idempotent triggers, exactly-once output
commits on `file://` — and the v1 engine is deleted. Planned as reviewable PRs:

1. ✅ **Store** (`internal/state`): `events` table + projections (tenants,
   pipeline versions, pipelines, schedule cursors, runs, task runs, attempts,
   outputs), `Append(term, events, apply)` in one transaction, term fencing
   (`StartTerm`, `ErrStaleTerm`), `LoadCatalog`/`LoadActive` for leader start,
   read queries on a separate read-only pool. Lives next to v1's
   `internal/store`, which is deleted in step 5. Pools, asset partitions and
   backfills get their tables in the milestones that need them.
2. ✅ **Core** (`internal/core`, with `internal/clock` and `internal/layout`):
   single-goroutine event loop with group commit; run, task and attempt state
   machines; per-task readiness and trigger rules; `on_failure`; retries with
   backoff; leader-side timeouts; cancellation; `on_overlap`; idempotent
   triggers and rerun generations; params; template rendering into
   assignments; restore on leader start (in-flight attempts lost unless a
   `Recovery` accounts for them). Execution semantics are documented in
   pipeline-format.md. Not yet: fair-share dispatch (ready tasks go straight
   to the runtime, which may queue), cron (step 4).
3. ✅ **Blob + task wrapper** (`internal/blob`, `internal/taskexec`):
   `file://` bucket with atomic `PutIfAbsent`; the task contract for host
   processes (environment incl. `_PATH` variables, process-group kill on
   timeout/cancel, log chunks + index, result file, task-level commit marker,
   superseded attempts); `taskexec.Recovery` checks markers on leader restart;
   dev-mode secrets from the environment.
4. ✅ **Local runtime + cron** (`internal/runtime/localrt`, `internal/core/cron.go`):
   in-process agent with slots for the configured pools (others fail fast with
   `no_runtime`), cancellation of queued and running attempts; cron with
   persisted cursors, catch-up (batched), `catchup: false`, pause/resume, and
   contiguous calendar intervals so DST skips/repeats can't lose or double an
   interval (the compiler's schedule check no longer rejects them either).
   End-to-end tests in `tests/engine_test.go`, including recovery of an
   attempt that committed just before the leader died.
5. ✅ **`hubwagen dev` and `hubwagen run`** (`internal/devmode`): one process
   owns a data directory (lock file); `dev` loads and watches pipeline files,
   runs schedules, and shows the dashboard (or `--headless` output); `run`
   runs one pipeline once (`--date`/`--interval`, `--param`, `--rerun`,
   Ctrl-C cancels) and exits non-zero unless it succeeded. The dashboard
   reads and acts through `tui.Source`. v1 (`internal/dag`, `engine`,
   `executor`, `scheduler`, `log`, `store`, `cmd/validate.go`) is deleted;
   `compile --check` replaces `validate`.

   Known limits until M3: other processes can't talk to a running
   `hubwagen dev` (no API), so `run` needs its own data directory while
   `dev` is running; container (`kind: job`) tasks fail with `no_runtime` in
   dev mode until M3b.

   Afterwards the v1 compatibility mapping, the v1 example DAGs and the
   `tui` alias were removed; v1alpha1 is the only input format, and examples
   live in `pipelines/examples/` (`dev` defaults to `pipelines/`).

## Later milestones (summary)

- **M3**: Connect API (protobuf, `buf`), `hubwagen publish`, `hubwagen agent`
  pulling work, dashboard on `WatchRuns`/`TailLogs` instead of polling SQLite.
  Instance config ([config.md](config.md)): `internal/config`, `hubwagen.yaml`
  with defaults < file < env < flags, `hubwagen config check|show`, and the
  sections the server and agents need (`data_dir`, `bucket`, `server`,
  `agent`, `log`).
- **M3b**: `standalone` profile — its config sections (auth tokens,
  replication, secrets) and `/etc/hubwagen/hubwagen.yaml`, systemd units, `flock`, static
  tokens, file/env secret providers, Docker/Podman launcher, S3-compatible
  store for multi-host, Litestream to a local path.
- **M4**: Lease election, terms and fencing, Litestream replication and
  restore, reconciliation; failover drills on kind + MinIO.
- **M5**: Job runtime via controller-runtime, tenant namespaces and quotas,
  weighted fair share, OIDC + service-account auth.
- **M6**: asset-triggered runs, lazy backfills, explicit reruns.
- **M7**: OpenTelemetry traces, Prometheus metrics, live log tailing.
- **M8**: history archive, disk-full and bucket-outage behaviour, docs.

## Open decisions

| Decision | Proposed default | Needed by |
|---|---|---|
| Load target for one leader | 100 task transitions/s sustained, 2,000 running tasks, 200k attempts/day | R3 / M2 |
| Human login on Kubernetes | OIDC tokens, tenant roles from a group claim | M5 |
| Warm-agent isolation | Several tasks of one tenant may share an agent container; agents never serve two tenants | M3 |
| Config env variables | Only listed settings get a `HUBWAGEN_*` variable (no generic field mapping); a test rejects names the task contract reserves | M3 |
| Format name | Keep `hubwagen.dev/v1alpha1` (changing it later changes every version hash) | Before first publish (M3) |
