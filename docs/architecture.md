# Architecture

Where hubwagen is going and why. This replaces the spec-kit documents that
used to live under `specs/` (now ignored, kept only locally). What is built and
what is next is tracked in [roadmap.md](roadmap.md).

## Goals

A retrospective comparing hubwagen with Airflow and other Python DAG tools set
seven requirements:

1. **Static specs, never code.** Teams author YAML; CI compiles it into an
   immutable, versioned spec. The scheduler only reads data.
2. **Event-driven scheduler.** One active leader, an in-memory state machine
   per run, driven by events (task finished, asset updated, cron tick). No
   polling a database for work.
3. **Tasks are containers with a contract.** Image + command + args + declared
   inputs/outputs. The platform has no language dependency.
4. **Kubernetes-native, without etcd for history.** Informers instead of
   polling; run history in hubwagen's own store.
5. **Multi-tenancy.** Weighted fair share and per-tenant limits, so one team's
   backfill can't starve the others.
6. **Data-aware and idempotent.** Explicit data intervals, idempotency keys,
   asset/partition triggers, outputs passed as object-storage references.
7. **Observability.** A trace per run with a span per task attempt, logs in
   object storage, Prometheus metrics labelled per tenant.

And one constraint carried over from v1: **it still works as a single binary on
a laptop or a plain server**, with no database server and no message broker.

## Deployment profiles

One binary, three profiles. A compiled spec means the same thing in all of
them; only server configuration differs.

| | `dev` | `standalone` | `kubernetes` |
|---|---|---|---|
| Where | Laptop | One or more plain Linux servers (systemd) | Cluster |
| Leader | One process | One process, `flock` on the state dir | Kubernetes Lease, warm standby |
| Failover | — | Restart by systemd; restore from backup if the disk is lost | Standby restores and takes over |
| State backup | none | Litestream to a second disk, or to a bucket | Litestream to the bucket |
| Storage for logs/outputs | local directory | local directory (one host) or S3-compatible store (several hosts) | GCS / S3 |
| `agent` tasks | host processes | host processes or a pool image, on embedded or remote agents | agent pool Deployments |
| `job` tasks | Docker/Podman | Docker/Podman on container-capable agents | Kubernetes Jobs |
| Secrets | env / files | files / env | Kubernetes Secrets |
| Login | none (127.0.0.1 only) | static tokens, optional OIDC | OIDC + service-account tokens |

## Components

```
   CI: hubwagen compile → canonical spec JSON → hubwagen publish
                                                   │
                          ┌────────────────────────▼─────────────────┐
 cron ticks, asset ─────▶ │ Leader (hubwagen server)                  │
 events, API calls        │  one core goroutine owns all state        │──▶ SQLite (WAL)
                          │  append events → act                      │      │ Litestream
                          │  fair-share ready queues                  │      ▼
                          └──────┬──────────────────────▲─────────────┘   bucket: state/term-N/
                    dispatch     │                      │ results, heartbeats
              ┌──────────────────┴───┐          ┌───────┴──────────────┐
              │ Kubernetes Jobs      │          │ Agents (warm, N)     │
              │ (or Docker/Podman)   │          │ pull work over       │
              └──────────┬───────────┘          │ Connect              │
                         │                      └──────────┬───────────┘
                         └──── hubwagen exec (task wrapper) ┘
                               logs, outputs, commit markers ──▶ bucket
```

- **Leader**: the only process that writes state. It never runs user code.
- **Agents**: stateless, long-lived workers that pull tasks for one tenant's
  pool, run them, send heartbeats and report results. They hold no
  authoritative state and need no database credentials.
- **`hubwagen exec`**: wraps every task in every runtime. It sets the contract
  environment, runs the command, ships logs, and commits outputs. See
  [task-contract.md](task-contract.md).
- **Standby leaders** (Kubernetes only): wait for the Lease, keep a warm copy
  of the replica, and take over on failure.

## Key decisions

### Storage: SQLite + Litestream, single writer

The leader is the only writer, so a multi-writer database adds nothing for
writes. SQLite in WAL mode on local disk gives microsecond writes and no
server to run; Litestream copies it continuously to object storage (or a
second disk). Accepted costs: failover is a cold restore (kept short by a
small database, frequent snapshots and a warm standby), up to ~1 s of writes
can be lost on a crash (repaired by reconciliation and commit markers), and no
direct SQL access for other tools.

Postgres stays possible later behind the same `Store` interface. The core must
not use Postgres-only features (`SKIP LOCKED`, `LISTEN/NOTIFY`, advisory
locks). Rejected: NATS JetStream (poor history queries), embedded Raft (too
much complexity this early), a custom log (writing a storage engine).

### Event log + projections

Every state change is an event appended to an `events` table. In the same
transaction the leader updates projection tables (`runs`, `task_runs`,
`attempts`, `outputs`, `asset_partitions`, …) that the API queries. Events give
audit, live subscriptions and deterministic tests; projections give fast
queries. Events are group-committed (a few ms window), and **side effects only
happen after their events commit** — v1's write-before-act rule, kept.

Heartbeats are not persisted. Terminal runs past the retention window (default
90 days) are archived to the bucket as JSON and deleted from SQLite.

### The core: one goroutine, deterministic

All scheduling state lives in one goroutine fed by channels (API commands,
agent messages, informer events, timers). It validates input, emits events,
commits, applies them, then performs side effects. With a fake clock it is a
pure state machine. A task becomes ready as soon as **its own** dependencies
satisfy its trigger rule — v1's wave barrier goes away.

### Leader election, terms and fencing

Kubernetes Lease via controller-runtime. The **term** is the Lease's
`leaseTransitions` at acquisition. Litestream replicates to
`state/term-<N>/`, so a deposed leader can only write to a path nobody
restores from again. Agents and Jobs carry the term and reject commands from
older terms. A leader that can't renew its lease exits immediately.

Failover: lease expires → standby acquires term N → restores from the newest
`term-<M>` (M < N) → loads non-terminal runs → reconciles (labelled Jobs,
reconnecting agents, commit markers) → resumes. Target: ≤ 60 s (p95).

### Dispatch: agents pull over Connect

Agents hold a bidirectional Connect stream to the leader, announce pool, tenant
and free slots, and receive assignments. The leader picks tasks from in-memory
queues with full fair-share knowledge — no database round trip, no agent
discovery, works across NAT. Kubernetes Jobs are created by the leader and
observed through informers; Job pods never talk to the leader.

### Fair share

Across tenants: weighted deficit round robin, with per-tenant
`max_running_tasks` and per-pool slots as hard limits. Within a tenant:
`interactive` > `scheduled` > `backfill`, with aging. Backfills are generators
that materialise a bounded number of runs at a time.

### Idempotency and exactly-once commits

- **Run key**: `tenant/pipeline/<interval-start>_<interval-end>` (scheduled,
  backfill), `…/asset:<asset>@<partition>` (asset trigger), or caller-supplied
  (manual). Unique per `(run_key, generation)`; re-running an interval creates
  generation `g+1`.
- **Commits**: each attempt writes to its own staging prefix. On exit 0 the
  wrapper creates one task-level marker
  `commits/<tenant>/<pipeline>/<task>/<run-key>/gen-<g>.json` listing all
  outputs, with a create-if-absent write (GCS `ifGenerationMatch=0`, S3
  `If-None-Match: *`, write-then-`link(2)` locally). One marker per task, not
  per output, so racing attempts can't each win a different output. A losing
  attempt is recorded as `superseded` and the task uses the winner's outputs.
  On leader restart the same markers tell recovery which in-flight attempts
  actually finished.

Execution is at-least-once; visible output is exactly-once, and committing
doesn't need the leader to be up.

### Specs: compiled, canonical, content-addressed (built)

YAML (`apiVersion: hubwagen.dev/v1alpha1`) is compiled by `internal/spec` into
a normalised `Spec` serialised as RFC 8785 canonical JSON; the version ID is its
SHA-256. Image tags are resolved to digests at compile time. Templates are
restricted to an allow-list checked on the parse tree. v1 DAG files compile
through a compatibility mapping, marked for dev mode only. Protobuf is used for
the API only — specs travel as their canonical bytes so the hash never depends
on a protobuf JSON mapping. Format reference: [pipeline-format.md](pipeline-format.md).

### Observability

Trace ID derived from the run ID, span IDs from attempt IDs; spans emitted when
attempts finish with recorded timestamps, so failovers don't break traces.
`TRACEPARENT` is passed to tasks. Logs go from the wrapper straight to the
bucket in chunks (every 2 s or 1 MiB). Metrics: queue depth and scheduling lag
per tenant/pool, attempt outcomes, leader term, commit latency, replication lag,
failover restore time.

### Authentication

Kubernetes: OIDC for humans (tenant roles from a group claim), service-account
tokens for agents. Standalone: hashed static tokens in a file, optional OIDC;
agent tokens bound to one tenant and its pools. Dev: none, localhost only.
Roles per tenant: `viewer`, `operator`, `admin`.

## Internal interfaces

These seams keep the core free of Kubernetes and storage specifics and let it
be tested without either. Tests use real implementations (`:memory:` SQLite,
`file://` blob, in-process agent), not mocks.

| Interface | Implementations |
|---|---|
| `Store` (events + projections) | `internal/state` (SQLite). A concrete type for now; the core defines the interface it consumes, and a Postgres implementation would satisfy the same one |
| `Replicator` | `litestream` (bucket or local path), `snapshot` (fallback), `noop` |
| `Elector` | `k8slease`, `single` |
| `Blob` (incl. `PutIfAbsent`) | `internal/blob`: `file://` today (native, `link(2)` for create-if-absent); `gs://`/`s3://` later, possibly via `gocloud.dev/blob` (spike R2) |
| `Runtime` (leader side) | `agentrt`, `jobrt`, `localrt` |
| `Launcher` (agent side) | `process`, `container` (Docker Engine API; Podman via its compatible socket) |
| `SecretProvider` | `k8s`, `file`, `env` |
| `Clock` | real, fake |

`internal/profile` is the only package that knows which profile is active.

## State machines

- **Run**: `queued → running → succeeded | failed | cancelled`
- **Task run**: `pending → ready → dispatched → running → succeeded | failed | cancelled`;
  `retry_wait` between attempts; `skipped` / `upstream_failed` when the trigger
  rule isn't met.
- **Attempt**: `dispatched → running → succeeded | failed | lost | cancelled | superseded`

## Bucket layout

```
state/term-<N>/…                                              Litestream replica
specs/<tenant>/<pipeline>/<hash>.json                         compiled versions
logs/<tenant>/<pipeline>/<run>/<task>/<attempt>/<seq>.log
data/<tenant>/<pipeline>/<task>/<output>/<run-key>/gen-<g>/attempt-<n>/…   staging
data/…/gen-<g>/_COMMITTED                                     convenience marker
commits/<tenant>/<pipeline>/<task>/<run-key>/gen-<g>.json     the commit: winning attempt + all outputs
results/<tenant>/<run>/<task>/<attempt>.json
archive/<tenant>/runs/<yyyy>/<mm>/<run>.json
```

Each tenant's credentials are scoped to its own `<tenant>` prefixes.
