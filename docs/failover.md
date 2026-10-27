# Failover: nodes that supervise workers and can become leader

> **Status:** design note, not implemented. Builds on `hubwagen.yaml` and the
> worker supervisor (`internal/config`, `internal/supervise`). Decision
> requested: option A first (below), built so that option B stays possible.

## Problem

Today there is one leader and no standby. If it dies, nothing schedules,
dispatches or fires timers until someone restarts it on the same data
directory. A restart loses nothing durable: the event log is the truth, and
restore rebuilds hauls, lifts, timers and flights from it. But the outage
lasts as long as the leader is down, and a lost disk loses everything.

We want: when the leader's machine dies, another machine takes over within
seconds, with the same state, and the workers carry on against it.

## Shape: nodes

Every machine runs one **node**: the `hubwagen` binary in a new mode
(`hubwagen node`). A node does two things:

1. **Supervises the workers on its machine**, as `hubwagen dev` does today:
   it starts the `workers:` from `hubwagen.yaml`, prefixes their output,
   restarts them, and stops them on shutdown.
2. **Is a leader candidate.** Exactly one node is the leader and runs the
   core and the API. The others are standbys, ready to take over.

```
   machine 1                 machine 2                 machine 3
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ node a  LEADER   │◄────┤ node b  standby  │     │ node c  standby  │
│  core, SQLite    │◄────┼──────────────────┼─────┤                  │
│  ┌─py-1┐ ┌─py-2┐ │     │  ┌─py-1┐ ┌─py-2┐ │     │  ┌─py-1┐ ┌─py-2┐ │
└──────────────────┘     └──────────────────┘     └──────────────────┘
   workers talk to their own node; standbys forward to the leader
```

What stays the same:

- **The leader never runs user code.** Workers are still separate Python
  processes; the node only starts them. A lift that runs out of memory kills
  a Python child, not the leader. Leader candidates are Go nodes, never
  workers, so no Go SDK is needed for any of this.
- **One writer.** Only the leader's core writes state. Failover changes which
  node that is, not how many there are.
- **Workers outlive leaders.** A lift attempt that a worker took stays valid
  under the new leader; a haul task is replaced.

What's new:

- **Workers talk to their local node** (`HUBWAGEN_SERVER` is
  `http://127.0.0.1:<node port>`). A standby forwards worker and client calls
  to the leader. Workers need no membership list and never have to find the
  leader. If the local node dies, its workers stop too (they exit when
  their supervising node is gone).
- **Every reply carries the leader's term.** Workers remember the highest
  term they've seen and drop work from a lower one. Today `StartTerm` fences
  stale writers only within one SQLite file. Two leaders with separate copies
  of the database don't fence each other, so the term has to reach the
  workers and the bucket.
- **`hubwagen.yaml` lists the nodes:**

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Config

bucket: s3://acme-hubwagen          # pallets, plus the lease and replicas (option A)
nodes:
  - {name: a, listen: 10.0.0.1:7070, data_dir: /var/lib/hubwagen}
  - {name: b, listen: 10.0.0.2:7070, data_dir: /var/lib/hubwagen}
  - {name: c, listen: 10.0.0.3:7070, data_dir: /var/lib/hubwagen}
failover:
  lease_ttl: 15s

workers:                            # each node starts these on its machine
  - name: py
    replicas: 2
    command: [python3, -m, hubwagen.worker, app.hauls]
```

Each machine runs `hubwagen node --name a` (b, c) under systemd. One file
for the whole cluster; `--name` picks the node's own entry.

Picking a leader is the easy part. **The hard part is that the leader's state
is a SQLite file on its own disk.** A new leader must start from the old
one's state, or it reruns or forgets work. There are two honest ways to get
it there.

## Option A: lease in the bucket, Litestream replication

**Election.** The leader holds a lease object in the bucket,
`_hubwagen/leader.json` = `{node, term, renewed}`, and renews it every
`lease_ttl / 3` with a conditional write: GCS generation match, S3
`If-Match`, or a rename under a lock for `file://`. When a standby sees the
lease unchanged for a full `lease_ttl` (by its own monotonic clock, never by
comparing timestamps across machines), it writes a new lease with `term + 1`,
again conditionally. Only one standby's write can succeed. The term comes from
the lease, as `StartTerm(term)` already expects.

**Self-fencing.** A leader that can't renew stops itself before its lease
could have expired anywhere else: it stops accepting and dispatching, and
stops writing, at `lease_ttl − margin` after its last successful renewal.
This depends on clocks running at roughly the same *rate* (not on them being
in sync), which is the usual assumption for leases.

**State handoff.** The leader runs Litestream as a library. It copies the
SQLite WAL to the bucket under a replica path per term
(`_hubwagen/replica/<term>/`), so a deposed leader still uploading can't
overwrite what the new one writes. A new leader:

1. takes the lease (term N+1);
2. restores the database from the term-N replica (snapshot + WAL);
3. runs `StartTerm(N+1)` and the existing restore, then serves.

Standbys can keep restoring in the background to make step 2 short.

**What it costs: the tail.** Litestream copies asynchronously (every second
by default). If the leader's machine dies, the last second or so of events is
gone. That weakens *write before act*: an action can have happened whose
event was lost. Case by case:

| Lost event | Effect after failover | Severity |
|---|---|---|
| `HaulStarted` (client got the ID) | haul ID unknown; the client's wait fails | the caller starts it again (an idempotency key on `StartHaul` would make that safe; there is none yet) |
| `HaulTaskCompleted`, timers fired | replayed or fired again | harmless: replay is deterministic |
| `LiftAttemptDispatched` | the worker's result is rejected (`not_found`); the lift is dispatched again | **runs twice** |
| `LiftFinished`, `PalletRecorded` | the lift runs again and records a new pallet; the old manifest stays in the bucket, orphaned | **runs twice**; extra storage |
| `HaulFinished` | a completed haul goes back to running and completes again | a status that flips back |

Lifts already have to tolerate running twice: retries, lost leases and
crashed workers do the same thing today. But a failover would add
duplicate runs that nothing failed to cause. Two mitigations reduce the damage:

- **Reconcile from the bucket.** Manifests are written synchronously, with
  `PutIfAbsent`, before `PalletRecorded`. On takeover, the new leader lists
  manifests newer than its restored state and records them again. Lifts
  whose pallet made it to the bucket are then not rerun.
- **Shorter sync interval** for the replica. This narrows the window but
  never closes it.

**Failover time:** `lease_ttl` (15 s) + restore (S4 measures it; with
standbys restoring continuously, seconds).

**Kubernetes variant:** the lease can be a Kubernetes `Lease` object instead
of a bucket object (the roadmap's `k8slease` elector). Everything else is the
same.

## Option B: embedded Raft

**Election and replication together.** The nodes run Raft
(`hashicorp/raft`: pure Go, with `raft-boltdb` or a WAL on disk, no CGO).
The leader turns each batch of events into a Raft log entry. The batch commits
only once a majority of nodes has it on disk; then every node applies it to
its own SQLite copy. The Raft term is the store's term.

**What it gives:**

- **No data loss** on failover: every committed event is on a majority of
  disks. *Write before act* holds exactly as it does today.
- **No split brain** by construction: a leader that can't reach a majority
  can't commit, so it can't act.
- **Fast failover:** every standby already has the state, so taking over is
  an election (1–3 s) plus the in-memory restore. Nothing to download.

**What it costs:**

- **Three nodes to survive one failure.** With two nodes, losing either
  stops everything, which is worse than option A. A laptop or a single server
  gets nothing from Raft.
- **Write latency:** a network round trip plus an fsync on a majority, per
  batch. Group commit amortizes it, but the S3 load target has to be measured
  again.
- **Core changes:** group commit becomes "propose, wait for commit, apply".
  The core's rule that side effects run after the batch commits maps onto
  this directly, but the store becomes a Raft state machine (apply, snapshot
  via `VACUUM INTO`, restore from snapshot).
- **Operations:** membership changes (adding, replacing a node), quorum loss
  recovery, snapshots, upgrades of a replicated log format. It is a
  distributed database now, and it needs tests to match (partitions, clock
  jumps, slow disks).
- Litestream becomes optional: a backup, not the failover path.

## Comparison

| | A: bucket lease + Litestream | B: embedded Raft |
|---|---|---|
| Nodes for failover | 2 or more | 3 or more (odd) |
| Data lost on failover | the last ~1 s of events (some lifts run twice) | none |
| Split brain | prevented by the lease and self-fencing (clock rate) | impossible (quorum) |
| Failover time | lease TTL + restore: ~15–30 s | election: ~1–3 s |
| Commit latency | unchanged (local SQLite) | + network round trip and remote fsync |
| Change to the core | small: elector, replicator, term on replies | large: the store becomes a Raft state machine |
| New dependencies | Litestream (library) | `hashicorp/raft` + a log store |
| Disaster recovery (all nodes lost) | yes: restore from the bucket | only with a separate backup |
| Fits the roadmap | yes: `Replicator`, `Elector`, S4 are already planned | new direction |

## Simulating it on a laptop

The same file, with three nodes on one machine, a shared `file://` bucket and
two workers each:

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Config

bucket: file://.hubwagen/bucket
nodes:
  - {name: a, listen: 127.0.0.1:7071, data_dir: .hubwagen/a}
  - {name: b, listen: 127.0.0.1:7072, data_dir: .hubwagen/b}
  - {name: c, listen: 127.0.0.1:7073, data_dir: .hubwagen/c}
failover:
  lease_ttl: 5s                     # short, to watch it happen

workers:
  - name: py
    replicas: 2
    command: [python3, -m, hubwagen.worker, revenue.hauls]
```

```bash
hubwagen dev --cluster              # starts nodes a, b, c as processes; a wins the lease
hubwagen nodes                      # who leads, which term, standby lag
hubwagen replay daily --param day=2026-10-01..2026-10-07 --server http://127.0.0.1:7072
kill -9 <pid of the leader>         # or: hubwagen dev --cluster --chaos 30s
```

What to watch:

- **Takeover:** after ~5 s a standby takes the lease (term +1), restores and
  serves. Its workers never saw an outage, because their node forwarded
  calls throughout. They just see the new term.
- **The dead node's workers** exit with it. `dev --cluster` restarts the
  node, which comes back as a standby with its workers.
- **The replay** keeps going, apart from errors on calls that were in
  flight, which the client retries.
- **Option A:** lifts that were dispatched in the last second can run
  twice. `hubwagen nodes` reports the events the new leader didn't get and
  what reconciliation recovered.
- **Option B:** the same file without the lease. Kill any one node and
  nothing is lost; kill two and the cluster stops, but stays correct.

For a test that runs in CI, `tests/` can do the same in-process: three nodes
over a temp-dir bucket and a fake clock, with partitions done by refusing
calls.

## Recommendation

**Build option A first, and keep option B possible.**

- It's what the roadmap already plans (`Replicator`, `Elector`, S4), and
  it's useful from two nodes up, including the common case of one server and
  one standby.
- The bucket replica is also the disaster-recovery story, which option B
  doesn't give.
- Duplicate lift runs are a failure mode we already design for (lifts are
  retried and should be idempotent). Reconciling manifests from the bucket
  removes the costly case: a finished lift whose pallet exists.

**Move to option B** when a deployment can't accept even rare duplicate runs
(lifts that are expensive or not idempotent at all) or needs failover under
5 s. The pieces we build for A carry over:

- the node and its supervisor,
- forwarding from standbys,
- the term on every reply,
- `hubwagen nodes`.

Only the elector and the store's commit path change.

**Don't** let workers elect the leader, and don't merge workers into the
leader process. Both would put user code next to the one writer.

## Steps (PR-sized)

1. **Nodes:** `hubwagen node --name`, `nodes:` in the config, and the
   supervisor moved out of `dev`. Workers exit when their node is gone, and
   lift results are retried until delivered (both needed anyway).
2. **Terms reach workers:** the term goes on every API reply; the SDK drops
   lower terms; the core rejects results from attempts of a term it no longer
   honours.
3. **Forwarding:** a standby proxies worker and client calls to the leader
   (and answers `unavailable` while there is none).
4. **Elector interface** with `single` (today) and `bucketlease`; a
   conditional-write primitive on `Blob` (`PutIfMatch`); self-fencing.
5. **Replicator** with Litestream, using a replica path per term (spike S4
   first); restore-on-takeover; standbys restoring continuously.
6. **Reconcile from the bucket** on takeover (manifests newer than the
   restored state).
7. **`hubwagen dev --cluster`, `hubwagen nodes`**, `--chaos`; an end-to-end
   failover test in `tests/`.

## Open questions

- **Lease storage on `file://`:** a rename under `flock` works on one
  machine. Across machines it needs a shared filesystem we shouldn't
  recommend. Is a bucket-backed lease on `file://` a laptop-only feature?
- **S3 conditional writes:** `If-Match` on PUT is recent. Do we require it,
  or fall back to a DynamoDB or Kubernetes lease?
- **Who may lead:** every node, or a `leader: false` flag for worker-only
  machines (no local state, smaller disks)?
- **Client and worker addresses:** forwarding through any node is simple.
  Is the extra hop for workers acceptable at the S3 load target, or should
  the SDK follow a leader hint instead?
- **Raft later:** if we go to B, one Raft group for everything, or one per
  shard, as in the sharding direction in [architecture.md](architecture.md)?
