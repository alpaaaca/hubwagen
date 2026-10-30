# Worker pools: the leader starts the workers its work needs

> **Status:** design note, not implemented. Builds on `hubwagen.yaml`'s
> `workers:` ([config.md](config.md)), `internal/supervise`, the core's
> record of workers seen lately (`internal/core/workers.go`) and the node
> design in [failover.md](failover.md). Decision requested: steps 1 and 2
> (below) first, with the launcher interface shaped so containers and
> Kubernetes fit later.

## Problem

A leader that starts with runs in its database waits for workers that
nothing starts. `hubwagen dev` supervises the config's `workers:`, but
`hubwagen server`, the production server, deliberately reads none of it:
workers come from systemd, a Deployment or `hubwagen workers`, configured
separately. So:

- **After a restart, queued runs wait** until someone starts workers. Since
  #96 the run at least says so ("no worker is connected"), but it doesn't
  fix it.
- **Capacity is fixed by hand.** Three workers run whether there is nothing
  to do or a backfill of 300 days. Nobody scales up for the backfill or
  down at night, and a dock with a GPU has a GPU worker idling all day.
- **Two places describe one thing.** Which docks a worker polls, how many
  steps it runs at once and how it is started live in a unit file or a
  manifest; the server learns them only when a worker polls.

The leader already knows what is missing: per queued haul task and lift,
`Core.Waiting` says whether no worker polls the dock, none has the code, or
all are busy, and `Core.Workers` lists who polls what. It should act on it.

## What stays the same

- **The leader never runs user code.** It starts processes, containers or
  pods that run it, as `hubwagen dev` already does. A step that runs out of
  memory kills its worker, never the leader.
- **Workers pull.** Started workers poll their docks like any other. Workers
  started by someone else still work: pools are how the leader adds
  capacity, not the only way work runs.
- **The code says what a pipeline does; the config says how to run its
  workers.** The leader can't build a Python environment or know which
  secrets a worker needs, so how to start one (a command, an image) stays
  in the config. What a step needs (memory, a GPU) belongs next to the step
  in code, versioned with the build.

## Shape: pools

A **pool** is the `workers:` group of today, with bounds instead of a fixed
count, declared docks and capacity, and a launcher:

```yaml
pools:                           # today: workers:
  - name: py
    launcher: process            # process | container | kubernetes (later)
    command: [python3, -m, hubwagen.worker, --lifts, "2"]
    dir: .
    env: {PYTHONPATH: src}
    docks: [default]             # what its workers poll; default [default]
    lifts: 2                     # steps one worker runs at once
    min: 1                       # always running; 0 = start on demand
    max: 6
    idle_after: 5m               # stop a worker above min idle this long

  - name: gpu
    launcher: kubernetes
    image: registry.acme/revenue-gpu:2026-10-30
    resources: {gpu: 1, memory: 16Gi}
    docks: [gpu]
    lifts: 1
    min: 0
    max: 2
```

- `replicas: n` stays valid and means `min: n, max: n`, so today's files
  keep working and mean the same.
- `docks` and `lifts` become what the pool's workers are started with
  (`--dock`, `--lifts`), so the config and the worker can't disagree. The
  leader needs them before any worker exists, to scale from zero.

## Who decides, who acts

```
  core (one goroutine)          scaler (leader only)            launcher
  queued work, workers seen ──► desired count per pool ───────► start / stop
         ▲                                                          │
         └──────────────── workers poll, register ◄─────────────────┘
```

- **The core** doesn't change its job: it queues, matches and remembers
  workers. The scaler reads it the way the API does (`Waiting`, `Workers`).
- **The scaler** runs on the leader, outside the core loop, every few
  seconds. It computes a desired count per pool and asks its launcher to
  get there. Scaling isn't scheduler state, so it writes no events, like
  leases: a new leader recomputes from what queues and what polls.
- **A launcher** starts and stops one pool's workers: `process`
  (`internal/supervise`, extended to a changing count), `container` (local
  Docker or Podman), `kubernetes` (a Deployment per pool). It reports which
  workers it started, by worker ID, so the scaler can match them with
  `Core.Workers`.

"Write before act" holds: the rule protects scheduler state. A started
worker changes none until it polls, and a stopped one finishes its running
steps first (SIGTERM, as dev does today) or loses them to the lease, as any
dead worker does.

## Scaling

Per pool, from the queued work on its docks that it can run:

- **Up**, when work waits with reason `no_worker` or `busy`:
  `desired = running + ceil(queued lifts / lifts)`, at least 1 while a haul
  task waits, at most `max`. One step at a time per interval, so a burst of
  300 queued runs doesn't start `max` workers before the first ones poll.
- **Down**, when a worker above `min` has polled without work for
  `idle_after`: stop that worker. The process and container launchers can
  stop a chosen worker; a Deployment can't choose (see open questions).
- **Never past `max`**, and `dock_limit` waits don't scale: the dock is the
  bottleneck, not workers.
- **Which pool**: those whose `docks` include the task's dock. With several,
  prefer a pool whose last registered build defines the haul or lift; if
  none has registered yet, start one worker of each, once, and let builds
  decide from then on.

### When the code changed

`no_build` means workers poll the dock but their code doesn't define the
pipeline or step. For a pool, that usually means its workers run older code
than the one that queued the work (a deploy updated the files or the image
after they started). The scaler restarts the pool's workers once, one at a
time, as each finishes its running steps. If the newly registered build
still doesn't define it, it stops trying and the run keeps saying why it
waits: restarting again wouldn't help.

### Scaling from zero

`StartHaul` already accepts only pipelines some registered build defines,
and builds stay in the database across restarts. So a pool with `min: 0`
that ran once can be started for queued work after any restart. A pipeline
no worker ever registered still needs one worker to run first, which
`min: 1` or a one-off `hubwagen workers` gives.

## Resources (later, with pod steps)

`@step(resources={"gpu": 1, "memory": "16Gi"})` registers what a step needs
with the build. The scaler sends such a step only to a pool whose
`resources` cover it, and the router (`canRun`) stops handing it to workers
that don't. Pod steps ([architecture.md](architecture.md), "Workers and
execution") are the extreme case: a pool of size 0 whose launcher runs one
container per lift attempt, sized by the step.

## Kubernetes: launch, or let the platform scale

Two ways, not exclusive:

- **The leader scales a Deployment** per pool through the scale
  subresource. It needs RBAC to patch Deployments in one namespace, and
  `profile: kubernetes` ([config.md](config.md)) is where that client
  lives, never the core.
- **The platform scales**: the leader publishes each pool's desired count
  (`GET /v1/pools`, or a KEDA external scaler) and KEDA or an HPA applies
  it. No cluster credentials in hubwagen; teams that already run KEDA keep
  one scaling system.

The second is less code and fits platform teams; the first works without
anything installed. Both read the same desired count.

## Failover

With nodes ([failover.md](failover.md)), every node supervises its
machine's pools, and only the leader runs the scaler. The scaler tells
nodes how many workers of each pool to run (a call standbys would
forward, like every other); `min` stays a per-node floor, so a standby's machine keeps
workers ready for when it leads. A node that dies takes its workers with
it, as today, and the next scaler pass starts replacements elsewhere.

## Visibility

`ListPools` next to `ListWorkers`: each pool's bounds, desired and running
count, its last build and the last scaling decision with its reason ("2
lifts queued on gpu, no worker"). The web UI shows it with the workers, and
a waiting run says "starting a worker in pool gpu" instead of "no worker is
connected" when one is on its way.

## Steps (PR-sized)

1. **`hubwagen server` supervises `workers:`**, as dev does, with
   `--no-workers` for setups that run them elsewhere. Fixed counts. This
   alone fixes "the leader starts and runs wait for workers nobody starts".
2. **Pools with the process launcher**: `min`, `max`, `docks`, `lifts`,
   `idle_after`; the scaler; `ListPools`; the waiting message names the
   pool. Tests with the fake clock and real subprocesses.
3. **Desired counts for external scalers**: `GET /v1/pools` and a KEDA
   external scaler.
4. **Container and Kubernetes launchers**, behind `internal/profile`.
5. **Step resources and pod steps.**

## Open questions

- **Stopping the right pod.** A Deployment removes pods it chooses, maybe
  a busy one. Options: `controller.kubernetes.io/pod-deletion-cost` set
  from the leader's view of idle workers, one Job per worker, or let busy
  workers finish their steps under a long `terminationGracePeriodSeconds`.
- **Should haul tasks get their own small pool?** They are short replays;
  a pool of lift workers all busy with hour-long steps delays every
  replay. Today a worker's haul thread keeps polling while its lifts run,
  which may be enough.
- **Cost limits across pools** (at most N workers in all), per tenant once
  tenants exist.
- **Where `pools:` lives** when several projects share one server: one
  file per server, or each project's file contributing its pools.
