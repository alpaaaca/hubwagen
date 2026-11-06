# Projects: `moksha deploy` instead of hand-run workers

> **Status:** design note; steps 1–5 are built (the dry run, deployments,
> registries and secrets, `moksha dev` on deploy, no `moksha workers`);
> 6–7 are not, and are next (roadmap: M6). Builds on worker pools
> ([worker-pools.md](worker-pools.md), steps 1–4 built), container images
> ([deploy.md](deploy.md)) and builds (`RegisterBuild`). Decided
> (2026-11-06): one worker, and pools declared in one place, the project
> file ([below](#one-worker-pools-in-one-place)).

## Problem

Before projects, one `moksha.yaml` mixed two things:

- **The instance**: where the server keeps its data, its bucket, what it
  listens on. That belongs to whoever runs the server.
- **An application**: a repo's pipelines and how to run their workers
  (`workers:`: a command, a directory, a `PYTHONPATH`). That belongs to
  the repo.

So `moksha dev --config moksha.yaml` in a project starts
a server *and* worker processes from the repo's file, and a production
server has to be told about every project's workers in its own config,
by hand. There is no step where a repo says "here is my application, run
it", and nothing that shows what a change will rerun before it runs.

## Shape

A repo describes itself in a **project file**; `moksha deploy` hands it
to a server; the server runs and scales its workers.

```
  repo (or CI)                              server
  ┌──────────────────────────┐   Deploy    ┌───────────────────────────────┐
  │ moksha.yaml (Project)  │ ──────────► │ records the deployment (event)│
  │ Dockerfile, code         │             │ launcher starts its pools     │
  │ moksha deploy:         │             │ scaler sizes them from work   │
  │  build, diff, push image │             │ old version drains            │
  └──────────────────────────┘             └───────────────────────────────┘
           │ push                                    │ pull
           └──────────────► registry ◄───────────────┘
```

### The project file

The project file is the repo's `moksha.yaml`, with `kind: Project`
(the server's file stays `kind: Config`); `moksha dev` and `moksha
deploy` both read it. A repo whose `moksha.yaml` is still a server
config keeps its project in `moksha.project.yaml`; both take whichever
of the two is `kind: Project`. It is not the pipelines: those
are code, found the way they are today, from the modules listed in
`pyproject.toml` (`[tool.moksha] modules`). The project file says how to
build and run that code:

```yaml
apiVersion: moksha.dev/v1alpha1
kind: Project
name: revenue

build:
  dockerfile: Dockerfile             # FROM ghcr.io/alpaaaca/moksha-worker + the code
  image: ghcr.io/acme/revenue        # pushed as :<version>; a local server needs no push
  args: {WORKER_IMAGE: moksha-worker:dev}   # --build-arg, optional

pools:
  - name: default                    # the project's default queue
    min: 1                           # always running: a fixed baseline
    max: 6                           # scaled up from what waits, down after idle_after
    steps: 2
  - name: gpu
    queues: [gpu]
    min: 0
    max: 2
    steps: 1
    resources: {gpu: 1, memory: 16Gi}   # with pod steps (worker-pools step 5)

env:                                 # names and where values come from, never values
  DB_URL: {secret: revenue-db}       # a secret the server's launcher knows
  LOG_LEVEL: info                    # plain values are fine
```

- **Fixed and scaled in one**: a pool's `min` workers always run, and the
  server adds up to `max` from the work waiting; `min: max` is a fixed
  count. This is the pool of worker-pools step 2, now declared by the
  project instead of the server.
- **Why YAML next to `pyproject.toml`**, not a `[tool.moksha]` table:
  workers may not be Python forever, and the server's config is YAML too.
  The modules stay in `pyproject.toml`, where Python tools expect them.
- **No secrets** in the file: `{secret: name}` names a secret the server's
  side holds (a Kubernetes Secret, a file under the server's secrets
  directory); the value never passes through the CLI.

### `moksha deploy`

Run in the repo, by a person or by CI:

1. **Read** `moksha.yaml` and `pyproject.toml`.
2. **Build** the image (`docker` or `podman build`), tagged with the
   deployment's version.
3. **Work out the build**: run the worker's `--versions` in the image
   (on the CLI's machine: the server never runs project code), which
   gives the build ID and each pipeline's and step's code version.
4. **Show what changes** against what the server runs now:

   ```
   revenue: v3 → v4 (build b-8f2c… → b-1a9e…)
     daily_revenue    code changed (py-41a → py-77c): runs again for new runs
     revenue_report   unchanged
     extract_orders   unchanged
     pipelines: + weekly (new)
   ```

   `--dry-run` stops here.
5. **Push** the image to the registry, unless the server shares the
   local image store (a server on this machine with the container
   launcher).
6. **Deploy**: call `DeployService.Deploy(project, version, image digest,
   build, pools, env)`. The server records it as an event before acting
   (write before act); a deployment is state, unlike scaling.
7. **The server rolls out**: the new version's pools start (at `min`, then
   scaled from waiting work); the old version's workers stop polling,
   finish their running steps and go. Runs in flight carry on: their next
   replay runs on the new code, as today when workers restart.

### Builds go through a registry

Yes, a registry, for any server that isn't on the deploying machine:

- **The CLI builds and pushes; the server only pulls.** The server never
  runs project code, building included, so the build happens where the
  code is: a laptop or CI. The server's launcher pulls by digest, so what
  runs is exactly what was diffed.
- **Credentials stay apart.** The deployer needs push rights (`docker
  login`, or CI's token); the server's launcher needs pull rights, set
  up once on the server's side (a Kubernetes `imagePullSecret`, a
  `docker login` on the host). The CLI never hands credentials to the
  server.
- **Local needs none**: with a server on the same machine and the
  container launcher, the image stays in the local store; with the
  process launcher (below), there is no image at all.

### Other commands

- `moksha projects`: each project, its current version, when and by
  whom it was deployed, its pools (running/min/max).
- `moksha delete revenue`: stops the project's workers (draining, as on
  a new version) and records the project deleted. Its pipelines stop
  accepting new runs; its history, data versions and catalog stay. Runs
  still open are refused unless `--cancel-runs`.
- The web UI gets a projects page: versions, what each deploy changed,
  pools and their last scaling decision.

### `moksha dev` becomes a deploy

`moksha dev` in a repo starts a server in the process (as now) and
deploys the current directory to it with the **process launcher**: no
image, the workers run the code in place, in the project's environment.
It redeploys when the code changes. The repo's file is the project file;
nobody writes `workers:` any more.

### The server's config

Only the instance: data, bucket, listen address, and how to launch
(`launcher: container | kubernetes | process`, with its settings: the
runtime, the namespace, pull secrets, the secrets directory). A
project's pools come from its deployment. The section for pools the
server doesn't start (`launcher: external`, for KEDA) is renamed from
`workers:` to `pools:`, and is the only kind left there.

### One worker, pools in one place

Today three things are called a worker, and pools are declared in two
places:

| Today | What it is |
|---|---|
| `python3 -m moksha.worker` | the worker: one process that pulls work and runs pipelines and steps; the worker image's `CMD`, so what a Kubernetes pod runs |
| `moksha workers` | a supervisor that starts the config's `workers:` without a server, for workers on another machine |
| `workers:` in the server's `moksha.yaml` | pools, declared by the instance |
| `pools:` in a `kind: Project` file | pools, declared by the project |

Decided: a project has pools, a pool runs workers, and moksha starts and
scales them, as processes on one machine or as pods on Kubernetes.

- **One worker**: `python -m moksha.worker` is the only thing called a
  worker.
- **Pools come from the project file.** `moksha dev` deploys the
  current directory (step 4), so nobody writes `workers:` for their own
  code; `moksha deploy` does the same against a server.
- **No `moksha workers`.** Kubernetes doesn't use it (pods run the
  worker directly) and one machine won't need it once dev deploys. What
  goes with it: restarting the server while its workers keep running.
  Deployed workers stop and start with the server; on Kubernetes the
  pods keep running anyway. Workers someone else runs elsewhere still
  work (`python -m moksha.worker` with `MOKSHA_SERVER`); the docs no
  longer lead with them.
- **The server's config says `pools:`**, and holds only pools the server
  doesn't start (`launcher: external`). It is v0.1: the rename breaks
  configs now, with an error that says what to write instead.

### What it builds on

| Exists | Becomes |
|---|---|
| Pools with `min`/`max`, scaler, `ListPools` | what a deployment's pools are |
| Container and Kubernetes launchers | how a deployment's workers start; for Kubernetes the server creates and updates the Deployment itself, so the Helm chart only installs the server (and RBAC to manage Deployments in its namespace) |
| `RegisterBuild`, code versions, "why it ran" | the diff that `deploy` shows before anything changes |
| Restarting workers on `no_build` (worker-pools, not built) | the rollout: old version drains, new one starts |

## Steps (PR-sized)

1. ✅ **Project file and `moksha deploy --dry-run`**: `kind: Project`
   (strict, `${…}` variables, every problem at once); the build worked out
   in the project's image (`<image>:dry-run`, with `build.args`) or in
   place (`--local`, or without a Dockerfile); `ListCode`, the code the
   server runs (each step's code version and each pipeline, from the
   newest build that defines it); the diff in words. No deployment yet:
   useful on its own in CI ("this PR reruns daily_revenue"). Pipelines
   are compared by name only: builds don't record a pipeline's code
   version.
2. ✅ **Deployments on the server**: `DeployService` (`Deploy`,
   `ListProjects`, `DeleteProject`), events (`ProjectDeployed`,
   `ProjectDeleted`) and a `projects` projection; versions v1, v2, … per
   project. A deploy registers the project's build in the same batch
   (`python -m moksha.worker --build-json`, run on the deploying side),
   so its pipelines are accepted and its schedules apply before any
   worker runs. The server turns each live project's current version
   into pools named `<project>-<pool>-v<version>` (containers of its
   image, or processes in its directory); a new version replaces the
   old one's pools, whose workers get SIGTERM and finish what they run.
   `moksha deploy`, `projects`, `delete [--cancel-runs]`. Limits: an
   in-place deployment needs the server on the deploying machine, one
   project per pipeline name, and a deleted project's schedules still
   fire (each start is refused and recorded) until it is deployed again.
3. ✅ **Registries and secrets**: to a server elsewhere (or with
   `--push`), `deploy` pushes the image with the deploying machine's
   login and deploys it by digest; the server pulls with its own
   credentials (`projects.auth_file`: Podman `--authfile`, Docker
   `DOCKER_CONFIG`). `{secret: name}` env travels as the name; the server
   checks it has the secret at deploy (`projects.secrets_dir`, a file per
   secret) and reads the value only when a worker starts. Kubernetes
   Secrets come with step 6.
4. ✅ **`moksha dev` on deploy**: in a directory with a project file
   (or `--config` naming one), dev starts the server, its data in
   `.moksha` next to the file, and deploys the directory in place (the
   process launcher, `--python`). It looks at the Python files,
   `pyproject.toml` and the project file every second and deploys again
   once a change has settled, saying which steps run again; a deploy that
   fails (a syntax error) is printed and the next change tries again. A
   dev started on code the server already runs (same build, directory,
   pools and secrets) deploys nothing. `--no-workers` deploys nothing. The
   test project (`sdk/python/tests/projects/revenue`) has a project file;
   moksha-demo follows in M6 step 4.
5. ✅ **No `moksha workers`; `workers:` becomes `pools:`**: the command is
   removed; the server's config takes `pools:`, `launcher: external`
   only (and `kubernetes` until step 6), and rejects `workers:` with what
   to write instead. Docs (README, config.md, deploy.md) lead with one
   machine, then Kubernetes.
6. **Kubernetes**: the server creates, updates and deletes a Deployment
   per pool of a project and version (old versions drain), `{secret:}`
   from Kubernetes Secrets; the Helm chart shrinks to the server and the
   RBAC to manage Deployments in its namespace.
   ✅ The server's side: `projects.launcher: kubernetes` runs each pool
   as `moksha-<project>-<pool>-v<version>`, created at `min`, scaled from
   the work waiting, deleted when its version is replaced or the project
   deleted, and leftovers of its `kubernetes.instance` deleted at start;
   `{secret: name}` is key `value` of the Secret `name` (not checked at
   deploy: the server doesn't read Secrets); code in place is refused
   ([config.md](config.md#projects-on-kubernetes-a-deployment-per-pool)).
   Next: the chart installs only the server, with that config and RBAC,
   and `launcher: kubernetes` in the server's `pools:` goes.
7. **The web UI**: projects, versions and what each deploy changed.

## Open questions

- **Several projects on one server**: queues per project (`revenue/default`)
  or shared, pipeline names that clash, tenants. Later; until then, one
  project per server is the supported shape, and the server refuses a
  second project whose pipeline names clash.
- **Rollout control**: drain immediately, or keep the old version for runs
  started on it (pinning, on the roadmap as "build pinning")?
- **Who may deploy**: needs authentication on the API first.
- **Versions**: the build ID (from the code) alone, or a counter per
  project (v1, v2, …) shown to people, with the build ID underneath.
