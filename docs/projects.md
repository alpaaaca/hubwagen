# Projects: `hubwagen deploy` instead of hand-run workers

> **Status:** design note; steps 1–3 are built (the dry run, deployments,
> registries and secrets); 4–6 are not. Builds on worker pools
> ([worker-pools.md](worker-pools.md), steps 1–4 built), container images
> ([deploy.md](deploy.md)) and builds (`RegisterBuild`). Decision
> requested: the shape below, and steps 1–2 first.

## Problem

Today one `hubwagen.yaml` mixes two things:

- **The instance**: where the server keeps its data, its bucket, what it
  listens on. That belongs to whoever runs the server.
- **An application**: a repo's pipelines and how to run their workers
  (`workers:`: a command, a directory, a `PYTHONPATH`). That belongs to
  the repo.

So `hubwagen dev --config pipelines/examples/revenue/hubwagen.yaml` starts
a server *and* worker processes from the repo's file, and a production
server has to be told about every project's workers in its own config,
by hand. There is no step where a repo says "here is my application, run
it", and nothing that shows what a change will rerun before it runs.

## Shape

A repo describes itself in a **project file**; `hubwagen deploy` hands it
to a server; the server runs and scales its workers.

```
  repo (or CI)                              server
  ┌──────────────────────────┐   Deploy    ┌───────────────────────────────┐
  │ hubwagen.yaml (Project)  │ ──────────► │ records the deployment (event)│
  │ Dockerfile, code         │             │ launcher starts its pools     │
  │ hubwagen deploy:         │             │ scaler sizes them from work   │
  │  build, diff, push image │             │ old version drains            │
  └──────────────────────────┘             └───────────────────────────────┘
           │ push                                    │ pull
           └──────────────► registry ◄───────────────┘
```

### The project file

The project file is the repo's `hubwagen.yaml`, with `kind: Project`
(the server's file stays `kind: Config`). Until `hubwagen dev` reads
project files (step 5), a repo whose `hubwagen.yaml` is still a server
config keeps its project in `hubwagen.project.yaml`, as the revenue example
does; `deploy` takes whichever of the two is `kind: Project`. It is not the pipelines: those
are code, found the way they are today, from the modules listed in
`pyproject.toml` (`[tool.hubwagen] modules`). The project file says how to
build and run that code:

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Project
name: revenue

build:
  dockerfile: Dockerfile             # FROM ghcr.io/alpaaaca/hubwagen-worker + the code
  image: ghcr.io/acme/revenue        # pushed as :<version>; a local server needs no push
  args: {WORKER_IMAGE: hubwagen-worker:dev}   # --build-arg, optional

pools:
  - name: default                    # the project's default dock
    min: 1                           # always running: a fixed baseline
    max: 6                           # scaled up from what waits, down after idle_after
    lifts: 2
  - name: gpu
    docks: [gpu]
    min: 0
    max: 2
    lifts: 1
    resources: {gpu: 1, memory: 16Gi}   # with pod steps (worker-pools step 5)

env:                                 # names and where values come from, never values
  DB_URL: {secret: revenue-db}       # a secret the server's launcher knows
  LOG_LEVEL: info                    # plain values are fine
```

- **Fixed and scaled in one**: a pool's `min` workers always run, and the
  server adds up to `max` from the work waiting; `min: max` is a fixed
  count. This is the pool of worker-pools step 2, now declared by the
  project instead of the server.
- **Why YAML next to `pyproject.toml`**, not a `[tool.hubwagen]` table:
  workers may not be Python forever, and the server's config is YAML too.
  The modules stay in `pyproject.toml`, where Python tools expect them.
- **No secrets** in the file: `{secret: name}` names a secret the server's
  side holds (a Kubernetes Secret, a file under the server's secrets
  directory); the value never passes through the CLI.

### `hubwagen deploy`

Run in the repo, by a person or by CI:

1. **Read** `hubwagen.yaml` and `pyproject.toml`.
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

- `hubwagen projects`: each project, its current version, when and by
  whom it was deployed, its pools (running/min/max).
- `hubwagen delete revenue`: stops the project's workers (draining, as on
  a new version) and records the project deleted. Its pipelines stop
  accepting new runs; its history, data versions and catalog stay. Runs
  still open are refused unless `--cancel-runs`.
- The web UI gets a projects page: versions, what each deploy changed,
  pools and their last scaling decision.

### `hubwagen dev` becomes a deploy

`hubwagen dev` in a repo starts a server in the process (as now) and
deploys the current directory to it with the **process launcher**: no
image, the workers run the code in place, in the project's environment.
It redeploys when the code changes. The repo's file is the project file;
nobody writes `workers:` any more.

### The server's config

Only the instance: data, bucket, listen address, and how to launch
(`launcher: container | kubernetes | process`, with its settings: the
runtime, the namespace, pull secrets, the secrets directory). The
`workers:` section stays for static workers someone else runs and for
pools not tied to a project (`launcher: external`); a project's pools
come from its deployment.

### What it builds on

| Exists | Becomes |
|---|---|
| Pools with `min`/`max`, scaler, `ListPools` | what a deployment's pools are |
| Container and Kubernetes launchers | how a deployment's workers start; for Kubernetes the server creates and updates the Deployment itself, so the Helm chart only installs the server (and RBAC to manage Deployments in its namespace) |
| `RegisterBuild`, code versions, "why it ran" | the diff that `deploy` shows before anything changes |
| Restarting workers on `no_build` (worker-pools, not built) | the rollout: old version drains, new one starts |

## Steps (PR-sized)

1. ✅ **Project file and `hubwagen deploy --dry-run`**: `kind: Project`
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
   (`python -m hubwagen.worker --build-json`, run on the deploying side),
   so its pipelines are accepted and its schedules apply before any
   worker runs. The server turns each live project's current version
   into pools named `<project>-<pool>-v<version>` (containers of its
   image, or processes in its directory); a new version replaces the
   old one's pools, whose workers get SIGTERM and finish what they run.
   `hubwagen deploy`, `projects`, `delete [--cancel-runs]`. Limits: an
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
   Secrets come with step 4.
4. **Kubernetes**: the server creates, updates and deletes a Deployment
   per pool of a project; the Helm chart shrinks to the server.
5. **`hubwagen dev` on deploy**: a local server, the process launcher,
   redeploy on change; `workers:` in the examples goes away.
6. **The web UI**: projects, versions and what each deploy changed.

## Open questions

- **Several projects on one server**: docks per project (`revenue/default`)
  or shared, pipeline names that clash, tenants. Later; until then, one
  project per server is the supported shape, and the server refuses a
  second project whose pipeline names clash.
- **Rollout control**: drain immediately, or keep the old version for runs
  started on it (pinning, on the roadmap as "build pinning")?
- **Who may deploy**: needs authentication on the API first.
- **Versions**: the build ID (from the code) alone, or a counter per
  project (v1, v2, …) shown to people, with the build ID underneath.
