# Instance configuration

> **To be revised for v2.** Written for v1's M3 (agents, pools, profiles).
> Server, dock and worker settings change with v2; see
> [v2-migration.md](v2-migration.md).

How a hubwagen instance (leader, agents, dev mode) is configured: one YAML
file, `hubwagen.yaml`, plus environment variables and flags on top.

> **Status:** design, not implemented. Today every setting is a flag
> (`--data-dir`, `--slots`, `--pools`, `--log-level`). The loader lands with M3,
> when `hubwagen server` and `hubwagen agent` need settings; the `standalone`
> sections follow in M3b. See [roadmap.md](roadmap.md).

## What belongs here

The config file describes the **instance**: where state and data live, which
profile is active, what the server listens on, how agents and logins
authenticate, what agents serve, retention. It never describes what a pipeline
does. Anything that changes a run's behaviour belongs in the pipeline spec,
which is versioned and hashed; the config file is neither. When a setting
could live in both (a default timeout, say), the spec wins and the config only
supplies limits the server enforces (a maximum timeout).

## Format

YAML, decoded strictly like pipelines: unknown fields, wrong types and
duplicate keys are errors, and every error names its location
(`hubwagen.yaml: agent.slotz: unknown field`). All problems are reported at
once.

Why YAML rather than INI (Airflow's `airflow.cfg`) or TOML: pipelines are
already YAML, so are Kubernetes ConfigMaps and Helm values, and
`gopkg.in/yaml.v3` with `KnownFields` is already how `internal/spec` decodes.
INI has no types or nesting (lists of pools and tenants get awkward); TOML
would be a second format and dependency for little gain.

The file carries an `apiVersion` and `kind`, like a pipeline, so the format
can change later without guessing what a file is:

```yaml
apiVersion: hubwagen.dev/v1alpha1
kind: Config

profile: standalone              # dev | standalone | kubernetes
data_dir: /var/lib/hubwagen      # SQLite state, lock file, local bucket in dev
bucket: s3://hubwagen-data       # file:///… for dev and single-host standalone

server:
  listen: 127.0.0.1:7480
  auth:
    tokens_file: /etc/hubwagen/tokens       # hashed static tokens (standalone)

agent:
  server: https://hubwagen.internal:7480    # remote agents; empty = embedded
  token_file: /etc/hubwagen/agent-token
  pools: [local]
  slots: 4

scheduler:
  max_task_timeout: 48h
  history_retention: 2160h       # 90 days; Go durations, as in pipelines

replication:                     # Litestream (M3b/M4)
  target: file:///mnt/backup/hubwagen

secrets:
  provider: file                 # env | file | k8s
  dir: /etc/hubwagen/secrets

log:
  level: info                    # debug | info | warn | error
  format: text                   # text | json
```

The field list above is a sketch; each milestone adds the sections it needs.
Every field has a default, so an empty file (or none) is a valid dev-mode
config.

## Where settings come from

Later sources override earlier ones, field by field:

1. Built-in defaults (per profile: `dev` defaults to `data_dir: .hubwagen`,
   `bucket: file://<data_dir>/bucket`, `server.listen: 127.0.0.1:…`).
2. The config file.
3. Environment variables.
4. Command-line flags (`--data-dir`, `--slots`, … keep working).

The file is found at `--config <path>`, else `$HUBWAGEN_CONFIG`, else
`./hubwagen.yaml`, else `/etc/hubwagen/hubwagen.yaml`. If `--config` or
`$HUBWAGEN_CONFIG` names a file that doesn't exist, that is an error; the two
default paths are optional.

Settings are read once at start-up. Changing them needs a restart; there is no
hot reload (pipeline files are what `dev` watches, not the config).

## Environment variables

Only settings that are listed explicitly get a variable; there is no generic
mapping of every field. The variables share the `HUBWAGEN_` prefix with the
[task contract](task-contract.md) (`HUBWAGEN_RUN_ID`, `HUBWAGEN_SECRET_…`, …),
so a test checks that no config variable collides with a name or prefix the
task contract reserves. The task wrapper already strips every `HUBWAGEN_*`
variable from a task's environment, so instance settings never leak into
tasks.

| Variable | Field |
|---|---|
| `HUBWAGEN_CONFIG` | path of the config file |
| `HUBWAGEN_PROFILE` | `profile` |
| `HUBWAGEN_DATA_DIR` | `data_dir` |
| `HUBWAGEN_BUCKET` | `bucket` |
| `HUBWAGEN_LOG_LEVEL` | `log.level` |

## Secrets

The file never contains secret values, so it can be committed or shipped as a
ConfigMap. It points to where they are instead: `*_file` fields for tokens and
keys, the secret provider for task secrets, and the platform's own mechanisms
for bucket credentials (`AWS_*` variables, workload identity).

## Profiles

`profile` is read only by `internal/profile`, which turns the loaded config
into the concrete implementations (store, bucket, elector, replicator,
runtimes, secret provider). No other package looks at the profile or at the
raw config. Fields that make no sense for the active profile (`replication`
in `dev`, `server.auth.tokens_file` in `kubernetes`) are errors, not silently
ignored.

| Profile | Typical source |
|---|---|
| `dev` | no file, flags only |
| `standalone` | `/etc/hubwagen/hubwagen.yaml`, installed with the systemd units |
| `kubernetes` | a ConfigMap mounted as a file, rendered by the Helm chart |

## Commands

- `hubwagen config check [path]` validates a file (CI, before a deploy) and
  exits non-zero on errors.
- `hubwagen config show` prints the effective configuration and, per field,
  where it came from (default, file, env, flag). Paths of secret files are
  shown; their contents never are.

## Implementation notes

- Package `internal/config`: `Config` struct, `Load(path, env, flags)`,
  defaults per profile, validation. Pure; tested with table tests and no I/O
  beyond reading the file.
- Flags stay the cobra flags they are today; the loader takes the set of flags
  the user actually passed (`Flags().Changed`) so unset flags don't override
  the file.
