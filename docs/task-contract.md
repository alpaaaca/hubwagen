# Task contract

What a task's process can rely on, in every runtime (`agent`, `job`, `local`).

> **Status: design.** Not implemented yet. It will be implemented once, in
> `hubwagen exec` (`internal/taskexec`), and used by both the Job entrypoint and
> the agents — see [roadmap.md](roadmap.md).

## Process

- The wrapper runs `command` + rendered `args` as a child process, with the
  working directory set to a fresh, writable scratch directory (`$HUBWAGEN_WORKDIR`).
- `stdout` and `stderr` are captured line by line, timestamped, and shipped to
  the bucket (see Logs). They are also echoed to the wrapper's own stdout, so
  `kubectl logs` still works for Jobs.
- On timeout or cancellation the wrapper sends `SIGTERM` to the process group,
  waits `termination_grace` (default 30 s), then sends `SIGKILL`.

## Environment

| Variable | Value |
|---|---|
| `HUBWAGEN_TENANT`, `HUBWAGEN_PIPELINE` | Names |
| `HUBWAGEN_RUN_ID`, `HUBWAGEN_RUN_KEY`, `HUBWAGEN_RUN_GENERATION` | Run identity |
| `HUBWAGEN_TASK_ID`, `HUBWAGEN_ATTEMPT`, `HUBWAGEN_ATTEMPT_ID` | Task and attempt identity |
| `HUBWAGEN_IDEMPOTENCY_KEY` | `<run-key>/gen-<g>/<task-id>`: stable across attempts, use it for external side effects |
| `HUBWAGEN_INTERVAL_START`, `HUBWAGEN_INTERVAL_END` | RFC 3339, empty for ad-hoc runs |
| `HUBWAGEN_INPUT_<NAME>` | URI of the committed upstream output prefix |
| `HUBWAGEN_OUTPUT_<NAME>` | URI of this attempt's staging prefix for that output |
| `HUBWAGEN_RESULT_FILE` | Local path the task may write a result JSON to |
| `HUBWAGEN_WORKDIR` | Scratch directory, deleted after the attempt |
| `TRACEPARENT` | W3C trace context, parent = this attempt's span |
| declared `env` and `secrets` | As specified in the pipeline |

`<NAME>` is the input/output name upper-cased, with `-` replaced by `_`.

Bucket credentials come from the pod's Kubernetes service account (workload
identity), scoped to the tenant's prefixes. The wrapper does not pass any
platform credentials to the task.

## Storage URIs

Input and output URIs use the scheme of the store the server is configured with:

| Profile / store | Scheme | Example |
|---|---|---|
| `dev`, single-host `standalone` | `file://` | `file:///var/lib/hubwagen/bucket/data/analytics/…/attempt-2/` |
| `standalone` with MinIO/Garage/S3 | `s3://` | `s3://hubwagen/data/analytics/…/attempt-2/` (endpoint in `AWS_ENDPOINT_URL`) |
| `kubernetes` on GCP / AWS | `gs://`, `s3://` | |

Tasks that should run unchanged in several profiles must accept any of these
schemes. Libraries that handle this already: `fsspec`/`pyarrow.fs` (Python),
`gocloud.dev/blob` (Go), the AWS/GCS SDKs plus a `file://` branch, or
`rclone`. In container tasks with a `file://` store, the referenced directories
are bind-mounted at the same path, so `file://` URIs are valid inside the
container.

## Container launch (`job` tasks and image pools)

| Profile | How the container is started | Where the wrapper comes from |
|---|---|---|
| `kubernetes` | One Job per attempt in the tenant namespace | Init container copies `hubwagen` into an `emptyDir` |
| `dev`, `standalone` | A container-capable agent calls the Docker/Podman API | The agent bind-mounts its own `hubwagen` binary read-only at `/hw/hubwagen` |

Either way the entrypoint is `/hw/hubwagen exec -- <command> <args>`, the
`resources` become container limits, and everything else in this contract
applies unchanged.

## Outputs and commit

1. The task writes each output under `$HUBWAGEN_OUTPUT_<NAME>` (an
   attempt-specific staging prefix). Writing anywhere else in the platform's
   `data/` area is not supported.
2. When the process exits with code 0, the wrapper commits: for each declared
   output it creates `…/gen-<g>/_COMMITTED` with create-if-absent semantics.
   The marker contains:
   ```json
   {"v":1,"attempt_id":"01J…","attempt":2,"uri":"gs://…/attempt-2/","committed_at":"…","metadata":{}}
   ```
3. If a marker already exists from another attempt, the commit fails, the
   attempt is reported as `superseded`, and its staging data is left for
   garbage collection.
4. Outputs declared but never written are committed as empty prefixes. Tasks
   that must produce data should fail explicitly instead.

## Result file (optional)

If the task writes `$HUBWAGEN_RESULT_FILE`, it must be JSON, at most 64 KiB:

```json
{
  "outputs": {"orders_raw": {"rows": 40213, "bytes": 18234411}},
  "metadata": {"dbt_models_failed": []}
}
```

`outputs.<name>` is merged into that output's marker metadata. `metadata` is
stored on the attempt. The wrapper uploads the file to `results/…` and includes
it in the attempt report.

## Exit codes

| Exit | Meaning |
|---|---|
| 0 | Success → commit outputs |
| 1–125 | Failure → retry if the policy allows |
| 126, 127 | Command not executable / not found → recorded with reason `exec_error`, **not retried** |
| signal | Killed: `timeout`, `cancelled`, or `oom` (from Pod status) |

## Logs

- Chunks are written every 2 s or 1 MiB to
  `logs/<tenant>/<pipeline>/<run-id>/<task-id>/<attempt>/<seq>.log`, as
  newline-delimited JSON: `{"ts":"…","stream":"stdout","line":"…"}`.
- On exit, the wrapper flushes and writes `…/<attempt>/index.json` (chunk count,
  total bytes, line count, final status).
- If the bucket is unreachable, chunks are buffered on local disk up to
  `log_buffer_limit` (default 256 MiB); beyond that the oldest unshipped lines
  are dropped and a marker line records how many.

## Heartbeats (agent runtime)

The agent sends a heartbeat for each running attempt every 10 s. The leader's
lease per attempt is 30 s. Job-runtime attempts are tracked through Pod status
instead and have no heartbeat.
