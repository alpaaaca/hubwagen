# Task contract

What a task's process can rely on, in every runtime (`agent`, `job`, `local`).

> **Status:** implemented in `internal/taskexec` for host processes with a
> `file://` bucket (M2). Not yet: `hubwagen exec` as a container entrypoint,
> object-store URIs, `TRACEPARENT`, heartbeats — see [roadmap.md](roadmap.md).
> The tests in `internal/taskexec` are the executable version of this page.

## Process

- The wrapper runs `command` + rendered `args` as a child process, with the
  working directory set to a fresh, writable scratch directory (`$HUBWAGEN_WORKDIR`).
- The process runs in its own process group. Host processes inherit the
  runner's environment, minus every `HUBWAGEN_*` variable, plus the variables
  below.
- `stdout` and `stderr` are captured line by line, timestamped, and shipped to
  the bucket (see Logs). They can also be echoed to the wrapper's own stdout,
  so `kubectl logs` still works for Jobs.
- On timeout or cancellation the wrapper sends `SIGTERM` to the process group,
  waits `termination_grace` (default 30 s), then sends `SIGKILL`. Children
  that ignore `SIGTERM` are killed with the group.

## Environment

| Variable | Value |
|---|---|
| `HUBWAGEN_TENANT`, `HUBWAGEN_PIPELINE` | Names |
| `HUBWAGEN_RUN_ID`, `HUBWAGEN_RUN_KEY`, `HUBWAGEN_RUN_GENERATION` | Run identity |
| `HUBWAGEN_TASK_ID`, `HUBWAGEN_ATTEMPT`, `HUBWAGEN_ATTEMPT_ID` | Task and attempt identity |
| `HUBWAGEN_IDEMPOTENCY_KEY` | `<run-key>/gen-<g>/<task-id>`: stable across attempts, use it for external side effects |
| `HUBWAGEN_INTERVAL_START`, `HUBWAGEN_INTERVAL_END` | RFC 3339 in UTC (`2026-10-20T00:00:00Z`), empty for ad-hoc runs. Templates render the interval in the pipeline timezone instead |
| `HUBWAGEN_INPUT_<NAME>` | URI of the committed upstream output prefix |
| `HUBWAGEN_OUTPUT_<NAME>` | URI of this attempt's staging prefix for that output (the directory exists before the task starts) |
| `HUBWAGEN_INPUT_<NAME>_PATH`, `HUBWAGEN_OUTPUT_<NAME>_PATH` | Plain filesystem paths for the same, only when the bucket is local (`file://`) |
| `HUBWAGEN_RESULT_FILE` | Local path the task may write a result JSON to |
| `HUBWAGEN_WORKDIR` | Scratch directory, deleted after the attempt |
| `TRACEPARENT` | W3C trace context, parent = this attempt's span |
| declared `env` and `secrets` | As specified in the pipeline |

`<NAME>` is the input/output name upper-cased, with `-` replaced by `_`.

Secrets are resolved where the task runs, never in the leader. In dev mode
they come from the runner's environment as
`HUBWAGEN_SECRET_<TENANT>_<NAME>_<KEY>` (upper-cased, other characters as
`_`); a missing secret fails the attempt with reason `secret_error`.

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
2. When the process exits with code 0, the wrapper reads the result file and
   then commits by creating **one task-level marker** with create-if-absent
   semantics: `commits/<tenant>/<pipeline>/<task>/<run-key>/gen-<g>.json`.
   It lists every output, so a single attempt wins all of them:
   ```json
   {"v":1,"attempt_id":"01J…","attempt":2,"committed_at":"…",
    "outputs":{"rows":{"uri":"file:///…/attempt-2/","metadata":{"rows":40213}}}}
   ```
   Tasks without outputs commit too, so recovery can tell "finished" from
   "lost".
3. If the marker already exists, another attempt committed first: this attempt
   is reported as `superseded`, the task succeeds with the **winner's**
   outputs, and this attempt's staging data is left for garbage collection.
4. After winning, the wrapper also writes `_COMMITTED` into each output's
   `gen-<g>/` directory (best effort), for people and tools browsing the data.
5. Outputs declared but never written are committed as empty prefixes. Tasks
   that must produce data should fail explicitly instead.
6. After a leader restart, an attempt that was in flight is checked against
   the marker: if it (or another attempt) committed, the task is recorded as
   succeeded instead of being retried.

## Result file (optional)

If the task writes `$HUBWAGEN_RESULT_FILE`, it must be JSON, at most 64 KiB:

```json
{
  "outputs": {"orders_raw": {"rows": 40213, "bytes": 18234411}},
  "metadata": {"dbt_models_failed": []}
}
```

`outputs.<name>` becomes that output's metadata in the marker. `metadata` is
stored on the attempt. Only these two keys are allowed, and `outputs` may only
name declared outputs. The wrapper uploads the file to `results/…` and
includes it in the attempt report. An invalid or oversized result file fails
the attempt with reason `result_error` (not retried) and nothing is
committed.

## Exit codes

| Exit | Meaning |
|---|---|
| 0 | Success → commit outputs |
| 1–125 | Failure (`exit_code`) → retry if the policy allows |
| 126, 127, or the command can't be started | `exec_error`, **not retried** |
| killed | `timeout` or `cancelled`; `oom` from Pod status (Kubernetes, later) |

Other reasons the wrapper reports: `secret_error` (secret missing),
`setup_error` (workdir or staging directory), `result_error` (not retried),
`commit_error` (bucket unreachable while committing; retried, since no marker
was created).

## Logs

- Chunks are written every 2 s or 1 MiB to
  `logs/<tenant>/<pipeline>/<run-id>/<task-id>/<attempt>/<seq>.log`, as
  newline-delimited JSON: `{"ts":"…","stream":"stdout","line":"…"}`.
- On exit, the wrapper flushes and writes `…/<attempt>/index.json` (chunk count,
  total bytes, line count, final status).
- A `hubwagen` stream records the wrapper's own lines (start failures,
  signals sent).
- Lines longer than 64 KiB are split, not dropped.
- If the bucket is unreachable, chunks are buffered in memory up to 256 MiB;
  beyond that the unshipped lines are dropped, a line records how many, and
  `index.json` carries the count and the last error.

## Heartbeats (agent runtime)

The agent sends a heartbeat for each running attempt every 10 s. The leader's
lease per attempt is 30 s. Job-runtime attempts are tracked through Pod status
instead and have no heartbeat.
