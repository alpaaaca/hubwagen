# Remote storage: object-store buckets and remote workers

Design note for object-store buckets (`s3://`) and workers that don't share
the server's filesystem. Built in the first step; later steps are in
[roadmap.md](roadmap.md) ("Remote storage").

## The problem

Until now the bucket was always `file://<data_dir>/bucket`, and workers
read and wrote it directly: the server put the staging prefix and input
manifests in a lift task as `file://` URIs, and the SDK turned them into
local paths. So workers had to run on the server's machine, or share its
filesystem.

What uses the bucket:

- **The server** writes only pallet manifests (the promote: create-if-absent,
  then a byte compare if one is there), lists a staging prefix (promote checks
  the reported files are there), reads manifests (catalog API, retention) and
  deletes prefixes (discarded attempts, expired pallets).
- **Workers** write a lift attempt's files to its staging prefix and read the
  files of its input pallets.

## Decision: only the server talks to the bucket

The server is the only bucket client. Workers move bytes with URLs the
server hands them for one running lift attempt:

- `GetUploadURLs(attempt, files)`: one PUT URL per output file, under that
  attempt's staging prefix.
- `GetDownloadURLs(attempt, pallet)`: an input's manifest and one GET URL
  per file. Only the attempt's inputs.

For an `s3://` bucket these are presigned URLs of the store (15 minutes).
For a `file://` bucket, and for `s3://` with `transfers: server`, they are
the server's own: `/v1/data/<path>?…`, relative to the server's URL, signed
with a key the server makes at start (HMAC over method, path, size, sha256
and expiry), streamed to and from the bucket. Either way, a URL reaches one
object, and an attempt that isn't running gets none.

**Why not object-store credentials on every worker?** The SDK would need
boto3 (or its own request signing), and one client per cloud in every SDK;
every machine would hold credentials to the whole bucket. With URLs the SDK
stays standard library only, a new store is one Go implementation in the
server, and workers hold no storage credentials. The costs: one or two extra
RPCs per lift, and with `transfers: server` the bytes go through the server.

**Until auth exists** anyone who can reach the API can get URLs for running
attempts' staging and inputs. They can already start, cancel and complete
work through it; tokens (`server.auth`) close both.

## Commits on an object store

- **Create-if-absent**: the manifest PUT carries `If-None-Match: *`. The
  store answers 412 if the object exists (ErrExists), and 409 to a write
  racing another conditional write, which is retried and then sees the
  winner. S3 has had this since 2024; MinIO, R2 and GCS (`x-goog-if-generation-match: 0`
  natively; If-None-Match through its XML API) have it too.
- **The server refuses a store without it**: opening the bucket writes a
  probe object twice; if the second write succeeds, the server doesn't start.
  Weaker commits would break "never reuse what can't be read".
- **Idempotent promote**: unchanged. A retried promote (or a deposed leader)
  finds the manifest and compares bytes.
- **Uploads are checked**: presigned PUT URLs sign the file's
  `x-amz-checksum-sha256`, so the store refuses other bytes; the server's own
  data endpoint hashes while it streams and fails the write (nothing becomes
  visible) on a mismatch. Promote still checks that every listed file is
  there.

## Configuration

```yaml
bucket:
  url: s3://acme-hubwagen/prod
  endpoint: https://minio.internal:9000     # S3-compatible; omit for AWS
  public_endpoint: https://s3.acme.example  # in workers' URLs, if they reach the store under another name
  region: eu-central-1                      # default: asked of the store
  path_style: true                          # MinIO
  access_key_id_file: /run/secrets/s3_key_id
  secret_access_key_file: /run/secrets/s3_secret
  transfers: presigned                      # or server: bytes through the server
```

Credentials are files, never values in the config. Without them the usual
AWS sources apply (environment, shared credentials file, instance or pod
role). Only the server reads this section.

A data directory works with one bucket: the server records the bucket on
first start and refuses another, since its catalog points to manifests
there. A data directory from before this check is tied to its `file://`
bucket.

## Workers: local or remote

A lift task names the bucket and, for a `file://` bucket, its ID, which the
server keeps in `<bucket>/.hubwagen-bucket`. The Python worker
(`--data auto`, the default) reads and writes the bucket directly only when
it finds that file with that ID, so it is on the server's machine (or shares
its filesystem); otherwise it is remote. `--data local|remote` forces one.

Remote, the step's code doesn't change: `hw.output().path` is a local
directory, uploaded once the step returns; a `DataVersion`'s `files()`,
`path()` and `load()` download what they need into a local directory first,
checking size and sha256 against the manifest. Both directories are per
attempt and removed when it ends. Transfers that fail are retried with new
URLs; then the attempt fails, for its retry policy.

## Tests

- `internal/blob`: every test runs on the `file://` bucket, on `s3fake` (an
  S3-compatible server in memory: real HTTP, conditional writes, checksums,
  pagination, presigned-URL expiry), and on a real store when
  `HUBWAGEN_TEST_S3=http://key:secret@host:port/bucket` names one (a local
  MinIO). s3fake doesn't check signatures; the real store does.
- `internal/api`: a haul runs twice with data only through URLs, on a file
  bucket, an s3 bucket with presigned URLs, and an s3 bucket through the
  server; the second run reuses both lifts. URLs are scoped and checked.
- SDK (`tests/test_remote.py`): the revenue example with a worker that shares
  no directory with `hubwagen dev`, against a file bucket and against s3fake;
  the rerun reuses the unchanged steps. Dataframes too.
