"""How a worker reaches the bucket.

A worker on the server's machine reads and writes a file:// bucket
directly: it knows it shares the bucket when the bucket's .hubwagen-bucket
file holds the ID the task names. Any other worker moves files through URLs
the server hands out for the running attempt (presigned URLs of the object
store, or the server's own): a lift writes its output to a local directory,
which is uploaded once the lift returns, and its inputs are downloaded into
a local directory when it first reads them. Both directories go when the
attempt ends.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .client import Client

MARKER = ".hubwagen-bucket"
MODES = ("auto", "local", "remote")
TRIES = 3  # per file; each try after the first asks for new URLs
TIMEOUT = 300  # seconds without progress on one transfer
CHUNK = 1 << 20


def shares_bucket(task: dict) -> bool:
    """The worker sees the task's file:// bucket under the same path."""
    from ._model import local_path

    bucket, bucket_id = task.get("bucket", ""), task.get("bucketId", "")
    if not bucket.startswith("file://") or not bucket_id:
        return False
    try:
        return (local_path(bucket) / MARKER).read_text().strip() == bucket_id
    except OSError:
        return False


def is_remote(mode: str, task: dict) -> bool:
    if mode == "remote":
        return True
    if mode == "local":
        if not task.get("bucket", "").startswith("file://"):
            raise RuntimeError(f"--data local: the server's bucket {task.get('bucket')} is not a local directory")
        return False
    if "bucket" not in task:  # a server from before transfer URLs: its bucket is local
        return False
    return not shares_bucket(task)


class TransferError(Exception):
    pass


class Remote:
    """One running lift attempt's data, moved through transfer URLs into and
    out of workdir."""

    def __init__(self, client: Client, attempt_id: str, workdir: Path):
        self.client, self.attempt, self.workdir = client, attempt_id, workdir
        self._inputs: dict[str, tuple[dict, dict[str, dict]]] = {}  # pallet → manifest, transfers by path

    @property
    def output_dir(self) -> Path:
        return self.workdir / "output"

    def _url(self, url: str) -> str:
        return urllib.parse.urljoin(self.client.server + "/", url)  # the server's own URLs are relative

    # --- inputs ---

    def _download_urls(self, pallet: str, fresh: bool = False) -> tuple[dict, dict[str, dict]]:
        if fresh or pallet not in self._inputs:
            res = self.client.call("WorkerService", "GetDownloadURLs", {"attemptId": self.attempt, "pallet": pallet})
            manifest = json.loads(res["manifestJson"])
            self._inputs[pallet] = manifest, {t["path"]: t for t in res.get("files", [])}
        return self._inputs[pallet]

    def manifest(self, pallet: str) -> dict:
        return self._download_urls(pallet)[0]

    def file(self, pallet: str, name: str) -> Path:
        """One file of an input, downloaded on first use and checked
        against its manifest."""
        manifest, _ = self._download_urls(pallet)
        entry = next((f for f in manifest["files"] if f["path"] == name), None)
        if entry is None:
            raise FileNotFoundError(f"data version {pallet} has no file {name!r}")
        dest = self.workdir / "inputs" / pallet / name
        if dest.exists():
            return dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        for attempt in range(TRIES):
            _, transfers = self._download_urls(pallet, fresh=attempt > 0)
            t = transfers[name]
            try:
                req = urllib.request.Request(self._url(t["url"]), headers=t.get("headers", {}))
                h, size = hashlib.sha256(), 0
                with urllib.request.urlopen(req, timeout=TIMEOUT) as resp, open(part, "wb") as f:
                    while chunk := resp.read(CHUNK):
                        h.update(chunk)
                        size += len(chunk)
                        f.write(chunk)
                if size != int(entry["size"]) or h.hexdigest() != entry["sha256"]:
                    raise TransferError(f"{name}: got {size} bytes with another sha256 than the manifest's")
                os.replace(part, dest)
                return dest
            except (OSError, urllib.error.URLError, TransferError) as e:
                err = e
                time.sleep(0.5 * (attempt + 1))
        raise TransferError(f"downloading {name} of data version {pallet}: {_describe(err)}")

    def files(self, pallet: str) -> list[Path]:
        return [self.file(pallet, f["path"]) for f in self.manifest(pallet)["files"]]

    # --- output ---

    def upload(self, files: list[dict[str, Any]]) -> None:
        """PUT the output files (as describe() lists them) to the attempt's
        staging prefix."""
        if not files:
            return
        transfers: dict[str, dict] = {}
        for f in files:
            for attempt in range(TRIES):
                if attempt > 0 or not transfers:
                    res = self.client.call("WorkerService", "GetUploadURLs", {"attemptId": self.attempt, "files": files})
                    transfers = {t["path"]: t for t in res.get("files", [])}
                t = transfers[f["path"]]
                try:
                    with open(self.output_dir / f["path"], "rb") as body:
                        headers = {**t.get("headers", {}), "Content-Length": str(f["size"]),
                                   "Content-Type": "application/octet-stream"}
                        req = urllib.request.Request(self._url(t["url"]), data=body, headers=headers, method="PUT")
                        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                            resp.read()
                    break
                except (OSError, urllib.error.URLError) as e:
                    err = e
                    time.sleep(0.5 * (attempt + 1))
            else:
                raise TransferError(f"uploading {f['path']}: {_describe(err)}")

    def close(self) -> None:
        shutil.rmtree(self.workdir, ignore_errors=True)


def _describe(e: Exception) -> str:
    if isinstance(e, urllib.error.HTTPError):
        try:
            body = e.read().decode(errors="replace").strip()
        except OSError:
            body = ""
        return f"HTTP {e.code}: {body[:300] or e.reason}"
    return str(getattr(e, "reason", e))
