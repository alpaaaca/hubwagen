"""A small client for hubwagen's Connect services over HTTP/JSON (standard
library only)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class ConnectError(Exception):
    """An error answer from the server: code is a Connect code such as
    "not_found" or "invalid_argument", or "unavailable" if it can't be
    reached."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code, self.message = code, message


class Client:
    def __init__(self, server: str = "http://127.0.0.1:7070"):
        self.server = server.rstrip("/")

    def call(self, service: str, method: str, body: dict, timeout: float = 30) -> dict:
        req = urllib.request.Request(
            f"{self.server}/hubwagen.v1.{service}/{method}", data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            try:
                err = json.loads(e.read() or b"{}")
            except ValueError:
                err = {}
            raise ConnectError(err.get("code", f"http_{e.code}"), err.get("message", e.reason)) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            raise ConnectError("unavailable", f"{self.server}: {getattr(e, 'reason', e)}") from None

    # --- HaulService ---

    def start_haul(self, name: str, params: dict[str, Any] | None = None, *, tenant: str = "default",
                   dock: str = "") -> str:
        from ._model import encode
        body = {"tenant": tenant, "name": name, "paramsJson": json.dumps(encode(params or {}))}
        if dock:
            body["dock"] = dock
        return self.call("HaulService", "StartHaul", body)["haulId"]

    def get_haul(self, haul_id: str) -> dict:
        return self.call("HaulService", "GetHaul", {"haulId": haul_id})["haul"]
