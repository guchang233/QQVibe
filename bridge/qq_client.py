"""Loopback JSON-RPC client for the injected QQNT reader.

The reader lives inside QQ's Electron main process (see `qqnt_install.py`) and
publishes a `{port, token, pid}` descriptor. This client talks to it over
`127.0.0.1` with the per-launch token. It is the only place that speaks HTTP to
the reader; `qq_source.py` consumes the decoded results.
"""
from __future__ import annotations

import http.client
import json
import threading
import time

import qqnt_install

READER_TOKEN_HEADER = "X-QQVibe-Token"
DEFAULT_TIMEOUT = 30.0
READY_TIMEOUT = 45.0


class ReaderUnavailable(RuntimeError):
    """The injected QQNT reader is not reachable yet."""

    def __init__(self, detail=""):
        super().__init__("当前 QQ 未就绪，请先登录 QQ 并保持其运行" + (f": {detail}" if detail else ""))


class ReaderError(RuntimeError):
    """The reader answered but the requested read failed."""


class QqClient:
    """Thread-safe, single-flight client for the injected reader."""

    def __init__(self, ensure=None, ready_timeout=READY_TIMEOUT):
        self._lock = threading.RLock()
        self._descriptor = None
        self._ensure = ensure or qqnt_install.ensure_injected
        self._ready_timeout = ready_timeout
        self._last_error = None

    # --- connection management -------------------------------------------------
    def _connect_descriptor(self, *, force=False):
        with self._lock:
            if not force and self._descriptor is not None:
                return self._descriptor
            descriptor = qqnt_install.read_descriptor()
            if descriptor and qqnt_install.process_alive(int(descriptor.get("pid") or 0)):
                self._descriptor = descriptor
                return descriptor
            self._descriptor = None
            return None

    def descriptor(self, *, wait=True):
        descriptor = self._connect_descriptor()
        if descriptor is not None:
            return descriptor
        if not wait:
            return None
        self._ensure(self._ready_timeout)
        descriptor = self._connect_descriptor(force=True)
        if descriptor is None:
            raise ReaderUnavailable(self._last_error or "")
        return descriptor

    def reset(self):
        with self._lock:
            self._descriptor = None

    def available(self):
        try:
            descriptor = self.descriptor(wait=False)
        except Exception:
            return False
        if descriptor is None:
            return False
        try:
            self.call("ping")
            return True
        except (ReaderUnavailable, ReaderError, OSError):
            return False

    # --- transport -------------------------------------------------------------
    def call(self, method, params=None, timeout=DEFAULT_TIMEOUT):
        descriptor = self.descriptor()
        payload = json.dumps({"method": method, "params": params or {}},
                             ensure_ascii=False).encode("utf-8")
        connection = http.client.HTTPConnection(
            "127.0.0.1", int(descriptor["port"]), timeout=timeout)
        try:
            connection.request(
                "POST", "/rpc", body=payload,
                headers={"Content-Type": "application/json; charset=utf-8",
                         "Content-Length": str(len(payload)),
                         "Connection": "close",
                         READER_TOKEN_HEADER: descriptor["token"]})
            response = connection.getresponse()
            body = response.read()
        except (OSError, http.client.HTTPException) as error:
            self._last_error = str(error)
            self.reset()
            raise ReaderUnavailable(str(error)) from error
        finally:
            connection.close()
        if response.status != 200:
            self._last_error = f"HTTP {response.status}"
            self.reset()
            raise ReaderUnavailable(f"HTTP {response.status}")
        try:
            envelope = json.loads(body or b"{}")
        except ValueError as error:
            raise ReaderError("读取器返回了非法响应") from error
        if not envelope.get("ok"):
            error = envelope.get("error") or {}
            raise ReaderError(error.get("message") or error.get("code") or "reader-error")
        return envelope.get("result")

    def wait_ready(self, timeout=None):
        """Poll the reader until it answers, or raise after the deadline."""
        deadline = time.monotonic() + (timeout if timeout is not None else self._ready_timeout)
        last = None
        while time.monotonic() < deadline:
            try:
                return self.call("ping")
            except (ReaderUnavailable, ReaderError, OSError) as error:
                last = error
                time.sleep(0.5)
        raise ReaderUnavailable(str(last) if last else "")

    # --- read helpers (thin wrappers over RPC methods) -------------------------
    def self_info(self):
        return self.call("self") or {}

    def contacts(self):
        return self.call("contacts") or {}

    def sessions(self):
        return self.call("sessions") or {}

    def messages(self, peer, *, is_group=False, limit=80, offset=0, order="latest"):
        return self.call("messages", {"peer": peer, "isGroup": is_group,
                                      "limit": limit, "offset": offset, "order": order})

    def history_highwater(self, peer, is_group=False):
        return self.call("historyHighwater", {"peer": peer, "isGroup": is_group})

    def history_page(self, peer, *, ceiling=None, after=None, limit=1000, is_group=False):
        return self.call("historyPage", {"peer": peer, "isGroup": is_group, "ceiling": ceiling,
                                         "after": after, "limit": limit},
                         timeout=60.0)

    def preceded_text(self, peer, before, limit=3, is_group=False):
        return self.call("precededText", {"peer": peer, "isGroup": is_group,
                                          "before": before, "limit": limit}) or []

    def history_before(self, peer, before, *, limit=100, is_group=False):
        return self.call("historyBefore", {"peer": peer, "isGroup": is_group,
                                           "before": before, "limit": limit}) or {}

    def history_after(self, peer, after, *, limit=100, is_group=False):
        return self.call("historyAfter", {"peer": peer, "isGroup": is_group,
                                          "after": after, "limit": limit}) or {}

    def message_at(self, peer, position, *, is_group=False):
        return self.call("messageAt", {"peer": peer, "isGroup": is_group,
                                       "position": position})

    def history_search(self, peer, *, before=None, start_ms=None, end_ms=None, query="",
                       limit=50, is_group=False):
        return self.call("historySearch", {"peer": peer, "isGroup": is_group,
                                           "before": before, "startMs": start_ms,
                                           "endMs": end_ms, "query": query, "limit": limit}) or {}

    def senders(self, peer, *, is_group=False):
        return self.call("senders", {"peer": peer, "isGroup": is_group}) or []

    def texts_by_ids(self, peer, ids, *, is_group=False):
        return self.call("textsByIds", {"peer": peer, "isGroup": is_group,
                                        "ids": list(ids)}) or []

    def quoted_history_page(self, peer, *, ceiling=None, after=None, limit=64,
                            member=None, is_group=False):
        return self.call("quotedHistoryPage", {"peer": peer, "isGroup": is_group,
                                               "ceiling": ceiling, "after": after,
                                               "limit": limit, "member": member})

    def profile_metadata(self, peer, member=None, is_group=False):
        return self.call("profileMetadata", {"peer": peer, "isGroup": is_group,
                                             "member": member}) or {}

    def image(self, ref):
        return self.call("image", {"ref": ref}, timeout=30.0)