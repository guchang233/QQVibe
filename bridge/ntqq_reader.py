"""QQ data acquisition through the Rust `ntqq-reader` sidecar.

This module replaces the previous in-process SQLCipher stack
(`qq_nt_local` / `qq_nt_crypto` / `qq_nt_key`).  Everything that used to be
hand-written here is now supplied by two pieces of existing, maintained code:

* ``ntdb_unwrap`` (MIT, https://github.com/artiga033/ntdb_unwrap) owns the hard
  part -- an SQLite VFS that skips the 1024-byte header QQNT prepends, plus the
  SQLCipher PRAGMA sequence and the HMAC_SHA256 -> HMAC_SHA1 fallback;
* ``sidecar/ntqq-reader`` (this repository) wraps that in a loopback HTTP
  service that opens every connection read-only.

What is deliberately *not* here any more:

* no SQLCipher key derivation and no page decryption (that was ~500 lines of
  hand-rolled crypto that had to agree with SQLCipher byte for byte);
* no ``ReadProcessMemory`` scan of ``QQ.exe`` (the key comes from NapCat's
  passphrase, which is what ``ntdb_unwrap``'s decrypt API wants anyway);
* no materialising the database in RAM.  The old reader peaked around 31 GiB on
  a real 5.6 GB ``nt_msg.db``; SQLCipher now decrypts one page at a time.

The public surface is exactly the interface ``QQSource`` expects from its
``local_backend``: ``Coordinator().ensure() -> (uin, ntdb)`` where ``ntdb``
exposes ``sessions`` / ``sender_display`` / ``newest_rows`` / ``older_rows``.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Schema facts.
#
# QQNT stores messages with numeric column names.  These numbers are the ones
# `ntdb_unwrap` uses in its own `group_msg_table` model (see its
# src/db/model/nt_msg/group_msg_table.rs) and the two sources agree, which is
# the closest thing to an authoritative reference available offline.
# ---------------------------------------------------------------------------
C2C_TABLE = "c2c_msg_table"
GROUP_TABLE = "group_msg_table"

COL_MSG_ID = "40001"      # INTEGER PRIMARY KEY, globally unique
COL_GROUP_SEQ = "40003"   # per-group sequence (group table only)
COL_CHAT_TYPE = "40010"   # 1 = c2c, 2 = group
COL_DIRECTION = "40013"   # 0 received, 1/2 self, 3 system
COL_PEER = "40030"        # c2c peer QQ / group number
COL_SENDER = "40033"      # sender QQ
COL_TIME = "40050"        # unix seconds
COL_CARD = "40090"        # group card (group table only)
COL_NICK = "40093"        # sender nickname
COL_CONTENT = "40800"     # protobuf MsgBody

MESSAGE_FIELDS = (COL_MSG_ID, COL_GROUP_SEQ, COL_DIRECTION, COL_SENDER,
                  COL_TIME, COL_CARD, COL_NICK, COL_CONTENT)

PASSPHRASE_ENV = "QQVIBE_NT_MSG_KEY"
SIDECAR_URL_ENV = "QQVIBE_SIDECAR_URL"
SIDECAR_EXE_ENV = "QQVIBE_SIDECAR_EXE"

START_TIMEOUT = 30.0
QUERY_TIMEOUT = 120.0


class NtqqReaderError(RuntimeError):
    """The sidecar could not be reached or answered with an error."""


class NtqqKeyMissing(NtqqReaderError):
    """No SQLCipher passphrase is available."""


class MessageBody:
    """Minimal reader for the protobuf blob in column ``40800``.

    This is a wire-format walker, not a protobuf implementation: it only needs
    to pick specific varint and length-delimited fields out of a flat message,
    using the field numbers documented in ``ntdb_unwrap``'s
    ``src/protos/message.proto``.  Pulling in a protobuf runtime would mean
    generating a module from that .proto at build time, which needs ``protoc``
    on every machine that runs the client.

    Layout::

        Message       { repeated SingleMessage messages = 40800; }
        SingleMessage { messageId 45001, messageType 45002, messageText 45101,
                        fileName 45402, imageUrlOrigin 45804, emojiText 47602,
                        noticeInfo 48214, ... }
    """

    #: field number -> attribute name, for the fields the bridge cares about.
    STRING_FIELDS = {
        45101: "text",           # messageText
        45402: "file_name",      # fileName
        45802: "image_url_low",
        45803: "image_url_high",
        45804: "image_url_origin",
        45815: "image_text",
        47602: "emoji_text",
        47901: "application",    # applicationMessage
        48157: "call_text",
        48214: "notice",
    }
    VARINT_FIELDS = {
        45001: "message_id",
        45002: "message_type",
        45405: "file_size",
    }

    #: QQ message type -> the bridge's coarse flag.
    #: QQ sends 1 text, 2 image, 3 file, 6 emoji, 7 reply, 8 notice,
    #: 10 application, 21 call, 26 feed.
    TYPE_FLAGS = {
        1: "text", 2: "image", 3: "card", 6: "face", 7: "text",
        10: "card", 21: "voice", 26: "card",
    }
    #: message types that carry no user-visible content of their own.
    SYSTEM_TYPES = frozenset({8})

    __slots__ = ("fields", "nested")

    def __init__(self):
        self.fields: dict[str, object] = {}
        self.nested: list["MessageBody"] = []

    @classmethod
    def parse(cls, data: bytes) -> "MessageBody":
        body = cls()
        _walk_messages(data, body, depth=0)
        return body

    def _first(self) -> "MessageBody | None":
        """The first decoded SingleMessage, if the payload was a Message."""
        return self.nested[0] if self.nested else (self if self.fields else None)

    def decode(self) -> tuple[str, str, str, bool]:
        """Return ``(flag, text, image_url, system)``.

        Mirrors the contract the previous implementation exposed, so
        ``QQSource._nt_item`` keeps working unchanged.
        """
        single = self._first()
        if single is None:
            return "text", "", "", False
        fields = single.fields
        raw_type = int(fields.get("message_type") or 0)
        system = raw_type in self.SYSTEM_TYPES
        flag = self.TYPE_FLAGS.get(raw_type, "card" if raw_type else "text")

        image_url = str(fields.get("image_url_origin")
                        or fields.get("image_url_high")
                        or fields.get("image_url_low") or "")
        text = str(fields.get("text") or "")
        if not text:
            text = str(fields.get("emoji_text") or fields.get("file_name")
                       or fields.get("image_text") or fields.get("call_text")
                       or fields.get("notice") or "")
        if system:
            return flag, "", image_url, True
        return flag, text, image_url, False


def _read_varint(data: bytes, index: int) -> tuple[int, int]:
    value, shift = 0, 0
    while index < len(data):
        byte = data[index]
        index += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, index
        shift += 7
        if shift > 63:
            break
    raise ValueError("truncated varint")


def _walk_fields(data: bytes, into: dict, nested: list, depth: int) -> None:
    index = 0
    while index < len(data):
        key, index = _read_varint(data, index)
        field, wire = key >> 3, key & 0x7
        if wire == 0:
            value, index = _read_varint(data, index)
        elif wire == 2:
            length, index = _read_varint(data, index)
            chunk = data[index:index + length]
            if len(chunk) < length:
                return
            index += length
            if field in MessageBody.STRING_FIELDS:
                into[MessageBody.STRING_FIELDS[field]] = _lossy(chunk)
            elif depth < 3:
                inner = MessageBody()
                _walk_messages(chunk, inner, depth + 1)
                nested.append(inner)
            continue
        elif wire == 5:
            index += 4
            continue
        elif wire == 1:
            index += 8
            continue
        else:
            return
        if field in MessageBody.VARINT_FIELDS:
            into[MessageBody.VARINT_FIELDS[field]] = value


def _walk_messages(data: bytes, body: MessageBody, depth: int) -> None:
    """A ``Message`` wraps its ``SingleMessage`` items in field 40800."""
    index = 0
    while index < len(data):
        start = index
        try:
            key, index = _read_varint(data, index)
        except ValueError:
            return
        field, wire = key >> 3, key & 0x7
        if wire == 2:
            try:
                length, index = _read_varint(data, index)
            except ValueError:
                return
            chunk = data[index:index + length]
            if len(chunk) < length:
                return
            index += length
            if field == 40800:
                single = MessageBody()
                _walk_fields(chunk, single.fields, single.nested, depth + 1)
                body.nested.append(single)
            continue
        if wire == 0:
            try:
                _value, index = _read_varint(data, index)
            except ValueError:
                return
            continue
        if wire == 5:
            index += 4
            continue
        if wire == 1:
            index += 8
            continue
        if index == start:
            return


def _lossy(chunk: bytes) -> str:
    return chunk.decode("utf-8", "replace").replace("\x00", "")


def parse_message_body(data) -> tuple[str, str, str, bool]:
    """``(flag, text, image_url, system)`` for a raw ``40800`` blob."""
    if not data:
        return "text", "", "", False
    if isinstance(data, str):
        try:
            data = bytes.fromhex(data)
        except ValueError:
            return "text", "", "", False
    try:
        return MessageBody.parse(bytes(data)).decode()
    except (ValueError, RecursionError):
        return "text", "", "", False


# ---------------------------------------------------------------------------
# Locating the databases and the passphrase.
# ---------------------------------------------------------------------------

def nt_db_dirs() -> list[Path]:
    """Every ``<uin>/nt_qq/nt_db`` directory we can find on this machine.

    QQ lets the user relocate its data directory, so the profile Documents
    folder is only one candidate.  The store actually in use is picked by
    modification time rather than by guessing.
    """
    roots: list[Path] = []
    seen: set[str] = set()

    def add(root: Path) -> None:
        key = str(root).casefold()
        if key not in seen and root.is_dir():
            seen.add(key)
            roots.append(root)

    home = os.environ.get("USERPROFILE") or os.path.expanduser("~")
    add(Path(home) / "Documents" / "Tencent Files")
    for drive in "CDEFGH":
        add(Path(f"{drive}:/Documents/Tencent Files"))
        add(Path(f"{drive}:/Tencent Files"))
    for name in ("APPDATA", "LOCALAPPDATA"):
        base = os.environ.get(name)
        if base:
            add(Path(base) / "Tencent" / "Tencent Files")

    found: list[Path] = []
    for root in roots:
        try:
            entries = [entry for entry in root.iterdir() if entry.is_dir()]
        except OSError:
            continue
        for entry in entries:
            nt_db = entry / "nt_qq" / "nt_db"
            if nt_db.is_dir():
                found.append(nt_db)
    return found


class NapCatPassphrase:
    """Resolve the SQLCipher passphrase that NapCat obtained from QQNT.

    NapCat reads the passphrase out of ``OidbSvcTrpcTcp.0xcde_2`` at runtime.
    Rather than re-implement that hook, the bridge accepts the same string over
    one of two channels, checked in this order:

    1. ``QQVIBE_NT_MSG_KEY`` -- the variable the client already documented, and
       the easiest one to set from a NapCat plugin or a launcher script;
    2. ``%APPDATA%/QQVibe/napcat-passphrase.txt`` -- a single line, for setups
       where an environment variable is awkward.

    Note the passphrase is *not* the derived 32-byte key: ``ntdb_unwrap``
    derives the key with PBKDF2-HMAC-SHA512 (4000 iterations) itself, which is
    exactly what QQNT does.  Passing a derived key here would not work, and
    that is why the memory-scanning key extractor the previous implementation
    needed is gone.
    """

    FILENAME = "napcat-passphrase.txt"

    def __init__(self, environ=None):
        self._environ = os.environ if environ is None else environ

    def _appdata_candidates(self) -> list[Path]:
        base = self._environ.get("APPDATA")
        roots = [Path(base)] if base else []
        roots.append(Path(tempfile.gettempdir()))
        return [root / "QQVibe" / self.FILENAME for root in roots]

    def resolve(self) -> str:
        value = (self._environ.get(PASSPHRASE_ENV) or "").strip()
        if value:
            return value
        for candidate in self._appdata_candidates():
            try:
                text = candidate.read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if text:
                return text
        raise NtqqKeyMissing(
            f"未找到 QQ 数据库口令。请把 NapCat 提供的 passphrase 写入环境变量 "
            f"{PASSPHRASE_ENV}，或写入 "
            f"{self._appdata_candidates()[0]}（单行）。"
        )

    def available(self) -> bool:
        try:
            self.resolve()
        except NtqqKeyMissing:
            return False
        return True


# ---------------------------------------------------------------------------
# The sidecar process and its HTTP contract.
# ---------------------------------------------------------------------------

#: Where the reader binary is searched for, relative to the project root -- which
#: is also the packaged client root, because the stage is copied to
#: ``resources/client``.
#:
#: The last entry is where the release staging writes the binary, so a packaged
#: build resolves the exe exactly the way a source checkout does.  Both the
#: search order and the staged location are kept here alone on purpose:
#: ``scripts/stage-real-client.py`` imports this module instead of repeating the
#: list, so the two cannot drift apart.
EXE_NAMES = ("ntqq-reader.exe", "ntqq-reader")
PACKAGED_FOLDER = "resources/ntqq-reader"
SEARCH_FOLDERS = ("sidecar/ntqq-reader/target/release",
                  "sidecar/ntqq-reader/target/x86_64-pc-windows-msvc/release",
                  PACKAGED_FOLDER)


def default_exe() -> Path | None:
    override = os.environ.get(SIDECAR_EXE_ENV)
    if override:
        candidate = Path(override)
        return candidate if candidate.is_file() else None
    root = Path(__file__).resolve().parents[1]
    for folder in SEARCH_FOLDERS:
        for name in EXE_NAMES:
            candidate = root / folder / name
            if candidate.is_file():
                return candidate
    found = shutil.which("ntqq-reader")
    return Path(found) if found else None


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class ReaderClient:
    """HTTP client for the read-only sidecar, plus its process lifecycle.

    When ``QQVIBE_SIDECAR_URL`` is set the client attaches to an already running
    sidecar instead of spawning one.  That is what the tests use, and it also
    lets an operator run the sidecar under their own supervision.
    """

    def __init__(self, db_dir: Path, passphrase: str, *, exe: Path | None = None,
                 base_url: str | None = None, token: str | None = None,
                 port: int | None = None, timeout: float = QUERY_TIMEOUT):
        self.db_dir = Path(db_dir)
        self.passphrase = passphrase
        self._token = token or ""
        self._timeout = timeout
        self._process: subprocess.Popen | None = None
        self._lock = threading.Lock()
        self.health: dict | None = None
        self._exe: Path | None = None

        explicit = base_url or os.environ.get(SIDECAR_URL_ENV)
        if explicit:
            # Attach to a sidecar somebody else supervises; never spawn one.
            self._attach = True
            self.base_url = str(explicit).rstrip("/")
            return
        self._attach = False
        if exe is None:
            exe = default_exe()
        if exe is None:
            raise NtqqReaderError(
                "未找到 ntqq-reader sidecar。请先构建 sidecar/ntqq-reader "
                "（cargo build --release），或用 QQVIBE_SIDECAR_EXE / "
                f"{SIDECAR_URL_ENV} 指定。"
            )
        self._exe = Path(exe)
        self._port = port or _free_port()
        self.base_url = f"http://127.0.0.1:{self._port}"

    # -- process ---------------------------------------------------------

    def start(self) -> dict:
        with self._lock:
            if self.health is not None:
                return self.health
            if self._process is None and not self._attach:
                creation = 0
                if sys.platform == "win32":
                    creation = getattr(subprocess, "CREATE_NO_WINDOW", 0)
                args = [str(self._exe), "--db-dir", str(self.db_dir),
                        "--pkey", self.passphrase, "--listen", f"127.0.0.1:{self._port}"]
                if self._token:
                    args += ["--token", self._token]
                self._process = subprocess.Popen(
                    args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL, creationflags=creation)
            deadline = time.monotonic() + START_TIMEOUT
            last = None
            while time.monotonic() < deadline:
                if self._process is not None and self._process.poll() is not None:
                    detail = ""
                    if self._process.stderr is not None:
                        detail = self._process.stderr.read().decode("utf-8", "replace").strip()
                    raise NtqqReaderError(
                        f"ntqq-reader 启动即退出（code={self._process.returncode}）: {detail[:400]}")
                try:
                    self.health = self._request("GET", "/health")
                    return self.health
                except NtqqReaderError as exc:
                    last = exc
                    time.sleep(0.25)
            raise NtqqReaderError(f"ntqq-reader 未在 {START_TIMEOUT:.0f}s 内就绪: {last}")

    def close(self) -> None:
        with self._lock:
            process, self._process = self._process, None
            self.health = None
        if process is None:
            return
        try:
            process.terminate()
            process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
            except OSError:
                pass
        finally:
            for stream in (process.stdout, process.stderr):
                try:
                    if stream is not None:
                        stream.close()
                except OSError:
                    pass

    # -- HTTP ------------------------------------------------------------

    def _request(self, method: str, path: str, payload=None) -> dict:
        data = None
        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=data, headers=headers, method=method)
        # The sidecar is loopback-only; never route it through a system proxy.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=self._timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            try:
                message = json.loads(detail).get("error", detail)
            except ValueError:
                message = detail
            raise NtqqReaderError(f"sidecar 拒绝请求（{exc.code}）: {message[:400]}") from exc
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise NtqqReaderError(f"无法连接 sidecar: {exc}") from exc

    def query(self, db: str, sql: str, params: list | None = None) -> dict:
        return self._request("POST", "/query",
                             {"db": db, "sql": sql, "params": list(params or [])})

    def rows(self, sql: str, params: list | None = None, db: str = "nt_msg") -> list[tuple]:
        """Run a read and return plain tuples, one per row.

        BLOB values arrive from the sidecar as lowercase hex so the transport
        stays plain JSON.  Column ``40800`` holds the protobuf message body, so
        it is decoded back to ``bytes`` here and callers see exactly what a
        direct sqlite3 connection would have handed them.
        """
        result = self.query(db, sql, params)
        columns = list(result.get("columns") or [])
        blob_indexes = [index for index, name in enumerate(columns) if name == COL_CONTENT]
        out: list[tuple] = []
        for values in (result.get("rows") or []):
            row = list(values)
            for index in blob_indexes:
                if index < len(row) and isinstance(row[index], str):
                    try:
                        row[index] = bytes.fromhex(row[index])
                    except ValueError:
                        pass
            out.append(tuple(row))
        return out

    def table_columns(self, table: str, db: str = "nt_msg") -> set[str]:
        """Introspect a table at runtime.

        ``PRAGMA`` is rejected by the sidecar (only reads are allowed), so this
        uses SQLite's table-valued ``pragma_table_info`` function, which is an
        ordinary SELECT.  Introspecting instead of hard-coding the column list
        means a QQ update that adds a column cannot break the bridge.
        """
        rows = self.rows("SELECT name FROM pragma_table_info(?)", [table], db=db)
        return {str(row[0]) for row in rows if row and row[0] is not None}


# ---------------------------------------------------------------------------
# The `ntdb` role: exactly the four methods `QQSource` calls.
# ---------------------------------------------------------------------------

class NtMsgDb:
    """Message reads used by ``QQSource``'s local path."""

    REFRESH_SECONDS = 300.0

    def __init__(self, client: ReaderClient):
        self._client = client
        self._tables: dict[bool, str] = {}

    # -- helpers ---------------------------------------------------------

    def _table(self, is_group: bool) -> str:
        table = GROUP_TABLE if is_group else C2C_TABLE
        if is_group not in self._tables:
            # Fail with a precise message rather than an opaque SQL error.
            columns = self._client.table_columns(table)
            if COL_CONTENT not in columns:
                raise NtqqReaderError(f"{table} 缺少消息体列 {COL_CONTENT}")
            self._tables[is_group] = table
        return table

    @staticmethod
    def _order(is_group: bool) -> str:
        return f'"{COL_GROUP_SEQ}" DESC, "{COL_MSG_ID}" DESC' if is_group else f'"{COL_MSG_ID}" DESC'

    @staticmethod
    def _sort_value(is_group: bool, row) -> int:
        if is_group and row[1]:
            return int(row[1])
        return int(row[0] or 0)

    def _select(self, is_group: bool, where: str, params: list, order: str, limit: int):
        columns = ", ".join(f'"{name}"' for name in MESSAGE_FIELDS)
        sql = (f'SELECT {columns} FROM "{self._table(is_group)}"'
               f'{where} ORDER BY {order} LIMIT ?')
        return self._client.rows(sql, [*params, int(limit)])

    # -- contract --------------------------------------------------------

    def sessions(self) -> list[tuple[str, bool, int, int]]:
        """``(peer, is_group, last_time, count)`` for every conversation."""
        found: list[tuple[str, bool, int, int]] = []
        for is_group in (False, True):
            table = self._table(is_group)
            sql = (f'SELECT "{COL_PEER}", MAX("{COL_TIME}"), COUNT(*) '
                   f'FROM "{table}" GROUP BY "{COL_PEER}"')
            for peer, last_time, count in self._client.rows(sql):
                if peer is None or str(peer).strip() in ("", "0"):
                    continue
                found.append((str(peer), is_group, int(last_time or 0), int(count or 0)))
        return found

    def sender_display(self, peer, is_group) -> str:
        """The most recent display name recorded for ``peer``.

        Names live on the message rows themselves (``40093`` nickname, ``40090``
        group card), so there is no separate contact table to consult.
        """
        table = self._table(bool(is_group))
        value = str(peer)
        column = f'"{COL_NICK}"'
        sql = (f'SELECT {column} FROM "{table}" WHERE "{COL_PEER}" = ? '
               f"AND {column} IS NOT NULL AND {column} <> '' "
               f'ORDER BY {self._order(bool(is_group))} LIMIT 1')
        try:
            rows = self._client.rows(sql, [value])
        except NtqqReaderError:
            return ""
        return str(rows[0][0]) if rows else ""

    def newest_rows(self, peer, is_group, limit) -> list[tuple]:
        """The newest ``limit`` rows for one conversation, newest first."""
        is_group = bool(is_group)
        where = f' WHERE "{COL_PEER}" = ?'
        return self._select(is_group, where, [str(peer)], self._order(is_group), int(limit))

    def older_rows(self, peer, is_group, sort_seq, local_id, limit) -> list[tuple]:
        """The page of rows immediately older than ``(sort_seq, local_id)``."""
        is_group = bool(is_group)
        value = str(peer)
        if is_group:
            where = (f' WHERE "{COL_PEER}" = ? AND ("{COL_GROUP_SEQ}" < ? '
                     f'OR ("{COL_GROUP_SEQ}" = ? AND "{COL_MSG_ID}" < ?))')
            params = [value, int(sort_seq), int(sort_seq), int(local_id)]
        else:
            where = f' WHERE "{COL_PEER}" = ? AND "{COL_MSG_ID}" < ?'
            params = [value, int(local_id)]
        return self._select(is_group, where, params, self._order(is_group), int(limit))

    # -- extras used by the source ---------------------------------------

    def sort_value(self, is_group, row) -> int:
        return self._sort_value(bool(is_group), row)

    def active_peer(self) -> str:
        """The conversation with the most recent message, for diagnostics."""
        rows = self.sessions()
        if not rows:
            return ""
        newest = max(rows, key=lambda item: item[2])
        return newest[0]

    def close(self) -> None:
        self._client.close()


# ---------------------------------------------------------------------------
# Coordinator: the object `QQSource` injects as `local_backend`.
# ---------------------------------------------------------------------------

class Coordinator:
    """Resolve the active account, then hand back a ready ``NtMsgDb``.

    Mirrors the old ``qq_nt_local.Coordinator`` interface (``ensure()``), but
    nothing is decrypted in this process any more: the sidecar owns the
    database handle and this class only decides *which* directory to read.
    """

    def __init__(self, *, passphrase=None, db_dir=None, exe=None, base_url=None,
                 clock=time.monotonic):
        self._passphrase = passphrase if passphrase is not None else NapCatPassphrase()
        self._db_dir = Path(db_dir) if db_dir else None
        self._exe = exe
        self._base_url = base_url
        self._clock = clock
        self._client: ReaderClient | None = None
        self._db: NtMsgDb | None = None
        self._uin: str | None = None
        self._checked = 0.0

    # -- account selection -----------------------------------------------

    @staticmethod
    def _newest_db_dir(dirs) -> Path | None:
        """The store QQ is actually writing to.

        A machine can hold several accounts; the live one is the directory whose
        ``nt_msg.db`` was touched most recently.
        """
        best, best_time = None, -1.0
        for directory in dirs:
            for name in ("nt_msg.db",):
                target = Path(directory) / name
                try:
                    stamp = target.stat().st_mtime
                except OSError:
                    continue
                if stamp > best_time:
                    best, best_time = Path(directory), stamp
        return best

    def _candidates(self) -> list[Path]:
        if self._db_dir is not None:
            return [self._db_dir]
        return nt_db_dirs()

    def _start(self, db_dir: Path) -> NtMsgDb:
        passphrase = self._passphrase.resolve() if hasattr(self._passphrase, "resolve") \
            else str(self._passphrase)
        client = ReaderClient(db_dir, passphrase, exe=self._exe, base_url=self._base_url)
        client.start()
        return NtMsgDb(client)

    # -- contract --------------------------------------------------------

    def ensure(self):
        """``(uin, ntdb)`` for the active account.

        Raises ``LookupError`` when no account store is present and
        ``NtqqReaderError``/``OSError`` when one exists but cannot be read --
        ``QQSource`` maps those onto ``AccountUnavailableError``.
        """
        if self._db is not None:
            return self._uin, self._db
        candidates = self._candidates()
        if not candidates:
            raise LookupError("未找到 NTQQ 数据目录（<QQ号>/nt_qq/nt_db）")
        db_dir = self._newest_db_dir(candidates)
        if db_dir is None:
            raise LookupError("NTQQ 数据目录中没有 nt_msg.db")
        self._uin = db_dir.parent.parent.name
        self._db = self._start(db_dir)
        return self._uin, self._db

    def refresh_interval(self) -> float:
        override = os.environ.get("QQVIBE_NT_REFRESH_SECONDS")
        if override:
            try:
                return max(0.0, float(override))
            except ValueError:
                pass
        return NtMsgDb.REFRESH_SECONDS

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
        self._db = None
        self._client = None

    # -- diagnostics -----------------------------------------------------

    def health(self) -> dict:
        _uin, db = self.ensure()
        client = db._client  # noqa: SLF001 - deliberate: diagnostics only
        return client.health or {}
