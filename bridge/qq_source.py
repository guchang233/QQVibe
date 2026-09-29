"""Read-only QQNT adapter over the ntqq-reader sidecar.

NTQQ keeps chat history in `nt_msg.db`: a SQLCipher database with a 1024-byte
custom header that QQ holds open while it runs. Reading it in Python would mean
reimplementing SQLCipher and digging the passphrase out of QQ's process memory,
so this adapter does neither. `ntdb_unwrap` supplies both halves of the hard
part and lives in sidecar/ntqq-reader; the sidecar exposes the result as a
read-only HTTP SELECT endpoint, and `ntqq_reader` is its client. The passphrase
comes from NapCat, which already has it.

It keeps the interface the former adapter exposed, so Backend and
history_browser consume it unchanged. Design notes:

- Group conversations keep the WeChat group marker suffix (`<gid>@chatroom`)
  so the backend's group logic keys on one convention.
- Fetched messages are materialized into an in-memory SQLite shard that mirrors
  the WeChat message schema (`Msg_<md5(user)>` with local_id/sort_seq/local_type/
  real_sender_id/create_time/message_content/server_id). The stable sort key is
  QQNT's per-conversation `real_seq`; `local_id == sort_seq`, so every composite
  cursor in history_browser reduces to a plain `sort_seq` comparison.
- History walks from the newest message towards older ones, asking the sidecar
  for successively older windows anchored on the (sort_seq, local_id) pair of
  the oldest row materialized so far. `_ConversationState` tracks that anchor
  and latches `exhausted` once a round contributes nothing new.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time as time_module
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from backend_contracts import (
    AccountChangedError, AccountUnavailableError, MAX_IMAGE_BYTES,
    MAX_ISSUED_IMAGES, MessageWindow, MessageWindowBatch,
)
from history_browser import decode_cursor, encode_cursor
from ntqq_reader import Coordinator, parse_message_body

GROUP_SUFFIX = "@chatroom"
SHARD_NAME = "message__message_0.db"
# Account workdir shared with AccountStore's default snapshot root, so account
# registration and the "清除账号" flow address QQ accounts the same way.
WORKDIR_ROOT = Path(tempfile.gettempdir()) / "qqvibe_db"
CONTACTS_TTL = 300.0
HISTORY_PAGE_SIZE = 200
MAX_FETCH_ROUNDS = 64
FULL_HISTORY_ROUNDS = 160
MAX_ROWS_PER_CHAT = 20000
RECENT_CONTACT_COUNT = 100
PREVIEW_LIMIT = 80

KIND_TEXT = "text"
KIND_IMAGE = "image"
KIND_OTHER = "other"

TYPE_NAMES = {1: "文本", 3: "图片", 34: "语音", 43: "视频", 47: "表情", 49: "卡片", 10000: "系统"}


def message_id(account, user, shard, row):
    identity = json.dumps([account, user, shard, row["local_id"], row["sort_seq"],
                           row["server_id"]], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def positive_timestamp(value):
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return (number * 1000 if number < 10_000_000_000 else number) if number > 0 else None


def avatar_candidates(*urls):
    candidates = []
    for url in urls:
        if not isinstance(url, str) or not url or url != url.strip():
            continue
        try:
            parsed = urlsplit(url)
        except ValueError:
            continue
        if (parsed.scheme.lower() in ("http", "https") and parsed.hostname and
                not parsed.username and not parsed.password and url not in candidates):
            candidates.append(url)
    return candidates


def _uin_avatars(uin):
    return avatar_candidates(
        f"https://q1.qlogo.cn/g?b=qq&nk={uin}&s=640",
        f"https://q1.qlogo.cn/g?b=qq&nk={uin}&s=100",
    )


def _group_avatars(gid):
    return avatar_candidates(
        f"https://p.qlogo.cn/gh/{gid}/{gid}/640",
        f"https://p.qlogo.cn/gh/{gid}/{gid}/100",
        f"https://q1.qlogo.cn/g?b=qq&nk={gid}&s=640",
    )


def _table_name(user):
    return "Msg_" + hashlib.md5(user.encode("utf-8")).hexdigest()


def _primary_flag(flags):
    for flag in ("text", "image", "video", "voice", "face", "file", "card"):
        if flag in flags:
            return flag
    return "other"


class _ConversationState:
    def __init__(self):
        self.has_newest = False
        self.anchor_short_id = None
        self.anchor_local = None  # (sort_seq, local_id) composite for the local backend
        self.oldest_seq = None
        self.exhausted = False
        self.use_reverse = False
        self.calibrated = False
        self.last_error_floor = None
        self.qce_page = 0

    def anchored(self):
        return self.anchor_short_id is not None or self.anchor_local is not None


class _QQDB:
    """File-backed shard mirroring the WeChat message schema for one account.

    The SQLite file lives under the account workdir and keeps its message
    tables across restarts. history_browser opens short-lived connections via
    `_msg_conns` and closes them itself, so the internal connection is only
    used for source-internal reads and writes.
    """

    # Bumped to 2 when the read path moved to the sidecar. local_id and sort_seq
    # are now derived from the message id and the group sequence rather than from
    # OneBot's real_seq, so a shard written by the previous materializer holds
    # rows that no longer collide with the new ones and every message would show
    # up twice. The guard in _check_version only fires on a version change, so
    # changing how messages are materialized means bumping this.
    SHARD_VERSION = 2

    def __init__(self, source, workdir=None):
        self.source = source
        self.path = None
        self._main = None
        if workdir is not None:
            self.path = str(Path(workdir) / SHARD_NAME)
            self._main = sqlite3.connect(self.path, check_same_thread=False, timeout=30,
                                        isolation_level=None)
            # WAL keeps history_browser's short-lived read connections from
            # blocking source writes when a browse stops mid-cursor.
            self._main.execute("PRAGMA journal_mode=WAL")
            self._check_version()

    def _check_version(self):
        """Wipe the shard when it was written by a different materializer version."""
        row = self._main.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_shard_meta'"
        ).fetchone()
        version = None
        if row:
            version = self._main.execute(
                "SELECT value FROM _shard_meta WHERE key='version'").fetchone()
        if version is not None and str(version[0]) == str(self.SHARD_VERSION):
            return
        names = [row[0] for row in self._main.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        for name in names:
            self._main.execute(f'DROP TABLE "{name}"')
        self._main.execute("CREATE TABLE _shard_meta (key TEXT PRIMARY KEY, value TEXT)")
        self._main.execute("INSERT OR REPLACE INTO _shard_meta VALUES ('version', ?)",
                           (str(self.SHARD_VERSION),))

    @property
    def account(self):
        return self.source.account

    def close(self):
        if self._main is not None:
            try:
                self._main.close()
            except sqlite3.Error:
                pass
            self._main = None

    def has_table(self, user):
        if self._main is None:
            return False
        name = _table_name(user)
        row = self._main.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
        return row is not None

    def ensure_table(self, user):
        name = _table_name(user)
        self._main.execute(
            f'CREATE TABLE IF NOT EXISTS "{name}" ('
            "local_id INTEGER PRIMARY KEY, local_type INTEGER NOT NULL, "
            "real_sender_id INTEGER NOT NULL DEFAULT 0, create_time INTEGER NOT NULL DEFAULT 0, "
            "message_content TEXT, compress_content BLOB, server_id INTEGER NOT NULL DEFAULT 0, "
            "sort_seq INTEGER NOT NULL DEFAULT 0)"
        )
        return name

    def register_sender(self, uin):
        """Keep Name2Id populated; history_browser resolves senders through it.

        rowid must equal the numeric uin so rowid-keyed sender lookups match
        the stored real_sender_id column.
        """
        if self._main is None or not str(uin or "").isdigit():
            return
        try:
            self._main.execute("CREATE TABLE IF NOT EXISTS Name2Id (user_name TEXT)")
            self._main.execute("INSERT OR IGNORE INTO Name2Id(rowid, user_name) VALUES (?, ?)",
                               (int(uin), str(uin)))
        except sqlite3.Error:
            pass

    def insert(self, user, row):
        name = self.ensure_table(user)
        cursor = self._main.execute(
            f'INSERT OR IGNORE INTO "{name}" (local_id, local_type, real_sender_id, create_time,'
            "message_content, compress_content, server_id, sort_seq) VALUES (?,?,?,?,?,?,?,?)",
            (row["local_id"], row["local_type"], row["real_sender_id"], row["create_time"],
             row["message_content"], row["compress_content"], row["server_id"], row["sort_seq"]))
        if cursor.rowcount:
            self._trim(name)
        return bool(cursor.rowcount)

    def _trim(self, name):
        count = self._main.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        if count <= MAX_ROWS_PER_CHAT:
            return
        cutoff = self._main.execute(
            f'SELECT local_id FROM "{name}" ORDER BY local_id ASC LIMIT 1 OFFSET ?',
            (count - MAX_ROWS_PER_CHAT,)).fetchone()
        if cutoff:
            self._main.execute(f'DELETE FROM "{name}" WHERE local_id <= ?', (cutoff[0],))

    _SELECT = ("SELECT local_id,local_type,real_sender_id,create_time,message_content,"
               "compress_content,server_id,sort_seq FROM ")

    def rows_desc(self, user, limit, offset=0):
        if not self.has_table(user):
            return []
        name = _table_name(user)
        return self._main.execute(
            self._SELECT + f'"{name}" ORDER BY sort_seq DESC,local_id DESC LIMIT ? OFFSET ?',
            (limit, offset)).fetchall()

    def rows_range_desc(self, user, high_seq, after_seq=None, limit=1):
        if not self.has_table(user):
            return []
        name = _table_name(user)
        if after_seq is None:
            clause, args = "sort_seq <= ?", (high_seq,)
        else:
            clause, args = "sort_seq <= ? AND sort_seq > ?", (high_seq, after_seq)
        return self._main.execute(
            self._SELECT + f'"{name}" WHERE {clause} '
            "ORDER BY sort_seq DESC,local_id DESC LIMIT ?", (*args, limit)).fetchall()

    def rows_range_asc(self, user, high_seq, after_seq=None, limit=1):
        """Oldest-first page inside the range — the shape history walks expect."""
        if not self.has_table(user):
            return []
        name = _table_name(user)
        if after_seq is None:
            clause, args = "sort_seq <= ?", (high_seq,)
        else:
            clause, args = "sort_seq <= ? AND sort_seq > ?", (high_seq, after_seq)
        return self._main.execute(
            self._SELECT + f'"{name}" WHERE {clause} '
            "ORDER BY sort_seq ASC,local_id ASC LIMIT ?", (*args, limit)).fetchall()

    def rows_before(self, user, seq, limit):
        if not self.has_table(user):
            return []
        name = _table_name(user)
        return self._main.execute(
            self._SELECT + f'"{name}" WHERE sort_seq < ? '
            "ORDER BY sort_seq DESC,local_id DESC LIMIT ?", (seq, limit)).fetchall()

    def _msg_conns(self, user):
        """Fresh (conn, table) pairs; the caller owns and closes each connection."""
        if self._main is None or not self.has_table(user):
            return []
        self.ensure_table(user)
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30,
                               isolation_level=None)
        return [(conn, _table_name(user))]

    def message_row(self, user, seq):
        if not self.has_table(user):
            return None
        name = _table_name(user)
        return self._main.execute(
            self._SELECT + f'"{name}" WHERE local_id=? AND sort_seq=?', (seq, seq)).fetchone()

    def sender_counts(self, user):
        if not self.has_table(user):
            return {}
        name = _table_name(user)
        return {str(row[0]): int(row[1]) for row in self._main.execute(
            f'SELECT real_sender_id,COUNT(*) FROM "{name}" GROUP BY real_sender_id')}

    def text_counts(self, user):
        if not self.has_table(user):
            return {}
        name = _table_name(user)
        return {str(row[0]): int(row[1]) for row in self._main.execute(
            f'SELECT real_sender_id,COUNT(*) FROM "{name}" WHERE local_type=1 AND '
            "TRIM(COALESCE(message_content,''))<>'' GROUP BY real_sender_id")}

    def total_count(self, user):
        if not self.has_table(user):
            return 0
        name = _table_name(user)
        return self._main.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]

    def text_count(self, user):
        if not self.has_table(user):
            return 0
        name = _table_name(user)
        return self._main.execute(
            f'SELECT COUNT(*) FROM "{name}" WHERE local_type=1 AND '
            "TRIM(COALESCE(message_content,''))<>''").fetchone()[0]

    def member_count(self, user, uin):
        if not self.has_table(user):
            return 0
        name = _table_name(user)
        return self._main.execute(
            f'SELECT COUNT(*) FROM "{name}" WHERE real_sender_id=?', (int(uin),)).fetchone()[0]

    def last_key(self, user):
        if not self.has_table(user):
            return None
        name = _table_name(user)
        row = self._main.execute(
            f'SELECT sort_seq,local_id FROM "{name}" ORDER BY sort_seq DESC,local_id DESC LIMIT 1'
        ).fetchone()
        return (int(row[0]), SHARD_NAME, int(row[1])) if row else None


class QQSource:
    def __init__(self, local_backend=None):
        # `local_backend` is injectable so tests can substitute a fake; the default
        # is the real sidecar client.
        self.lock = threading.RLock()
        self.closed = False
        self.db = _QQDB(self)
        self._account = None
        self._nickname = None
        self._contact_index = {}
        self._contacts_at = 0.0
        self._group_members_loaded = set()
        self._conversations = {}
        self._request_active = threading.local()
        self.issued_images = OrderedDict()
        self.window_images = {}
        self._image_urls = {}
        self.media_reason = threading.local()
        self._local = local_backend or Coordinator()

    # -- identity ----------------------------------------------------------

    @property
    def account(self):
        return self._account

    def verified_identity(self, *, messages=False):
        """Resolve the account QQ currently has open, via the sidecar.

        The account is derived from the nt_msg.db that QQ itself holds open, so
        switching the logged-in account surfaces as AccountChangedError rather
        than as history read from the wrong account.
        """
        with self.lock:
            if self.closed:
                raise AccountUnavailableError()
            try:
                uin, _ntdb = self._local.ensure()
            except LookupError as exc:
                self._release_account()
                raise AccountUnavailableError(f"当前QQ账号未就绪（{exc}）") from exc
            except (OSError, ValueError) as exc:
                self._release_account()
                raise AccountUnavailableError(
                    f"当前QQ账号未就绪（nt_msg.db 读取失败: {exc}）") from exc
            if self._account is not None and self._account != str(uin):
                self._release_account()
                raise AccountChangedError()
            workdir = WORKDIR_ROOT / str(uin)
            try:
                workdir.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise AccountUnavailableError() from exc
            if self.db.path != str(workdir / SHARD_NAME):
                old = self.db
                self.db = _QQDB(self, workdir)
                old.close()
            self._account = str(uin)
            self._nickname = str(uin)
            return str(uin), workdir.resolve()

    def identity(self):
        return self.verified_identity()

    def _release_account(self):
        self._account = None
        self._nickname = None
        self._contact_index = {}
        self._contacts_at = 0.0
        self._group_members_loaded.clear()
        self._conversations.clear()
        self._image_urls.clear()
        self.issued_images.clear()
        self.window_images.clear()
        old = self.db
        self.db = _QQDB(self)
        old.close()

    def self_user(self, db=None):
        with self.lock:
            if self._account is None:
                self.verified_identity()
            return self._account

    def require_messages_ready(self):
        self.verified_identity(messages=True)

    def request_scope(self):
        @contextmanager
        def scope():
            previous = getattr(self._request_active, "active", False)
            self._request_active.active = True
            try:
                yield
            finally:
                self._request_active.active = previous
        return scope()

    def forget_account(self, account):
        """Drop one account's reader and cached message copies (account deletion)."""
        with self.lock:
            target = self._account if str(account) == self._account else str(account)
            if self._account is None or str(account) == self._account:
                self._release_account()
            if target:
                shutil.rmtree(WORKDIR_ROOT / str(target), ignore_errors=True)

    def close(self):
        with self.lock:
            self.closed = True
            self._release_account()

    # -- contacts ----------------------------------------------------------

    def _contact_display(self, user):
        entry = self._contact_index.get(user)
        if entry:
            return dict(entry)
        is_group = user.endswith(GROUP_SUFFIX)
        peer = user[:-len(GROUP_SUFFIX)] if is_group else user
        candidates = _group_avatars(peer) if is_group else _uin_avatars(peer)
        name = None
        if not is_group:
            try:
                _uin, ntdb = self._local.ensure()
                name = ntdb.sender_display(peer, False)
            except (LookupError, OSError, ValueError):
                name = None
        return {"name": name or peer, "avatar": candidates[0] if candidates else "",
                "avatarCandidates": candidates}

    # -- sessions ----------------------------------------------------------

    def sessions(self):
        with self.lock:
            self.verified_identity(messages=True)
            return self._sessions_local()

    def contact(self, user):
        with self.lock:
            self.verified_identity()
            return self._contact_display(user)

    # -- history fetching ----------------------------------------------------

    def _sessions_local(self):
        """Conversation directory from the decrypted nt_msg.db message tables."""
        uin, ntdb = self._local.ensure()
        own_candidates = _uin_avatars(uin)
        own = {"username": uin, "name": uin, "avatar": own_candidates[0] if own_candidates else "",
               "avatarCandidates": own_candidates}
        items = []
        for peer, is_group, last_time, _count in ntdb.sessions():
            user = peer + GROUP_SUFFIX if is_group else peer
            display = self._contact_index.get(user)
            if not display:
                name = None if is_group else ntdb.sender_display(peer, False)
                candidates = _group_avatars(peer) if is_group else _uin_avatars(peer)
                display = {"name": name or peer,
                           "avatar": candidates[0] if candidates else "",
                           "avatarCandidates": candidates}
                self._contact_index[user] = display
            preview = "[消息]"
            rows = ntdb.newest_rows(peer, is_group, 1)
            if rows:
                item = self._nt_item(user, is_group, rows[0])
                if item and item["message_content"]:
                    preview = item["message_content"][:PREVIEW_LIMIT]
            items.append({"username": user, **display, "preview": preview,
                          "time": positive_timestamp(last_time), "sortTimestamp": positive_timestamp(last_time),
                          "unreadCount": 0, "lastMsgType": None, "lastMsgSubType": None,
                          "pinned": None, "lastSender": "", "isGroup": is_group})
        items.sort(key=lambda item: item["sortTimestamp"] or 0, reverse=True)
        return {"self": own, "sessions": items, "account": uin, "messagesReady": True}

    def _nt_item(self, user, is_group, row):
        """nt_msg.db row tuple -> the normalized item shape _store_row expects."""
        msg_id, group_seq, direction, sender, seconds, card, nick, content = row
        if not msg_id:
            return None
        flag, text, image_url, system = parse_message_body(content or b"")
        if system or direction == 3:
            local_type, content_text = 10000, ""
        else:
            local_type = {"text": 1, "image": 3, "video": 43, "voice": 34,
                          "face": 47, "card": 49}.get(flag, 49)
            if not text:
                text = {"1": "文本", "3": "图片", "43": "视频", "34": "语音",
                        "47": "表情", "49": "卡片"}.get(str(local_type), "消息")
                text = f"[{text}]"
            content_text = text
        sender_uin = str(sender or "")
        sender_name = str(card or nick or "") if is_group else str(nick or "")
        sort_seq = int(group_seq) if is_group and group_seq else int(msg_id)
        return {"local_id": int(msg_id), "sort_seq": sort_seq, "server_id": 0,
                "real_sender_id": int(sender_uin) if sender_uin.isdigit() else 0,
                "create_time": int(seconds or 0), "local_type": local_type,
                "message_content": content_text, "sender_uin": sender_uin,
                "sender_name": sender_name, "time": int(seconds or 0),
                "image_urls": [image_url] if image_url else []}

    def _fetch_page_local(self, user, conv, count):
        """One keyset page straight out of the decrypted nt_msg.db (newest first)."""
        is_group = user.endswith(GROUP_SUFFIX)
        peer = user[:-len(GROUP_SUFFIX)] if is_group else user
        _uin, ntdb = self._local.ensure()
        newest_first = conv.anchor_local is None
        if newest_first:
            rows = ntdb.newest_rows(peer, is_group, count)
        else:
            rows = ntdb.older_rows(peer, is_group, conv.anchor_local[0],
                                   conv.anchor_local[1], count)
        if not rows:
            conv.has_newest = True
            conv.exhausted = True
            return 0
        inserted = 0
        for row in rows:
            item = self._nt_item(user, is_group, row)
            if item and self._store_row(user, item):
                inserted += 1
        conv.has_newest = True
        oldest = rows[-1]
        sort_seq = int(oldest[1]) if is_group and oldest[1] else int(oldest[0])
        anchor = (sort_seq, int(oldest[0]))
        if conv.anchor_local is None or anchor < conv.anchor_local:
            conv.anchor_local = anchor
            conv.oldest_seq = anchor[0]
            return inserted
        conv.exhausted = True
        return inserted

    def _conv(self, user):
        state = self._conversations.get(user)
        if state is None:
            state = _ConversationState()
            self._conversations[user] = state
        return state

    def _fetch_page(self, user, conv, count):
        """Fetch one page, materialize it, and advance the anchor. Returns new rows."""
        return self._fetch_page_local(user, conv, count)

    def _store_row(self, user, item):
        sender = item["sender_uin"]
        if sender and sender not in self._contact_index and item["sender_name"]:
            candidates = _uin_avatars(sender)
            self._contact_index[sender] = {"name": item["sender_name"],
                                           "avatar": candidates[0] if candidates else "",
                                           "avatarCandidates": candidates}
        self.db.register_sender(sender)
        self.db.register_sender(self._account)
        row = {key: item[key] for key in ("local_id", "local_type", "real_sender_id",
                                          "create_time", "message_content", "server_id",
                                          "sort_seq")}
        row["compress_content"] = None
        inserted = self.db.insert(user, row)
        if inserted and item["image_urls"]:
            self._image_urls[(self._account, user, item["local_id"])] = item["image_urls"][0]
        return inserted

    def _ensure_newest(self, user):
        conv = self._conv(user)
        if conv.has_newest:
            return
        self._fetch_page(user, conv, HISTORY_PAGE_SIZE)
        conv.has_newest = True

    def _ensure_older(self, user, after_seq, need):
        """Materialize rows above `after_seq` until `need` exist or history ends."""
        conv = self._conv(user)
        rounds = 0
        while not conv.exhausted and rounds < MAX_FETCH_ROUNDS:
            have = len(self.db.rows_range_desc(user, 1 << 62, after_seq, need))
            if have >= need:
                return
            if not conv.anchored():
                self._ensure_newest(user)
                if not conv.anchored():
                    return
            self._fetch_page(user, conv, HISTORY_PAGE_SIZE)
            rounds += 1

    def _ensure_before(self, user, seq, need):
        """Materialize rows below `seq` until `need` exist or history ends."""
        conv = self._conv(user)
        rounds = 0
        while not conv.exhausted and rounds < MAX_FETCH_ROUNDS:
            if len(self.db.rows_before(user, seq, need)) >= need:
                return
            if not conv.anchored():
                self._ensure_newest(user)
                if not conv.anchored():
                    return
            self._fetch_page(user, conv, HISTORY_PAGE_SIZE)
            rounds += 1

    def prepare_browse(self, user, *, before=None, around=None):
        """Make rows around a UI history cursor exist before history_browser runs."""
        with self.lock:
            self.verified_identity(messages=True)
            for cursor in (before, around):
                if not cursor:
                    continue
                try:
                    seq, _shard, _local = decode_cursor(cursor, self._account or "", user)
                except ValueError:
                    continue
                self._ensure_before(user, int(seq), 200)
                self._ensure_newest(user)

    # -- rendering ------------------------------------------------------------

    def _db(self, fresh=False):
        return self.db

    def _contacts(self, db=None):
        """Contact display map, in the shape history_browser expects."""
        return self._contacts_map()

    def _contacts_map(self):
        return {user: dict(entry) for user, entry in self._contact_index.items()}

    def _render_record(self, db, user, record, contacts, own_user):
        local_id, local_type, sender_id, created, content, _compressed, server_id, seq = record
        if local_type == 10000:
            return None
        kind_name = TYPE_NAMES.get(local_type, "消息")
        sender = str(sender_id) if sender_id else user
        side = "self" if sender and sender == own_user else "other"
        contact = (contacts.get(sender) or self._contact_index.get(sender) or
                   {"name": sender, "avatar": "", "avatarCandidates": []})
        timestamp = int(created or 0)
        time_ms = timestamp * 1000 if 0 < timestamp < 10_000_000_000 else timestamp
        stable_id = message_id(self._account or "", user, SHARD_NAME,
                               {"local_id": local_id, "sort_seq": seq, "server_id": server_id})
        if local_type == 3:
            url = self._image_urls.get((self._account, user, local_id))
            if url:
                self.issued_images[(self._account, user, stable_id)] = (int(seq), SHARD_NAME,
                                                                        int(local_id), url)
                self.issued_images.move_to_end((self._account, user, stable_id))
                while len(self.issued_images) > MAX_ISSUED_IMAGES:
                    self.issued_images.popitem(last=False)
        return {"id": stable_id,
                "historyCursor": encode_cursor(self._account or "", user,
                                               (int(seq), SHARD_NAME, int(local_id))),
                "side": side, "text": content or f"[{kind_name}]",
                "kind": KIND_TEXT if local_type == 1 else
                        KIND_IMAGE if local_type == 3 else KIND_OTHER,
                "time": time_ms, "type": kind_name, "senderId": sender,
                "senderName": contact.get("name", sender),
                "senderAvatar": contact.get("avatar", ""),
                "senderAvatarCandidates": contact.get("avatarCandidates", []),
                "_sort": [int(seq), SHARD_NAME, int(local_id)]}

    def _render_row(self, db, user, item, contacts, own_user):
        record, _shard, _senders = item
        return self._render_record(db, user, record, contacts, own_user)

    # -- message windows --------------------------------------------------------

    def messages(self, user, limit, offset=0):
        with self.lock:
            self.verified_identity(messages=True)
            if limit <= 0:
                return MessageWindow([], False)
            self._ensure_newest(user)
            need = limit + offset + 1
            if len(self.db.rows_desc(user, need)) < need:
                self._ensure_older(user, 0, need)
            rows = self.db.rows_desc(user, need)
            has_more = len(rows) > offset + limit
            contacts = self._contacts_map()
            own_user = self._account or ""
            rendered = []
            for record in reversed(rows[offset:offset + limit]):
                message = self._render_record(self.db, user, record, contacts, own_user)
                if message:
                    rendered.append(message)
            return MessageWindow(rendered, has_more)

    def message_windows(self, users, limit=80, expected_account=None):
        with self.lock:
            account, _workdir = self.verified_identity(messages=True)
            if expected_account is not None and account != expected_account:
                raise AccountChangedError()
            windows, has_more = {}, {}
            for user in users:
                window = self.messages(user, limit)
                windows[user] = list(window)
                has_more[user] = bool(window.has_more_before)
            return MessageWindowBatch(windows, has_more)

    # -- incremental/history walking -----------------------------------------------

    def history_highwater(self, user):
        with self.lock:
            self.verified_identity(messages=True)
            self._ensure_newest(user)
            return self.db.last_key(user)

    def history_page(self, user, highwater, after=None, page_size=256):
        with self.lock:
            self.verified_identity(messages=True)
            if highwater is None:
                return [], None
            high_seq = int(highwater[0])
            after_seq = None if after is None else int(after[0])
            if after_seq is not None and after_seq >= high_seq:
                return [], None
            if after_seq is None:
                # Walk start: the page must begin at the oldest message in range,
                # which only exists once the whole history is materialized.
                self._materialize_all(user)
            else:
                rows = self.db.rows_range_asc(user, high_seq, after_seq, page_size)
                if len(rows) < page_size:
                    self._ensure_older(user, after_seq, page_size)
            rows = self.db.rows_range_asc(user, high_seq, after_seq, page_size)
            if not rows:
                return [], None
            last = rows[-1]
            next_after = (int(last[7]), SHARD_NAME, int(last[0]))
            contacts = self._contacts_map()
            own_user = self._account or ""
            messages = [self._render_record(self.db, user, record, contacts, own_user)
                        for record in rows]
            return [message for message in messages if message], next_after

    def _materialize_all(self, user):
        """Fetch the whole reachable history once, before an ascending walk."""
        conv = self._conv(user)
        self._ensure_newest(user)
        rounds = 0
        while not conv.exhausted and rounds < FULL_HISTORY_ROUNDS:
            if not conv.anchored():
                return
            self._fetch_page(user, conv, HISTORY_PAGE_SIZE)
            rounds += 1

    def quoted_history_page(self, user, ceiling, after=None, page_size=64, member=None):
        """Text rows inside an already-consumed history prefix (QQ has no 49/57 split)."""
        with self.lock:
            self.verified_identity(messages=True)
            if ceiling is None:
                return [], None
            ceiling_seq = int(ceiling[0])
            after_seq = None if after is None else int(after[0])
            rows = self.db.rows_range_asc(user, ceiling_seq, after_seq, page_size * 4)
            if len(rows) < page_size and not self._conv(user).exhausted:
                if after_seq is None:
                    self._materialize_all(user)
                else:
                    self._ensure_older(user, after_seq, page_size)
                rows = self.db.rows_range_asc(user, ceiling_seq, after_seq, page_size * 4)
            if not rows:
                return [], None
            last = rows[-1]
            next_after = (int(last[7]), SHARD_NAME, int(last[0]))
            contacts = self._contacts_map()
            own_user = self._account or ""
            rendered = []
            for record in rows:
                if member is not None and str(record[2]) != member:
                    continue
                message = self._render_record(self.db, user, record, contacts, own_user)
                if message and message["kind"] == "text" and message["text"].strip():
                    rendered.append(message)
            return rendered[:page_size], next_after

    def preceding_text_context(self, user, before, limit=3):
        with self.lock:
            self.verified_identity(messages=True)
            seq = int(before[0])
            contacts = self._contacts_map()
            own_user = self._account or ""
            rounds = 0
            while True:
                candidates = []
                for record in self.db.rows_before(user, seq, limit * 8):
                    message = self._render_record(self.db, user, record, contacts, own_user)
                    if message and message["kind"] == "text" and message["text"].strip():
                        candidates.append(message)
                    if len(candidates) >= limit:
                        break
                if candidates:
                    return [{"id": item["id"], "side": item["side"], "text": item["text"]}
                            for item in candidates[:limit]]
                if rounds >= MAX_FETCH_ROUNDS or self._conv(user).exhausted:
                    return []
                self._ensure_before(user, seq, limit + 1)
                rounds += 1

    def texts_for_refs(self, user, refs, with_ids=False):
        with self.lock:
            self.verified_identity(messages=True)
            texts = []
            for shard, local_id, stable_id in refs:
                if shard != SHARD_NAME:
                    continue
                record = self.db.message_row(user, int(local_id))
                if record is None:
                    continue
                expected = message_id(self._account or "", user, SHARD_NAME,
                                      {"local_id": record[0], "sort_seq": record[7],
                                       "server_id": record[6]})
                if stable_id and stable_id != expected:
                    continue
                rendered = self._render_record(self.db, user, record, self._contacts_map(),
                                               self._account or "")
                if rendered and rendered["kind"] == "text" and rendered["text"].strip():
                    texts.append((expected, rendered["text"]) if with_ids else rendered["text"])
            return texts

    # -- profile/stat reads ----------------------------------------------------------

    def stats(self, user, member=None):
        with self.lock:
            self.verified_identity(messages=True)
            self._materialize_all(user)
            if member is not None:
                count = self.db.member_count(user, member)
                text_count = self.db.text_counts(user).get(member, 0)
            else:
                count = self.db.total_count(user)
                text_count = self.db.text_count(user)
            contacts = self._contacts_map()
            return count, text_count, [{"id": sender, **(contacts.get(sender) or {
                "name": sender, "avatar": "", "avatarCandidates": []})}
                for sender in sorted(self.db.sender_counts(user))]

    def profile_metadata(self, user, member=None):
        with self.lock:
            self.verified_identity(messages=True)
            group = user.endswith(GROUP_SUFFIX)
            self._materialize_all(user)
            counts = self.db.sender_counts(user)
            if member and (not group or member not in counts):
                raise ValueError("unknown member")
            text_counts = self.db.text_counts(user)
            subject = member if group else user
            contacts = self._contacts_map()
            return {"contact": contacts.get(subject) or self._contact_display(subject),
                    "members": [{"id": sender, **(contacts.get(sender) or {
                        "name": sender, "avatar": "", "avatarCandidates": []})}
                        for sender in sorted(counts)] if group else [],
                    "count": counts.get(subject, 0) if subject else 0,
                    "textCount": text_counts.get(subject, 0) if subject else 0}

    def profile_overview(self, user, member=None, highwater=None):
        with self.lock:
            self.verified_identity(messages=True)
            group = user.endswith(GROUP_SUFFIX)
            self._materialize_all(user)
            counts = self.db.sender_counts(user)
            if member and (not group or member not in counts):
                raise ValueError("unknown member")
            contacts = self._contacts_map()
            subject = member or user
            return {"contact": contacts.get(subject) or self._contact_display(subject),
                    "members": [{"id": sender, **(contacts.get(sender) or {
                        "name": sender, "avatar": "", "avatarCandidates": []})}
                        for sender in sorted(counts)] if group else [],
                    "count": counts.get(member, 0) if member else
                             (self.db.total_count(user) if group else counts.get(user, 0)),
                    "textCount": None}

    # -- media ------------------------------------------------------------------------

    def media(self, user, stable_id):
        self.require_messages_ready()
        self.media_reason.value = None

        def unavailable(reason):
            self.media_reason.value = reason
            return None
        if not isinstance(stable_id, str) or len(stable_id) != 64:
            return unavailable("invalid-id")
        with self.lock:
            issued = self.issued_images.get((self._account, user, stable_id))
        if not issued:
            return unavailable("not-issued")
        url = issued[-1]
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            return unavailable("url-unavailable")
        try:
            request = Request(url, headers={"User-Agent": "QQVibe/1.0"})
            with urlopen(request, timeout=20) as response:
                data = response.read(MAX_IMAGE_BYTES + 1)
        except OSError:
            return unavailable("fetch-failed")
        if len(data) > MAX_IMAGE_BYTES:
            return unavailable("image-too-large")
        if data.startswith(b"\xff\xd8\xff"):
            return data, "image/jpeg"
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            return data, "image/png"
        if data.startswith((b"GIF87a", b"GIF89a")):
            return data, "image/gif"
        if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
            return data, "image/webp"
        return unavailable("unsupported-format")
