"""Read-only QQ adapter: account identity, sessions, messages, and media.

This is the QQ counterpart of the former QQ source and the only module that
talks to `qq_client.QqClient`. Chat history comes from the injected QQNT reader
(see `qqnt_install.py`); nothing here sends, recalls or mutates a message.

No inference scheduling or analysis-cache writes belong in this module.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import qq_client
from backend_contracts import (
    AccountChangedError, AccountUnavailableError, MAX_IMAGE_BYTES, MAX_ISSUED_IMAGES,
    MessageWindow, MessageWindowBatch, MessagesUnavailableError, PROFILE_METADATA_CACHE_LIMIT,
    ROOT, SYSTEM_NAMES,
)
from history_browser import encode_cursor


GROUP_SUFFIX = "@chatroom"
STABLE_ID = re.compile(r"[0-9a-f]{64}\Z")
# QQNT has no message shard files; every position carries this constant label so
# the backend's (time, shard, seq) ordering keeps working unchanged.
SHARD = "qq"
DIRECTORY_TTL_SECONDS = 30.0
PROFILE_OVERVIEW_TTL_SECONDS = 60.0
IMAGE_MIMES = ("image/jpeg", "image/png", "image/gif", "image/webp")


def split_user(user):
    """Return ``(peer, is_group)`` for one backend conversation identifier."""
    if isinstance(user, str) and user.endswith(GROUP_SUFFIX):
        return user[: -len(GROUP_SUFFIX)], True
    return user, False


def positive_timestamp(value):
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return (number * 1000 if number < 10_000_000_000 else number) if number > 0 else None


def session_preview(summary, message_type=None, sub_type=None):
    content = summary.strip() if isinstance(summary, str) else ""
    return content or "[消息]"


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


def contact_display(contacts, user):
    contact = contacts.get(user)
    if contact:
        return {**contact,
                "name": SYSTEM_NAMES.get(user, user) if contact["name"] == user else contact["name"]}
    return {"name": SYSTEM_NAMES.get(user, user), "avatar": "", "avatarCandidates": []}


def stable_message_id(account, user, msg_id):
    """Stable 64-hex id shared with the reader's `stableId` helper."""
    identity = json.dumps([account or "", user, str(msg_id)],
                          ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


# Name kept for the compatibility facade in `real_backend`.
message_id = stable_message_id


def _workdir(account):
    # Account-scoped app cache directory. `account_store` expects exactly
    # `<snapshot_root>/<account>`, and AccountAPI points that root at
    # `.local/qqnt/accounts`, so the two layout assumptions stay identical.
    return str((ROOT / ".local" / "qqnt" / "accounts" / account).resolve())


class QQSource:
    """Duck-typed data source over the injected QQNT reader."""

    def __init__(self, client=None, ensure=None):
        self.client = client or qq_client.QqClient(ensure=ensure)
        self.lock = threading.RLock()
        self.closed = False
        # The live account is resolved on every read; there is no snapshot to pin.
        self.dynamic_account = False
        self.active_account_locator = None
        # Truthy sentinel: the live QQ reader is always the message source, so the
        # backend's "is there a real source?" probe passes even before first read.
        self.db = {"account": None}
        self._self = None
        self._contacts = None
        self._contacts_at = 0.0
        self._members = {}
        self.issued_images = OrderedDict()
        self.window_images = {}
        self.profile_metadata_cache = OrderedDict()
        self.profile_overview_counts_cache = OrderedDict()
        self.media_reason = threading.local()

    # --- lifecycle -------------------------------------------------------------
    @contextmanager
    def request_scope(self):
        """Kept for the HTTP layer's reader-scope protocol."""
        yield

    def close(self):
        with self.lock:
            self.closed = True
            self._self = None
            self._contacts = None
            self._members.clear()
            self.issued_images.clear()
            self.window_images.clear()
            self.profile_metadata_cache.clear()
            self.profile_overview_counts_cache.clear()
            self.client.reset()

    def forget_account(self, account):
        with self.lock:
            for key in list(self.issued_images):
                if key[0] == account:
                    del self.issued_images[key]
            for key in list(self.window_images):
                if key[0] == account:
                    del self.window_images[key]
            for key in list(self.profile_metadata_cache):
                if key[0] == account:
                    del self.profile_metadata_cache[key]
            for key in list(self.profile_overview_counts_cache):
                if key[0] == account:
                    del self.profile_overview_counts_cache[key]
            self._members.clear()
            self.client.reset()

    # --- identity --------------------------------------------------------------
    def _account(self, *, messages):
        with self.lock:
            if self.closed:
                raise AccountUnavailableError()
            try:
                info = self.client.self_info()
            except (qq_client.ReaderUnavailable, qq_client.ReaderError) as exc:
                if messages:
                    raise MessagesUnavailableError() from exc
                raise AccountUnavailableError() from exc
            account = str(info.get("account") or "")
            if not account:
                if messages:
                    raise MessagesUnavailableError()
                raise AccountUnavailableError()
            self._self = info
            self.db = {"account": account}
            return account

    def identity(self):
        account = self._account(messages=False)
        return account, _workdir(account)

    def verified_identity(self, *, messages=False):
        account = self._account(messages=messages)
        return account, _workdir(account)

    def require_messages_ready(self):
        self._account(messages=True)

    def self_user(self, db=None):
        with self.lock:
            account = self._account(messages=False)
            info = self._self or {}
            return str(info.get("uid") or info.get("uin") or account)

    # --- directory -------------------------------------------------------------
    def _contacts_map(self, force=False):
        now = time.monotonic()
        if not force and self._contacts is not None and now - self._contacts_at < DIRECTORY_TTL_SECONDS:
            return self._contacts
        try:
            data = self.client.contacts()
        except (qq_client.ReaderUnavailable, qq_client.ReaderError):
            data = {}
        contacts = {}
        for entry in list(data.get("friends") or []) + list(data.get("groups") or []):
            identifier = str(entry.get("id") or "")
            if not identifier:
                continue
            candidates = avatar_candidates(entry.get("avatar"))
            contacts[identifier] = {"name": str(entry.get("name") or identifier),
                                    "avatar": candidates[0] if candidates else "",
                                    "avatarCandidates": candidates}
        self._contacts = contacts
        self._contacts_at = now
        return contacts

    def _member_map(self, user):
        peer, is_group = split_user(user)
        if not is_group:
            return {}
        try:
            rows = self.client.senders(peer, is_group=True)
        except (qq_client.ReaderUnavailable, qq_client.ReaderError):
            return {}
        members = {}
        for row in rows or []:
            identifier = str(row.get("id") or "")
            if not identifier:
                continue
            candidates = avatar_candidates(row.get("avatar"))
            members[identifier] = {"name": str(row.get("name") or identifier),
                                   "avatar": candidates[0] if candidates else "",
                                   "avatarCandidates": candidates}
        self._members.update(members)
        return members

    def _display(self, target, contacts, members):
        return members.get(target) or contact_display(contacts, target)

    def contact(self, user):
        with self.lock:
            self._account(messages=False)
            contacts = self._contacts_map()
            if user in contacts or user in self._members:
                return contacts.get(user) or self._members[user]
            return contact_display(contacts, user)

    # --- sessions --------------------------------------------------------------
    def sessions(self):
        with self.lock:
            account = self._account(messages=False)
            try:
                data = self.client.sessions()
            except (qq_client.ReaderUnavailable, qq_client.ReaderError) as exc:
                raise AccountUnavailableError() from exc
            if str(data.get("account") or "") != account:
                raise AccountChangedError()
            own = data.get("self") or {}
            items = []
            for raw in data.get("sessions") or []:
                username = str(raw.get("username") or "")
                if not username:
                    continue
                timestamp = positive_timestamp(raw.get("time"))
                items.append({
                    "username": username,
                    "name": str(raw.get("name") or username),
                    "avatar": str(raw.get("avatar") or ""),
                    "avatarCandidates": avatar_candidates(raw.get("avatar")),
                    "preview": session_preview(raw.get("preview")),
                    "time": timestamp,
                    "sortTimestamp": positive_timestamp(raw.get("sortTimestamp")) or timestamp,
                    "unreadCount": max(0, int(raw.get("unreadCount") or 0)),
                    "lastMsgType": None, "lastMsgSubType": None,
                    "pinned": bool(raw.get("pinned")),
                    "lastSender": str(raw.get("lastSender") or ""),
                    "isGroup": bool(raw.get("isGroup")),
                })
            return {"self": {"username": str(own.get("username") or account),
                             "name": str(own.get("name") or ""),
                             "avatar": str(own.get("avatar") or ""),
                             "avatarCandidates": avatar_candidates(own.get("avatar"))},
                    "sessions": items, "account": account, "messagesReady": True}

    # --- message rendering -----------------------------------------------------
    def _render(self, raw, user, account):
        if not isinstance(raw, dict):
            return None, None
        msg_id = str(raw.get("msgId") or "")
        if not msg_id:
            return None, None
        sort = raw.get("sort")
        if not isinstance(sort, list) or len(sort) != 3:
            sort = [int(raw.get("msgTime") or 0), SHARD, int(raw.get("msgSeq") or 0)]
        sort = [int(sort[0]), str(sort[1]), int(sort[2])]
        avatar = str(raw.get("avatar") or "")
        message = {
            "id": stable_message_id(account, user, msg_id),
            "historyCursor": encode_cursor(account, user, (sort[0], sort[1], sort[2])),
            "side": "self" if raw.get("side") == "self" else "other",
            "text": raw.get("text") if isinstance(raw.get("text"), str) else "",
            "kind": str(raw.get("kind") or "other"),
            "time": sort[0],
            "type": str(raw.get("type") or "unknown"),
            "senderId": str(raw.get("senderId") or ""),
            "senderName": str(raw.get("senderName") or ""),
            "senderAvatar": avatar,
            "senderAvatarCandidates": [avatar] if avatar else [],
            "_sort": sort,
        }
        return message, (raw.get("image") or None)

    def _window(self, user, raw_messages):
        account = str((self.db or {}).get("account") or "")
        rendered, images = [], {}
        for raw in raw_messages or []:
            message, ref = self._render(raw, user, account)
            if message is None:
                continue
            rendered.append(message)
            if ref:
                images[message["id"]] = ref
        return rendered, images

    def _issue(self, account, user, images):
        for identifier, ref in images.items():
            key = (account, user, identifier)
            self.issued_images[key] = ref
            self.issued_images.move_to_end(key)
            while len(self.issued_images) > MAX_ISSUED_IMAGES:
                self.issued_images.popitem(last=False)

    # --- message reads ---------------------------------------------------------
    def messages(self, user, limit, offset=0):
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            data = self.client.messages(peer, is_group=is_group, limit=limit, offset=offset)
            rendered, images = self._window(user, data.get("messages"))
            self._issue(account, user, images)
            return MessageWindow(rendered, bool(data.get("hasMoreBefore")))

    def message_windows(self, users, limit=80, expected_account=None):
        with self.lock:
            account = self._account(messages=True)
            if expected_account is not None and account != expected_account:
                raise AccountChangedError()
            windows, more = {}, {}
            for user in users:
                peer, is_group = split_user(user)
                data = self.client.messages(peer, is_group=is_group, limit=limit)
                rendered, images = self._window(user, data.get("messages"))
                windows[user] = rendered
                more[user] = bool(data.get("hasMoreBefore"))
                self.window_images[(account, user)] = images
                self._issue(account, user, images)
            return MessageWindowBatch(windows, more)

    def history_before(self, user, before, limit=100):
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            data = self.client.history_before(peer, list(before), limit=limit, is_group=is_group)
            rendered, images = self._window(user, data.get("messages"))
            self._issue(account, user, images)
            return rendered, bool(data.get("hasMoreBefore"))

    def history_after(self, user, after, limit=100):
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            data = self.client.history_after(peer, list(after), limit=limit, is_group=is_group)
            rendered, images = self._window(user, data.get("messages"))
            self._issue(account, user, images)
            return rendered, bool(data.get("hasMoreAfter"))

    def message_at(self, user, position):
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            raw = self.client.message_at(peer, list(position), is_group=is_group)
            if not raw:
                return None
            rendered, images = self._window(user, [raw])
            self._issue(account, user, images)
            return rendered[0] if rendered else None

    def history_search(self, user, *, before=None, start_ms=None, end_ms=None, query="", limit=50):
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            data = self.client.history_search(
                peer, before=list(before) if before else None, start_ms=start_ms,
                end_ms=end_ms, query=query, limit=limit, is_group=is_group)
            rendered, images = self._window(user, data.get("messages"))
            self._issue(account, user, images)
            next_after = data.get("nextAfter")
            return rendered, bool(data.get("hasMore")), (tuple(next_after) if next_after else None)

    def history_highwater(self, user):
        with self.lock:
            self._account(messages=True)
            peer, is_group = split_user(user)
            value = self.client.history_highwater(peer, is_group=is_group)
            return tuple(value) if value else None

    def history_page(self, user, highwater, after=None, page_size=256):
        with self.lock:
            account = self._account(messages=True)
            if highwater is None:
                return [], None
            peer, is_group = split_user(user)
            data = self.client.history_page(peer, ceiling=list(highwater),
                                            after=list(after) if after else None,
                                            limit=page_size, is_group=is_group)
            rendered, images = self._window(user, data.get("messages"))
            self._issue(account, user, images)
            next_after = data.get("nextAfter")
            return rendered, (tuple(next_after) if next_after else None)

    def quoted_history_page(self, user, ceiling, after=None, page_size=64, member=None):
        with self.lock:
            account = self._account(messages=True)
            if ceiling is None:
                return [], None
            peer, is_group = split_user(user)
            data = self.client.quoted_history_page(
                peer, ceiling=list(ceiling), after=list(after) if after else None,
                limit=page_size, member=member, is_group=is_group)
            rendered, images = self._window(user, data.get("messages"))
            self._issue(account, user, images)
            next_after = data.get("nextAfter")
            return rendered, (tuple(next_after) if next_after else None)

    def preceding_text_context(self, user, before, limit=3):
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            rows = self.client.preceded_text(peer, list(before), limit=limit, is_group=is_group)
            result = []
            for raw in rows or []:
                message, _ref = self._render(raw, user, account)
                if message is not None:
                    result.append({"id": message["id"], "side": message["side"],
                                   "text": message["text"]})
            return result

    # --- profile reads ---------------------------------------------------------
    @staticmethod
    def _subject_counts(user, member, is_group, counts, meta):
        if is_group and not member:
            return int(meta.get("total") or 0), int(meta.get("textCount") or 0)
        subject = member if is_group else split_user(user)[0]
        entry = counts.get(subject) or {}
        return int(entry.get("count") or 0), int(entry.get("text") or 0)

    def stats(self, user, member=None):
        with self.lock:
            self._account(messages=True)
            peer, is_group = split_user(user)
            meta = self.client.profile_metadata(peer, member, is_group=is_group)
            counts = meta.get("counts") or {}
            contacts = self._contacts_map()
            members_map = self._member_map(user) if is_group else {}
            members = []
            for sender in sorted(counts):
                members.append({"id": sender, **self._display(sender, contacts, members_map)})
            count, text_count = self._subject_counts(user, member, is_group, counts, meta)
            return count, text_count, members

    def profile_metadata(self, user, member=None):
        with self.lock:
            self._account(messages=True)
            peer, is_group = split_user(user)
            meta = self.client.profile_metadata(peer, member, is_group=is_group)
            counts = meta.get("counts") or {}
            if member and (not is_group or member not in counts):
                raise ValueError("unknown member")
            contacts = self._contacts_map()
            members_map = self._member_map(user) if is_group else {}
            members = []
            if is_group:
                for sender in sorted(counts):
                    members.append({"id": sender, **self._display(sender, contacts, members_map)})
            count, text_count = self._subject_counts(user, member, is_group, counts, meta)
            target = member if member else user
            return {"contact": self._display(target, contacts, members_map),
                    "members": members, "count": count, "textCount": text_count}

    def profile_overview(self, user, member=None, highwater=None):
        """Contact, member list, and per-sender counts without classifying text.

        The API portrait read path must never re-render every message just to show
        who is in a group. Counts are reused across reader refreshes while the
        account, conversation, and newest message stay the same, with a bounded
        refresh window for older-message backfills.
        """
        with self.lock:
            account = self._account(messages=True)
            peer, is_group = split_user(user)
            cache_key = (account, _workdir(account), user,
                         tuple(highwater) if highwater else None)
            cached = self.profile_overview_counts_cache.get(cache_key)
            if cached is not None and cached[2] > time.monotonic():
                counts, total, _expires = cached
                self.profile_overview_counts_cache.move_to_end(cache_key)
            else:
                meta = self.client.profile_metadata(peer, None, is_group=is_group)
                raw_counts = meta.get("counts") or {}
                counts = {sender: int((value or {}).get("count") or 0)
                          for sender, value in raw_counts.items()}
                total = int(meta.get("total") or 0)
                self.profile_overview_counts_cache[cache_key] = (
                    counts, total, time.monotonic() + PROFILE_OVERVIEW_TTL_SECONDS)
                while len(self.profile_overview_counts_cache) > PROFILE_METADATA_CACHE_LIMIT:
                    self.profile_overview_counts_cache.popitem(last=False)
            contacts = self._contacts_map()
            members_map = self._member_map(user) if is_group else {}
            if member and (not is_group or member not in counts):
                raise ValueError("unknown member")
            members = []
            if is_group:
                for sender in sorted(counts):
                    members.append({"id": sender, **self._display(sender, contacts, members_map)})
            count = (counts.get(member, 0) if member else
                     (total if is_group else counts.get(peer, 0)))
            target = member if member else user
            return {"contact": self._display(target, contacts, members_map),
                    "members": members, "count": count, "textCount": None}

    def texts_for_refs(self, user, refs, with_ids=False):
        self.require_messages_ready()
        if not refs:
            return []
        with self.lock:
            self._account(messages=True)
            peer, is_group = split_user(user)
            ids = [ref[-1] for ref in refs if isinstance(ref[-1], str)]
            rows = self.client.texts_by_ids(peer, ids, is_group=is_group)
            if with_ids:
                return [(identifier, text) for identifier, text in rows]
            return [text for _identifier, text in rows]

    # --- media -----------------------------------------------------------------
    def media(self, user, stable_id):
        self.media_reason.value = None

        def unavailable(reason):
            self.media_reason.value = reason
            return None

        if not isinstance(stable_id, str) or not STABLE_ID.fullmatch(stable_id):
            return unavailable("invalid-id")
        with self.lock:
            account = self._account(messages=True)
            ref = (self.issued_images.get((account, user, stable_id)) or
                   self.window_images.get((account, user), {}).get(stable_id))
            if ref is None:
                return unavailable("not-issued")
            if stable_id != stable_message_id(account, user, ref.get("msgId")):
                return unavailable("identity-mismatch")
            try:
                result = self.client.image(ref)
            except (qq_client.ReaderUnavailable, qq_client.ReaderError):
                return unavailable("row-unavailable")
        if not result:
            return unavailable("row-unavailable")
        if result.get("unavailable"):
            return unavailable(str(result["unavailable"]))
        try:
            data = base64.b64decode(result.get("data") or "", validate=True)
        except (ValueError, binascii.Error):
            return unavailable("decode-failed")
        if len(data) > MAX_IMAGE_BYTES:
            return unavailable("image-too-large")
        mime = result.get("mime")
        if mime not in IMAGE_MIMES:
            return unavailable("unsupported-format")
        return data, mime