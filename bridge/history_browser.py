"""Source-level cursor pagination and search for one QQ conversation.

This module no longer touches a database directly. Every read goes through the
`QQSource` duck-typed contract, which forwards to the injected QQNT reader over
loopback RPC. Cursors are opaque, account- and peer-bound positions of the form
``(sortTime, shard, msgSeq)``.
"""
from __future__ import annotations

import base64
import binascii
import json
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo


# QQNT has no message shard files; the reader labels every position with the
# constant shard "qq". A short opaque label keeps the cursor format generic.
SHARD_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
CURSOR_TEXT = re.compile(r"[A-Za-z0-9_-]{1,1024}\Z")


def encode_cursor(account, user, position):
    seq, shard, local_id = position
    raw = json.dumps([1, account, user, int(seq), shard, int(local_id)],
                     ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def decode_cursor(value, account, user):
    if not isinstance(value, str) or not CURSOR_TEXT.fullmatch(value):
        raise ValueError("invalid history cursor")
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        parts = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError, TypeError, binascii.Error) as exc:
        raise ValueError("invalid history cursor") from exc
    if (not isinstance(parts, list) or len(parts) != 6 or type(parts[0]) is not int or parts[0] != 1 or
            parts[1] != account or parts[2] != user or
            type(parts[3]) is not int or type(parts[5]) is not int or
            not 0 <= parts[3] <= 2**63 - 1 or not 0 <= parts[5] <= 2**63 - 1 or
            not isinstance(parts[4], str) or not SHARD_NAME.fullmatch(parts[4])):
        raise ValueError("invalid history cursor")
    return parts[3], parts[4], parts[5]


def date_bounds(value):
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("invalid history date")
    try:
        selected = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("invalid history date") from exc
    timezone = ZoneInfo("Asia/Shanghai")
    begin = int(datetime(selected.year, selected.month, selected.day, tzinfo=timezone).timestamp())
    end = begin + 86400
    return begin, end, begin * 1000, end * 1000


def _position(message):
    seq, shard, local_id = message["_sort"]
    return int(seq), str(shard), int(local_id)


def browse(source, account, user, *, before=None, around=None, limit=80,
           max_issued_images=2048):
    """One bounded page of a conversation, oldest-first, cursor-addressable."""
    if before is not None and around is not None:
        raise ValueError("before and around cannot be combined")
    before_position = decode_cursor(before, account, user) if before is not None else None
    around_position = decode_cursor(around, account, user) if around is not None else None
    target = None
    with source.lock:
        if source.identity()[0] != account:
            raise ValueError("account mismatch")
        if around_position is not None:
            target = source.message_at(user, around_position)
            if target is None:
                raise ValueError("invalid history cursor")
            older, _older_more = source.history_before(user, around_position, limit // 2)
            newer, _newer_more = source.history_after(
                user, around_position, max(0, limit - 1 - len(older)))
            messages = list(older) + [target] + list(newer)
            # Older history still exists above the focused window.
            more_before = _older_more or len(older) >= max(1, limit // 2)
            more_after = _newer_more
        elif before_position is not None:
            selected, more_before = source.history_before(user, before_position, limit)
            messages = list(selected)
            more_after = True
        else:
            window = source.messages(user, limit)
            messages = list(window)
            more_before = bool(getattr(window, "has_more_before", False))
            more_after = False
        next_position = _position(messages[0]) if messages else None
        return {"messages": messages, "hasMoreBefore": bool(more_before),
                "hasMoreAfter": bool(more_after),
                "nextCursor": encode_cursor(account, user, next_position) if next_position else None,
                "oldestCursor": messages[0]["historyCursor"] if messages else None,
                "newestCursor": messages[-1]["historyCursor"] if messages else None,
                "focusId": target["id"] if target is not None else None}


def search(source, account, user, *, query=None, day=None, before=None, limit=50,
           max_issued_images=2048):
    """Newest-first search inside one conversation, bounded by text or date."""
    if query is not None and (not isinstance(query, str) or len(query) > 256 or
                              any(ord(char) < 32 for char in query)):
        raise ValueError("invalid history query")
    query = (query or "").strip()
    bounds = date_bounds(day)
    if not query and bounds is None:
        raise ValueError("query or date required")
    before_position = decode_cursor(before, account, user) if before is not None else None
    start_ms, end_ms = (bounds[2], bounds[3]) if bounds is not None else (None, None)
    with source.lock:
        if source.identity()[0] != account:
            raise ValueError("account mismatch")
        messages, has_more, next_after = source.history_search(
            user, before=before_position, start_ms=start_ms, end_ms=end_ms,
            query=query, limit=limit)
        return {"messages": list(messages), "hasMore": bool(has_more),
                "nextCursor": (encode_cursor(account, user, next_after)
                               if has_more and next_after is not None else None)}


def saved_results(store, account, user, version, messages):
    ids = list(dict.fromkeys(message["id"] for message in messages))
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    args = [account, user, version, *ids]
    with store.connect() as conn:
        analyzed = conn.execute(
            f"SELECT id,result FROM results_v2 WHERE account=? AND session=? AND version=? "
            f"AND id IN ({placeholders})", args).fetchall()
        skipped = conn.execute(
            f"SELECT id,reason FROM analysis_skips WHERE account=? AND session=? AND version=? "
            f"AND id IN ({placeholders})", args).fetchall()
    return ({stable_id: {key: value for key, value in json.loads(raw).items() if key != "score"}
             for stable_id, raw in analyzed} |
            {stable_id: {"state": "skipped", "reason": reason} for stable_id, reason in skipped})