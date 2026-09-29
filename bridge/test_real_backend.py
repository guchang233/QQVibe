import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from real_backend import AccountChangedError, Backend, ForecastRequestError, NodeAnalysis, ResultStore, QQSource, affinity, infer_mbti, message_id, validate_personality_evidence
import qq_client
from profile_signals import STYLE_LABELS, historical_mood
from real_http import ROOT, make_handler
from instance_identity import instance_id


def personality_evidence(preferences="ESTJ"):
    return {axis: {left: .85 if chosen == left else .1,
                   right: .85 if chosen == right else .1,
                   "insufficient": .05}
            for axis, (left, right), chosen in zip(
                ("EI", "SN", "TF", "JP"), (("E", "I"), ("S", "N"), ("T", "F"), ("J", "P")), preferences,
            )}


class SyntheticSource:
    def __init__(self, workdir, account="account-a"):
        self.workdir = workdir
        self.account = account
        self.db = object()
        self.rows = {}

    def identity(self):
        return self.account, self.workdir

    def messages(self, user, limit, offset=0):
        return self.rows.get(user, [])[-(limit + offset):len(self.rows.get(user, [])) - offset if offset else None][-limit:]

    def history_highwater(self, user):
        rows = self.rows.get(user, [])
        return max((tuple(item["_sort"]) for item in rows), default=None)

    def history_page(self, user, highwater, after=None, page_size=256):
        if highwater is None:
            return [], None
        rows = sorted((item for item in self.rows.get(user, [])
                       if tuple(item["_sort"]) <= highwater and
                       (after is None or tuple(item["_sort"]) > after)),
                      key=lambda item: item["_sort"])[:page_size]
        return ([item for item in rows if item["kind"] != "system"],
                tuple(rows[-1]["_sort"]) if rows else None)

    def stats(self, user, member=None):
        rows = self.rows.get(user, [])
        members = [{"id": sender, "name": sender, "avatar": ""}
                   for sender in sorted({row["senderId"] for row in rows})]
        selected = [row for row in rows if row["senderId"] == member] if member else rows
        return len(selected), len(selected), members

    def contact(self, user):
        return {"name": user, "avatar": ""}

    def media(self, user, stable_id):
        return None

    def add(self, user, count, start=0, sender=None, text=None):
        rows = self.rows.setdefault(user, [])
        sender = sender or ("member" if user.endswith("@chatroom") else user)
        for number in range(start, start + count):
            rows.append({"id": message_id(self.account, user, str(number + 1)),
                         "side": "self" if sender == "me" else "other", "kind": "text",
                         "text": text or f"synthetic {number}", "senderId": sender,
                         "_sort": [number + 1, "qq", number + 1]})


class FakeQqClient:
    """In-memory stand-in for `qq_client.QqClient` over the injected reader.

    It mirrors the wire shapes produced by `qqnt/src/reader.js`: decoded messages
    already carry ``kind``/``type``/``text`` and a ``[time, "qq", seq]`` sort key,
    so `QQSource` can be exercised without a real QQ process.
    """

    def __init__(self, account="account-a", uid="me", uin="10001", nickname="Self"):
        self.account = account
        self.uid = uid
        self.uin = uin
        self.nickname = nickname
        self.friends = {}
        self.groups = {}
        self.sessions_list = []
        self.rows = {}
        self.images = {}
        self.fail = False

    # --- fixture builders ------------------------------------------------------
    def add_contact(self, peer, *, group=False, name=None, avatar=""):
        identifier = peer + "@chatroom" if group else peer
        entry = {"id": identifier, "name": name or peer, "avatar": avatar,
                 "avatarCandidates": [avatar] if avatar else []}
        (self.groups if group else self.friends)[identifier] = entry
        return entry

    def add_message(self, peer, msg_id, *, seq=None, time_ms=0, text="", kind="text",
                    type_="textElement", side="other", sender=None, sender_name="",
                    avatar="", image=None):
        seq = seq if seq is not None else len(self.rows.get(peer, [])) + 1
        sender = sender if sender is not None else ("me" if side == "self" else peer)
        row = {"msgId": str(msg_id), "msgTime": time_ms, "msgSeq": seq, "side": side,
               "text": text, "kind": kind, "type": type_, "senderId": sender,
               "senderName": sender_name or sender, "avatar": avatar,
               "sort": [time_ms, "qq", seq]}
        if image is not None:
            row["image"] = image
        self.rows.setdefault(peer, []).append(row)
        return row

    def _ordered(self, peer):
        return sorted(self.rows.get(peer, []), key=lambda item: item["sort"])

    # --- RPC surface -----------------------------------------------------------
    def self_info(self):
        if self.fail:
            raise qq_client.ReaderUnavailable("synthetic")
        return {"account": self.account, "uid": self.uid, "uin": self.uin,
                "nickname": self.nickname, "ready": bool(self.account)}

    def contacts(self):
        return {"friends": list(self.friends.values()), "groups": list(self.groups.values())}

    def sessions(self):
        return {"self": {"username": self.uid, "name": self.nickname},
                "sessions": self.sessions_list, "account": self.account,
                "messagesReady": True}

    def messages(self, peer, *, is_group=False, limit=80, offset=0, order="latest"):
        rows = self._ordered(peer)
        window = rows[-(limit + offset):len(rows) - offset if offset else None]
        return {"messages": window[-limit:], "hasMoreBefore": len(rows) > limit + offset}

    def history_highwater(self, peer, is_group=False):
        rows = self._ordered(peer)
        return rows[-1]["sort"] if rows else None

    def history_page(self, peer, *, ceiling=None, after=None, limit=1000, is_group=False):
        rows = [item for item in self._ordered(peer)
                if (ceiling is None or item["sort"] <= ceiling) and
                (after is None or item["sort"] > after)]
        page = rows[:limit]
        return {"messages": page, "nextAfter": page[-1]["sort"] if page else None}

    def preceded_text(self, peer, before, limit=3, is_group=False):
        rows = [item for item in self._ordered(peer)
                if item["sort"] < before and item["kind"] == "text" and item["text"].strip()]
        return rows[-limit:]

    def history_before(self, peer, before, *, limit=100, is_group=False):
        rows = [item for item in self._ordered(peer) if item["sort"] < before]
        return {"messages": rows[-limit:], "hasMoreBefore": len(rows) > limit}

    def history_after(self, peer, after, *, limit=100, is_group=False):
        rows = [item for item in self._ordered(peer) if item["sort"] > after]
        return {"messages": rows[:limit], "hasMoreAfter": len(rows) > limit}

    def message_at(self, peer, position, *, is_group=False):
        return next((item for item in self._ordered(peer) if item["sort"] == position), None)

    def history_search(self, peer, *, before=None, start_ms=None, end_ms=None,
                       query="", limit=50, is_group=False):
        rows = self._ordered(peer)
        if before is not None:
            rows = [item for item in rows if item["sort"] < before]
        if start_ms is not None:
            rows = [item for item in rows if item["msgTime"] >= start_ms]
        if end_ms is not None:
            rows = [item for item in rows if item["msgTime"] < end_ms]
        needle = query.strip().lower()
        if needle:
            rows = [item for item in rows if needle in item["text"].lower()]
        matched = list(reversed(rows))
        page = matched[:limit]
        return {"messages": page, "hasMore": len(matched) > limit,
                "nextAfter": page[-1]["sort"] if page else None}

    def senders(self, peer, *, is_group=False):
        seen = {}
        for item in self._ordered(peer):
            seen.setdefault(item["senderId"], {"id": item["senderId"],
                                               "name": item["senderName"],
                                               "avatar": item["avatar"]})
        return list(seen.values())

    def texts_by_ids(self, peer, ids, *, is_group=False):
        wanted = {str(value) for value in ids}
        found = []
        for item in self._ordered(peer):
            if item["kind"] != "text" or not item["text"]:
                continue
            identifier = message_id(self.account, peer, item["msgId"])
            if identifier in wanted:
                found.append([identifier, item["text"]])
        return found

    def quoted_history_page(self, peer, *, ceiling=None, after=None, limit=64,
                            member=None, is_group=False):
        rows = [item for item in self._ordered(peer) if item["type"] == "replyElement"]
        if member:
            rows = [item for item in rows if item["senderId"] == member]
        if ceiling is not None:
            rows = [item for item in rows if item["sort"] <= ceiling]
        if after is not None:
            rows = [item for item in rows if item["sort"] > after]
        page = rows[:limit]
        return {"messages": page, "nextAfter": page[-1]["sort"] if page else None}

    def profile_metadata(self, peer, member=None, is_group=False):
        counts, text_count = {}, 0
        for item in self._ordered(peer):
            entry = counts.setdefault(item["senderId"], {"count": 0, "text": 0})
            entry["count"] += 1
            if item["kind"] == "text" and item["text"].strip():
                entry["text"] += 1
                text_count += 1
        selected = counts.get(member or peer, {"count": 0, "text": 0})
        return {"counts": counts, "total": sum(entry["count"] for entry in counts.values()),
                "count": selected["count"] if member else sum(
                    entry["count"] for entry in counts.values()),
                "textCount": selected["text"] if member else text_count}

    def image(self, ref):
        return self.images.get(ref.get("msgId"))

    def reset(self):
        pass

    def available(self):
        return not self.fail


class SyntheticAnalyzer:
    def __init__(self):
        self.model = {"state": "ready"}
        self.version = "synthetic-catalog+questions-v1"
        self.evidence_for = lambda _message: None
        self.style_for = lambda _message: None
        self.calls = []
        self.fail = False
        self.forecast_calls = []
        self.forecast_fail = False
        self.forecast_hook = None
        self.entered = None
        self.release = None

    def analysis_version(self):
        return self.version

    def analyze(self, session, messages, target):
        if self.entered:
            self.entered.set()
            self.release.wait(timeout=5)
        if self.fail:
            self.fail = False
            raise RuntimeError("synthetic model failure")
        self.calls.append((session, target, [item["id"] for item in messages]))
        target_message = next(item for item in messages if item["id"] == target)
        return {"messageId": target, "analysisVersion": self.version,
                "personalityEvidence": self.evidence_for(target_message),
                "styleEvidence": self.style_for(target_message),
                "intentBroad": [{"label": "提问", "rawLabel": "ask question", "probability": 1}],
                "emotion": [{"label": "平静", "rawLabel": "neutral", "probability": 1}],
                "intent": [{"label": "提问", "rawLabel": "ask question", "probability": 1}],
                "emotionLabel": "平静", "intentLabel": "提问", "emotionP": 1,
                "intentP": 1, "score": None if target_message["side"] == "self" else .5}

    def predict_reply(self, session, messages, draft):
        self.forecast_calls.append((session, [item["id"] for item in messages], draft))
        if self.forecast_fail:
            self.forecast_fail = False
            raise RuntimeError("synthetic forecast failure")
        if self.forecast_hook:
            self.forecast_hook()
        return {"analysisVersion": self.version, "candidates": [
            {"id": "ask_question", "label": "提问", "probability": .6, "description": "可能继续追问细节"},
            {"id": "small_talk", "label": "闲聊", "probability": .3, "description": "可能继续简单寒暄"},
            {"id": "reject", "label": "拒绝", "probability": .1, "description": "可能明确拒绝"},
        ]}


class BackendTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = SyntheticSource(Path(self.temp.name))
        self.analyzer = SyntheticAnalyzer()
        self.store_factory = lambda account, _workdir: ResultStore(Path(self.temp.name) / (account + ".sqlite3"))
        self.backend = Backend(self.source, self.analyzer, self.store_factory)

    def run_job(self, user, mode="recent", limit=80):
        job = self.backend.start(user, mode, limit)
        self.backend.tasks.join()
        return self.backend.analysis(user)["job"]

    def test_stale_account_cannot_enqueue_analysis(self):
        with self.assertRaises(AccountChangedError):
            self.backend.start("friend", "recent", 1, expected_account="other-account")
        self.assertEqual(self.backend.tasks.unfinished_tasks, 0)

    def test_model_provider_reflects_runtime_fallback(self):
        analyzer = NodeAnalysis()
        analyzer.version = "synthetic-version"
        process = Mock()
        analyzer.process = process
        observed = {}
        def lines():
            yield json.dumps({"ready": True, "analysisVersion": "synthetic-version",
                              "model": {"state": "ready", "provider": "webgpu"}})
            yield json.dumps({"id": 1, "modelStatus": {"state": "ready", "provider": "cpu"}})
            observed.update(analyzer.model)
        process.stdout = lines()
        analyzer._read(process)
        self.assertEqual(observed["provider"], "cpu")
        self.analyzer.model = observed
        self.assertEqual(self.backend.health()["model"]["provider"], "cpu")
        self.assertEqual(self.backend.analysis("friend")["modelProvider"], "cpu")

    def test_incremental_restart_and_old_history(self):
        self.source.add("friend", 20)
        self.assertEqual(self.run_job("friend", limit=20)["processed"], 20)
        self.source.add("friend", 1, start=20)
        self.assertEqual(self.run_job("friend", limit=21)["processed"], 21)
        self.assertEqual(len(self.analyzer.calls), 21)
        self.assertEqual(self.run_job("friend", limit=21)["total"], 21)
        restarted = Backend(self.source, self.analyzer, self.store_factory)
        restarted.start("friend", "recent", 21)
        restarted.tasks.join()
        self.assertEqual(len(self.analyzer.calls), 21)
        self.source.rows["friend"].insert(0, {**self.source.rows["friend"][0],
            "id": "older-record", "_sort": [0, "message__message_0.db", 0]})
        restarted.start("friend", "history", 22)
        restarted.tasks.join()
        self.assertEqual(len(self.analyzer.calls), 22)
        self.assertEqual(restarted.analysis("friend")["affinityCount"], 22)
        self.assertEqual(restarted.analysis("friend")["affinity"], 75)

    def test_isolation_groups_failure_retry_and_duplicate_job(self):
        self.source.add("friend", 1)
        self.source.add("room@chatroom", 1)
        self.analyzer.fail = True
        first = self.run_job("friend")
        self.assertEqual(first["status"], "error")
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 0)
        self.backend.start("friend", "recent", 80)
        duplicate = self.backend.start("friend", "recent", 80)
        self.backend.tasks.join()
        self.assertEqual(len(self.analyzer.calls), 1)
        self.assertIn(duplicate["status"], ("queued", "running", "done"))
        self.run_job("room@chatroom")
        self.assertIsNone(self.backend.analysis("room@chatroom")["affinity"])
        self.assertIsNone(self.backend.profile("room@chatroom")["affinity"])
        self.assertEqual(self.backend.profile("room@chatroom")["members"][0]["id"], "member")
        self.source.account = "account-b"
        self.source.add("friend", 1, start=1)
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 0)
        self.assertEqual(self.backend.analysis("friend")["account"], "account-b")
        self.assertEqual(self.backend.profile("friend")["account"], "account-b")
        self.run_job("friend", limit=1)
        self.assertEqual(len(self.analyzer.calls), 3)
        self.source.account = "account-a"
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 1)
        self.assertEqual(self.backend.analysis("friend")["account"], "account-a")

    def test_account_switch_discards_running_and_queued_old_account_work(self):
        self.source.add("friend", 1)
        self.source.add("other", 1)
        entered, release = threading.Event(), threading.Event()
        self.analyzer.entered, self.analyzer.release = entered, release
        first = self.backend.start("friend", "recent", 1)
        self.assertTrue(entered.wait(timeout=3))
        second = self.backend.start("other", "recent", 1)
        self.source.account = "account-b"
        release.set()
        self.backend.tasks.join()
        self.assertEqual(self.backend.jobs[("account-a", str(self.store_factory("account-a", self.source.workdir).path),
                                            "friend", self.analyzer.version)]["status"], "error")
        self.assertEqual(self.backend.jobs[("account-a", str(self.store_factory("account-a", self.source.workdir).path),
                                            "other", self.analyzer.version)]["status"], "error")
        old_store = self.store_factory("account-a", self.source.workdir)
        self.assertEqual(old_store.ids("account-a", "friend", self.analyzer.version), set())
        self.assertEqual(old_store.ids("account-a", "other", self.analyzer.version), set())
        self.assertEqual(len(self.analyzer.calls), 1, "queued old work must not infer over the new account")
        self.assertEqual(self.backend.analysis("friend")["account"], "account-b")
        self.assertEqual(self.backend.analysis("friend")["results"], {})
        self.source.account = "account-a"
        self.assertEqual(self.backend.analysis("friend")["results"], {})

    def test_account_switch_during_messages_read_never_returns_mixed_payload(self):
        self.source.add("friend", 1)
        original = self.source.messages

        def switch_after_read(*args, **kwargs):
            rows = original(*args, **kwargs)
            self.source.account = "account-b"
            return rows

        self.source.messages = switch_after_read
        with self.assertRaises(AccountChangedError):
            self.backend.messages("friend", 80)

    def test_messages_returns_page_without_full_stats_scan(self):
        self.source.add("friend", 2)
        self.source.stats = Mock(side_effect=AssertionError("full stats called"))
        result = self.backend.messages("friend", 80)
        self.assertEqual(len(result["messages"]), 2)
        self.assertIsNone(result["total"])
        self.source.stats.assert_not_called()

    def test_all_history_analyzes_visible_window_before_older_pages(self):
        self.source.add("friend", 300)
        job = self.run_job("friend", "history", "all")
        self.assertEqual((job["status"], job["total"], job["processed"]), ("done", 300, 300))
        self.assertEqual(self.analyzer.calls[0][1], self.source.rows["friend"][-1]["id"])
        self.assertEqual(len({target for _, target, _ in self.analyzer.calls}), 300)

    def test_recent_prioritizes_latest_other_over_newer_self(self):
        self.source.add("friend", 1, sender="friend")
        self.source.add("friend", 1, start=1, sender="me")
        self.source.add("friend", 1, start=2, sender="friend")
        self.source.add("friend", 1, start=3, sender="me")
        self.run_job("friend", "recent", 4)
        self.assertEqual(self.analyzer.calls[0][1], self.source.rows["friend"][2]["id"])

    def test_incremental_checkpoint_reuses_previous_scores_and_only_reads_new_page(self):
        self.source.add("friend", 20)
        first = self.run_job("friend", "incremental", None)
        self.assertEqual((first["status"], first["checkpointComplete"]), ("done", True))
        self.assertEqual((first["total"], len(self.analyzer.calls)), (20, 20))
        before = self.backend.analysis("friend")
        self.assertEqual(before["affinityCount"], 20)
        self.source.add("friend", 1, start=20)
        self.run_job("friend", "recent", 21)
        immediate = self.backend.analysis("friend")
        self.assertEqual((immediate["affinityCount"], immediate["affinity"]), (21, 75))
        store = self.store_factory("account-a", self.source.workdir)
        saved = store.items("account-a", "friend", self.analyzer.version)[-1][1]
        store.save("account-a", "friend", self.analyzer.version, self.source.rows["friend"][-1], saved)
        self.assertEqual(store.summary("account-a", "friend", self.analyzer.version)["scoreCount"], 21)
        restarted = Backend(self.source, self.analyzer, self.store_factory)
        self.assertEqual(restarted.analysis("friend")["affinityCount"], 21)
        original_page = self.source.history_page
        after_values = []

        def tracked_page(user, highwater, after=None, page_size=256):
            after_values.append(after)
            return original_page(user, highwater, after, page_size)

        self.source.history_page = tracked_page
        self.source.messages = Mock(side_effect=AssertionError("completed baseline reread visible window"))
        with patch.object(ResultStore, "items", side_effect=AssertionError("full results parsed")):
            restarted.start("friend", "incremental", None)
            restarted.tasks.join()
            after = restarted.analysis("friend")
        self.assertTrue(after_values)
        self.assertEqual(after_values[0], tuple(self.source.rows["friend"][19]["_sort"]))
        self.assertNotIn(None, after_values)
        self.assertEqual(len(self.analyzer.calls), 21)
        self.assertEqual(after["affinityCount"], 21)
        self.assertEqual(after["affinity"], 75)
        self.assertEqual(after["job"]["phase"], "incremental")
        self.assertEqual(after["job"]["checkpointComplete"], True)

    def test_running_incremental_coalesces_new_highwater(self):
        self.source.add("friend", 20)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("friend", "incremental", None)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.source.add("friend", 1, start=20)
            self.backend.start("friend", "incremental", None)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        progress = self.store_factory("account-a", self.source.workdir).progress(
            "account-a", "friend", self.analyzer.version)
        self.assertEqual(progress["cursor"], tuple(self.source.rows["friend"][-1]["_sort"]))
        self.assertEqual(progress["eligible"], 21)
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 21)

    def test_out_of_order_summary_and_one_time_existing_result_bootstrap(self):
        self.source.add("friend", 3)
        scores = dict(zip((item["id"] for item in self.source.rows["friend"]), (.1, .5, .9)))
        emotions = dict(zip((item["id"] for item in self.source.rows["friend"]),
                            ("happy", "sad", "happy")))
        original = self.analyzer.analyze

        def varied(session, messages, target):
            result = original(session, messages, target)
            raw = emotions[target]
            result["score"] = scores[target]
            result["emotion"] = [{"label": raw, "rawLabel": raw, "probability": 1.0}]
            return result

        self.analyzer.analyze = varied
        self.run_job("friend", "recent", 3)
        store = self.store_factory("account-a", self.source.workdir)
        expected_mood = historical_mood(
            store.items("account-a", "friend", self.analyzer.version),
            {"happy": "(＾▽＾)", "sad": "(╥﹏╥)"})
        observed = self.backend.analysis("friend")
        self.assertEqual(observed["affinity"], affinity([.1, .5, .9]))
        self.assertEqual(observed["mood"], expected_mood)
        with closing(sqlite3.connect(store.path)) as conn, conn:
            conn.execute("DELETE FROM summary_v1")
        with patch.object(ResultStore, "items", side_effect=AssertionError("full results API called")):
            rebuilt = self.backend.analysis("friend")
            again = self.backend.analysis("friend")
        self.assertEqual(rebuilt["affinity"], observed["affinity"])
        self.assertEqual(again["mood"], expected_mood)

    def test_incremental_persists_exact_long_text_skip_and_continues(self):
        self.source.add("friend", 3)
        long_id = self.source.rows["friend"][1]["id"]
        self.source.rows["friend"][1]["text"] = "very long synthetic text"
        original = self.analyzer.analyze

        def analyze_or_skip(session, messages, target):
            if target == long_id:
                self.analyzer.calls.append((session, target, [item["id"] for item in messages]))
                raise RuntimeError("ObservedTextTooLongError")
            return original(session, messages, target)

        self.analyzer.analyze = analyze_or_skip
        self.analyzer.evidence_for = lambda _item: personality_evidence()
        job = self.run_job("friend", "incremental", None)
        self.assertEqual(job["status"], "done")
        result = self.backend.analysis("friend")
        self.assertEqual(result["results"][long_id]["state"], "skipped")
        self.assertEqual(result["affinityCount"], 2)
        self.assertEqual(self.backend.profile("friend")["mbtiInference"]["eligibleMessages"], 2)
        restarted = Backend(self.source, self.analyzer, self.store_factory)
        restarted.start("friend", "incremental", None)
        restarted.tasks.join()
        self.assertEqual(len([target for _session, target, _context in self.analyzer.calls
                              if target == long_id]), 1)
        self.assertEqual(restarted.analysis("friend")["affinityCount"], 2)

    def test_incremental_account_isolation(self):
        self.source.add("friend", 2)
        self.run_job("friend", "incremental", None)
        self.source.account = "account-b"
        self.source.rows.clear()
        self.source.add("friend", 1)
        result = self.run_job("friend", "incremental", None)
        self.assertEqual(result["total"], 1)
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 1)
        self.source.account = "account-a"
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 2)

    def test_other_session_recent_work_runs_before_long_history_finishes(self):
        self.source.add("old", 300)
        self.source.add("current", 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "history", "all")
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("current", "recent", 1)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        current_id = self.source.rows["current"][0]["id"]
        self.assertLessEqual(calls.index(current_id), 8,
                             "another session must start before the first visible 80 finish")
        self.assertEqual(len(calls), 301)

    def test_recent_preempts_all_history_after_one_model_call(self):
        self.source.add("old", 24)
        self.source.add("current", 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "history", "all")
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("current", "recent", 1)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        self.assertLessEqual(calls.index(self.source.rows["current"][0]["id"]), 1)
        self.assertEqual(self.backend.analysis("old")["job"]["status"], "done")
        self.assertEqual(len(calls), 25)

    def test_recent_preempts_finite_history_after_one_model_call(self):
        self.source.add("old", 24)
        self.source.add("current", 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "history", 24)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("current", "recent", 1)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        self.assertLessEqual(calls.index(self.source.rows["current"][0]["id"]), 1)
        self.assertEqual(self.backend.analysis("old")["job"]["processed"], 24)
        self.assertEqual(len(calls), 25)

    def test_finite_history_upgrade_resumes_without_duplicate_inference(self):
        self.source.add("friend", 20)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("friend", "history", 10)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            upgraded = self.backend.start("friend", "history", 20)
            self.assertEqual(upgraded["requested"], {"mode": "history", "limit": 20})
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        job = self.backend.analysis("friend")["job"]
        self.assertEqual((job["status"], job["scope"]["limit"], job["processed"]),
                         ("done", 20, 20))
        self.assertEqual(len(self.analyzer.calls), 20)

    def test_finite_history_promotes_incremental_at_completion_boundary(self):
        self.source.add("friend", 12)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("friend", "history", 3)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            upgraded = self.backend.start("friend", "incremental", None)
            self.assertEqual(upgraded["requested"], {"mode": "incremental", "limit": None})
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        job = self.backend.analysis("friend")["job"]
        self.assertEqual((job["status"], job["checkpointComplete"]), ("done", True))
        self.assertEqual(len(self.analyzer.calls), 12)

    def test_cached_recent_window_finishes_before_baseline_continues(self):
        self.source.add("friend", 12)
        self.run_job("friend", "recent", 3)
        cached_calls = len(self.analyzer.calls)
        entered, release = threading.Event(), threading.Event()
        original_analyze = self.analyzer.analyze

        def pause_first_background(session, context, target):
            if not entered.is_set():
                entered.set()
                release.wait(timeout=5)
            return original_analyze(session, context, target)

        self.analyzer.analyze = pause_first_background
        original_recent_turn = self.backend._run_recent_turn
        completed_at = []

        def record_recent_completion(*args):
            original_recent_turn(*args)
            if args[2].get("recent", {}).get("status") == "done" and not completed_at:
                completed_at.append(len(self.analyzer.calls))

        self.backend._run_recent_turn = record_recent_completion
        try:
            self.backend.start("friend", "incremental", None)
            self.assertTrue(entered.wait(timeout=5))
            requested = self.backend.start("friend", "recent", 3)
            self.assertEqual(requested["recent"]["status"], "queued")
        finally:
            release.set()
            self.backend.tasks.join()
        self.assertEqual(completed_at, [cached_calls + 1])
        self.assertTrue(self.backend.analysis("friend")["job"]["checkpointComplete"])
        self.assertEqual(len(self.analyzer.calls), 12)

    def test_two_cached_recent_windows_finish_in_one_turn_each(self):
        self.source.add("first", 65)
        self.source.add("second", 65)
        self.run_job("first", "recent", 65)
        self.run_job("second", "recent", 65)
        calls_before = len(self.analyzer.calls)
        first_entered, release = threading.Event(), threading.Event()
        original_turn = self.backend._run_recent_turn
        turns = {}

        def count_turn(key, store, job, scope):
            user = key[2]
            turns[user] = turns.get(user, 0) + 1
            if user == "first" and turns[user] == 1:
                first_entered.set()
                release.wait(timeout=5)
            return original_turn(key, store, job, scope)

        self.backend._run_recent_turn = count_turn
        try:
            self.backend.start("first", "recent", 65)
            self.assertTrue(first_entered.wait(timeout=5))
            self.backend.start("second", "recent", 65)
        finally:
            release.set()
            self.backend.tasks.join()
        self.assertEqual(turns, {"first": 1, "second": 1})
        self.assertEqual(len(self.analyzer.calls), calls_before)
        self.assertEqual(self.backend.analysis("first")["job"]["recent"]["status"], "done")
        self.assertEqual(self.backend.analysis("second")["job"]["recent"]["status"], "done")

    def test_same_session_recent_preempts_finite_history_without_duplicate_model_calls(self):
        self.source.add("friend", 12)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("friend", "history", 12)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("friend", "recent", 12)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        self.assertEqual((len(calls), len(set(calls))), (12, 12))
        job = self.backend.analysis("friend")["job"]
        self.assertEqual((job["status"], job["processed"], job["recent"]["status"]),
                         ("done", 12, "done"))

    def test_same_session_recent_preempts_all_history_without_duplicate_model_calls(self):
        self.source.add("friend", 100)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("friend", "history", "all")
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("friend", "recent", 100)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        self.assertEqual((len(calls), len(set(calls))), (100, 100))
        job = self.backend.analysis("friend")["job"]
        self.assertEqual((job["status"], job["total"], job["processed"], job["recent"]["status"]),
                         ("done", 100, 100, "done"))

    def test_cached_incremental_and_all_history_finish_in_one_worker_turn(self):
        self.source.add("friend", 24)
        self.run_job("friend", "recent", 24)
        calls_before = len(self.analyzer.calls)
        original_enqueue = self.backend._enqueue
        enqueued = []

        def count_background_turns(task, interactive=False):
            if task[1] in ("incremental", "history"):
                enqueued.append(task[1])
            return original_enqueue(task, interactive)

        self.backend._enqueue = count_background_turns
        incremental = self.run_job("friend", "incremental", None)
        history = self.run_job("friend", "history", "all")
        self.assertEqual(enqueued, ["incremental", "history"])
        self.assertTrue(incremental["checkpointComplete"])
        self.assertEqual((history["status"], history["processed"]), ("done", 24))
        self.assertEqual(len(self.analyzer.calls), calls_before)

    def test_recent_queue_preempts_other_baselines_after_one_chunk(self):
        self.source.add("old", 24)
        self.source.add("backlog", 24)
        self.source.add("current", 12)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "incremental", None)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("backlog", "incremental", None)
            requested = self.backend.start("current", "recent", 12)
            self.assertEqual(requested["recent"]["status"], "queued")
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        current = self.source.rows["current"][-1]["id"]
        backlog = self.source.rows["backlog"][-1]["id"]
        self.assertLessEqual(calls.index(current), 8)
        self.assertLess(calls.index(current), calls.index(backlog))
        self.assertEqual(self.backend.analysis("current")["job"]["recent"]["status"], "done")

    def test_visible_baseline_preempts_old_background_without_losing_cursors(self):
        self.source.add("old", 24)
        self.source.add("backlog", 24)
        self.source.add("current", 24)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "incremental", None)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("backlog", "incremental", None)
            self.backend.start("current", "incremental", None)
            self.backend.analysis("current")  # the selected UI conversation
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        current_ids = {item["id"] for item in self.source.rows["current"]}
        backlog_ids = {item["id"] for item in self.source.rows["backlog"]}
        self.assertLess(min(calls.index(item) for item in current_ids),
                        min(calls.index(item) for item in backlog_ids))
        self.assertEqual(len(calls), len(set(calls)))
        for user in ("old", "backlog", "current"):
            job = self.backend.analysis(user)["job"]
            self.assertEqual(job["status"], "done")
            self.assertTrue(job["checkpointComplete"])
            self.assertEqual(job["processed"], 24)

    def test_recent_still_preempts_focused_baseline(self):
        self.source.add("old", 24)
        self.source.add("current", 24)
        self.source.add("urgent", 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "incremental", None)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("current", "incremental", None)
            self.backend.analysis("current")
            self.backend.start("urgent", "recent", 1)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        urgent = self.source.rows["urgent"][0]["id"]
        current = self.source.rows["current"][-1]["id"]
        self.assertLessEqual(calls.index(urgent), 1)
        self.assertLess(calls.index(urgent), calls.index(current))
        self.assertEqual(len(calls), len(set(calls)))

    def test_focused_recent_stays_ahead_of_other_recent(self):
        for user in ("old", "background", "visible"):
            self.source.add(user, 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("old", "history", 1)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("background", "recent", 1)
            self.backend.start("visible", "recent", 1)
            self.backend.analysis("visible")
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        self.assertEqual(calls, [self.source.rows[user][0]["id"]
                                 for user in ("old", "visible", "background")])

    def test_recent_window_precedes_focused_member_portrait(self):
        account, workdir, store = self.backend._scoped_identity()
        version = self.analyzer.analysis_version()
        focused = (account, str(store.path), "room@chatroom", version)
        other = (account, str(store.path), "friend", version)
        self.backend.batch_engine = Mock(focused_member=(focused, "member"))
        self.backend._focus(focused)

        def rank(key, mode, limit):
            return self.backend._task_priority((key, mode, limit, store, {}, (account, workdir)))

        self.assertLess(rank(other, "recent-window", 1),
                        rank(focused, "batch-subject", "member"))
        self.assertLess(rank(focused, "batch-subject", "member"),
                        rank(other, "incremental", None))

    def test_recent_requests_share_single_worker_in_eight_item_chunks(self):
        self.source.add("first", 12)
        self.source.add("second", 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            self.backend.start("first", "recent", 12)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.backend.start("second", "recent", 1)
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        calls = [target for _session, target, _context in self.analyzer.calls]
        second = self.source.rows["second"][0]["id"]
        self.assertLessEqual(calls.index(second), 8)
        self.assertEqual(len(calls), 13)

    def test_recent_at_baseline_completion_boundary_is_not_lost(self):
        self.source.add("friend", 1)
        store = self.backend._scoped_identity()[2]
        original_advance = store.advance
        entered, release = threading.Event(), threading.Event()

        def hold_final(*args, **kwargs):
            if kwargs.get("complete"):
                entered.set()
                release.wait(timeout=5)
            return original_advance(*args, **kwargs)

        store.advance = hold_final
        try:
            self.backend.start("friend", "incremental", None)
            self.assertTrue(entered.wait(timeout=5))
            self.source.add("friend", 1, start=1)
            request = self.backend.start("friend", "recent", 2)
            self.assertEqual(request["recent"]["status"], "queued")
        finally:
            release.set()
            self.backend.tasks.join()
        result = self.backend.analysis("friend")
        self.assertEqual(result["job"]["recent"]["status"], "done")
        self.assertIn(self.source.rows["friend"][-1]["id"], result["results"])
        self.assertEqual(len(self.analyzer.calls), 2)

    def test_history_upgrade_during_recent_window_runs_after_window(self):
        self.source.add("friend", 2)
        older, newer = (item["id"] for item in self.source.rows["friend"])
        history_entered, history_release = threading.Event(), threading.Event()
        recent_entered, recent_release = threading.Event(), threading.Event()
        original_analyze = self.analyzer.analyze

        def pause_each_stage(session, context, target):
            if target == newer:
                history_entered.set()
                history_release.wait(timeout=5)
            elif target == older:
                recent_entered.set()
                recent_release.wait(timeout=5)
            return original_analyze(session, context, target)

        self.analyzer.analyze = pause_each_stage
        try:
            self.backend.start("friend", "history", 1)
            self.assertTrue(history_entered.wait(timeout=5))
            self.backend.start("friend", "recent", 2)
            history_release.set()
            self.assertTrue(recent_entered.wait(timeout=5))
            upgraded = self.backend.start("friend", "history", "all")
            self.assertEqual(upgraded["requested"], {"mode": "history", "limit": "all"})
        finally:
            history_release.set()
            recent_release.set()
            self.backend.tasks.join()
        job = self.backend.analysis("friend")["job"]
        self.assertEqual(job["status"], "done")
        self.assertEqual(job["scope"]["mode"], "history")
        self.assertEqual(job["scope"]["limit"], "all")
        self.assertEqual(len(self.analyzer.calls), 2)

    def test_repeat_recent_coalesces_new_window_and_account_switch_rejects_old_work(self):
        self.source.add("friend", 9)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            first = self.backend.start("friend", "recent", 9)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            self.source.add("friend", 1, start=9)
            repeated = self.backend.start("friend", "recent", 10)
            self.assertEqual(repeated["recent"]["id"], first["recent"]["id"])
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        result = self.backend.analysis("friend")
        self.assertEqual(result["job"]["recent"]["status"], "done")
        self.assertIn(self.source.rows["friend"][-1]["id"], result["results"])
        self.assertEqual(len(self.analyzer.calls), 10)

        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        self.source.add("busy", 1)
        self.source.add("target", 1)
        try:
            self.backend.start("busy", "recent", 1)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            queued = self.backend.start("target", "recent", 1)
            self.source.account = "account-b"
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        old_job = next(job for job in self.backend.jobs.values() if job["id"] == queued["id"])
        self.assertEqual(old_job["recent"]["status"], "error")
        self.assertNotIn(self.source.rows["target"][0]["id"],
                         [target for _session, target, _context in self.analyzer.calls])

    def test_profile_reuses_saved_lexical_evidence_without_reading_source(self):
        self.source.add("friend", 1, text="alpha alpha")
        self.run_job("friend", limit=1)
        self.source.texts_for_refs = Mock(return_value=["alpha alpha"])
        self.source.history_highwater = Mock(side_effect=AssertionError("full history scan called"))
        profile = self.backend.profile("friend")
        self.assertIn({"word": "alpha", "count": 2}, profile["keywords"])
        self.source.texts_for_refs.assert_not_called()
        self.source.history_highwater.assert_not_called()

    def test_source_fetches_profile_text_by_reader_id(self):
        client = FakeQqClient()
        client.add_message("friend", "m1", seq=1, time_ms=101, text="alpha alpha")
        client.add_message("friend", "m2", seq=2, time_ms=102, text="private second")
        source = QQSource(client=client)
        stable_id = message_id("account-a", "friend", "m1")
        self.assertEqual(source.texts_for_refs("friend", [("qq", 1, stable_id),
                                                          ("qq", 2, "wrong-id")]),
                         ["alpha alpha"])

    def test_account_switch_during_analysis_or_profile_never_returns_old_result(self):
        self.source.add("friend", 1)
        store = self.backend._identity()[1]
        original_recent = store.recent

        def switch_after_rows(*args, **kwargs):
            rows = original_recent(*args, **kwargs)
            self.source.account = "account-b"
            return rows

        store.recent = switch_after_rows
        with self.assertRaises(AccountChangedError):
            self.backend.analysis("friend")
        store.recent = original_recent
        self.source.account = "account-a"
        original_contact = self.source.contact

        def switch_after_contact(*args, **kwargs):
            contact = original_contact(*args, **kwargs)
            self.source.account = "account-b"
            return contact

        self.source.contact = switch_after_contact
        with self.assertRaises(AccountChangedError):
            self.backend.profile("friend")

    def test_no_live_account_fails_closed_at_http_boundary(self):
        client = FakeQqClient()
        client.fail = True
        source = QQSource(client=client)
        backend = Backend(source, self.analyzer, self.store_factory)
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(backend))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            def get(path):
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port)
                conn.request("GET", path)
                response = conn.getresponse()
                status, body = response.status, json.loads(response.read())
                conn.close()
                return status, body

            status, health = get("/api/health")
            self.assertEqual(status, 200)
            self.assertEqual((health["ok"], health["data"]["state"]), (False, "account-unavailable"))
            self.assertNotIn("account", health["data"])
            status, sessions = get("/api/sessions")
            self.assertEqual((status, sessions["error"]), (503, "AccountUnavailableError"))
            self.assertNotIn("account", sessions)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_affinity_reference_and_stable_identity(self):
        self.assertEqual(affinity([1, -1]), 33)
        self.assertNotEqual(message_id("a", "user", "m1"), message_id("b", "user", "m1"))
        self.assertNotEqual(message_id("a", "user", "m1"), message_id("a", "room", "m1"))
        self.assertNotEqual(message_id("a", "user", "m1"), message_id("a", "user", "m2"))

    def test_health_and_http_contract_failures(self):
        self.analyzer.model = {"state": "missing", "message": "synthetic unavailable"}
        self.assertFalse(self.backend.health()["ok"])
        self.assertEqual(self.backend.health()["model"]["state"], "missing")
        self.source.db = None
        self.assertEqual(self.backend.health()["data"]["state"], "idle")
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.backend))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            def request(method, path, body=None, headers=None):
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port)
                conn.request(method, path, body, headers or {})
                response = conn.getresponse()
                status, payload = response.status, response.read()
                conn.close()
                return status, payload

            health = json.loads(request("GET", "/api/health")[1])
            self.assertEqual(health["version"], "real-ui-1")
            self.assertEqual(health["instanceId"], instance_id(ROOT))
            self.assertEqual(request("GET", "/api/health", headers={"Host": f"localhost:{server.server_port}"})[0], 200)
            self.assertEqual(request("GET", "/api/messages?user=friend&limit=-1")[0], 400)
            self.assertEqual(request("POST", "/api/analyze", json.dumps({"texts": ["not accepted"]}),
                                     {"Content-Type": "application/json"})[0], 400)
            self.assertEqual(request("GET", "/api/media?user=friend&id=missing")[0], 404)
            valid = {"Content-Type": "application/json; charset=UTF-8", "Origin": f"http://localhost:{server.server_port}"}
            self.assertEqual(request("POST", "/api/analyze", json.dumps({"user": "friend", "mode": "recent"}), valid)[0], 400)
            self.assertEqual(request("POST", "/api/analyze", json.dumps({"account": "account-a", "user": "friend", "mode": "recent"}), valid)[0], 202)
            self.assertEqual(request("POST", "/api/analyze", json.dumps({"account": "account-a", "user": "friend", "mode": "history", "limit": "all"}), valid)[0], 202)
            self.assertEqual(request("POST", "/api/analyze", json.dumps({"account": "account-a", "user": "friend", "mode": "recent", "limit": "all"}), valid)[0], 400)
            self.assertEqual(request("GET", "/", headers={"Host": f"evil.example:{server.server_port}"})[0], 403)
            self.assertEqual(request("GET", "/api/health", headers={"Origin": "https://evil.example"})[0], 403)
            self.assertEqual(request("POST", "/api/analyze", "{}", {**valid, "Origin": "null"})[0], 403)
            self.assertEqual(request("POST", "/api/analyze", "{}", {"Content-Type": "text/plain"})[0], 415)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_same_account_different_temporary_workdir_keeps_results(self):
        self.source.add("friend", 1)
        self.run_job("friend")
        other = Path(self.temp.name) / "other"
        other.mkdir()
        self.source.workdir = other
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 1)
        self.run_job("friend")
        self.assertEqual(len(self.analyzer.calls), 1)

    def test_concurrent_same_session_uses_single_job(self):
        self.source.add("friend", 1)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            first = self.backend.start("friend", "recent", 80)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            second = self.backend.start("friend", "history", 500)
            self.assertEqual(first["id"], second["id"])
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        self.assertEqual(len(self.analyzer.calls), 1)

    def test_running_recent_upgrades_to_requested_history(self):
        self.source.add("friend", 50)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        try:
            recent = self.backend.start("friend", "recent", 20)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            history = self.backend.start("friend", "history", 50)
            duplicate = self.backend.start("friend", "history", 50)
            self.assertEqual(recent["id"], history["id"])
            self.assertEqual(history["id"], duplicate["id"])
            self.assertEqual(history["requested"], {"mode": "history", "limit": 50})
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        job = self.backend.analysis("friend")["job"]
        self.assertEqual((job["status"], job["total"], job["processed"]), ("done", 50, 50))
        self.assertEqual(job["scope"]["limit"], 50)
        self.assertEqual(len(self.backend.analysis("friend")["results"]), 50)
        self.assertEqual(len(self.analyzer.calls), 50)
        self.assertEqual(len({target for _, target, _ in self.analyzer.calls}), 50)

    def test_all_history_snapshot_cache_and_upgrade(self):
        self.source.add("friend", 5002)
        self.source.rows["friend"][127]["kind"] = "system"
        self.source.rows["friend"][4099]["kind"] = "system"
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        original_analyze = self.analyzer.analyze

        def add_new_during_scan(session, messages, target):
            if len(self.analyzer.calls) == 20:
                self.source.add("friend", 1, start=5002)
            return original_analyze(session, messages, target)

        self.analyzer.analyze = add_new_during_scan
        try:
            recent = self.backend.start("friend", "recent", 20)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            upgraded = self.backend.start("friend", "history", "all")
            self.assertEqual(recent["id"], upgraded["id"])
            self.assertEqual(upgraded["requested"], {"mode": "history", "limit": "all"})
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        job = self.backend.analysis("friend")["job"]
        self.assertEqual((job["status"], job["total"], job["processed"]), ("done", 5000, 5000))
        self.assertEqual(job["scope"]["limit"], "all")
        self.assertEqual(len({target for _, target, _ in self.analyzer.calls}), 5000)
        self.assertEqual(self.run_job("friend", "history", "all")["total"], 5001)
        self.assertEqual(len(self.analyzer.calls), 5001)
        self.assertEqual(self.run_job("friend", "recent", 21)["processed"], 21)

    def test_history_signals_and_member_isolation(self):
        room = "room@chatroom"
        self.source.add(room, 1, sender="member", text="alpha alpha")
        self.source.add(room, 501, start=1, sender="someone", text="beta beta")
        self.source.add(room, 1, start=502, sender="member", text="laterword laterword")
        self.source.add(room, 1, start=503, sender="me", text="privateword privateword")
        self.analyzer.style_for = lambda message: {key: (.2 if message["senderId"] == "member" else .8)
                                                   for key in STYLE_LABELS}
        self.run_job(room, "history", "all")
        cached_result = next(iter(self.backend.analysis(room)["results"].values()))
        self.assertEqual(cached_result["intentBroad"][0]["rawLabel"], "ask question")
        self.assertEqual(set(cached_result["styleEvidence"]), set(STYLE_LABELS))
        member = self.backend.profile(room, "member")
        whole = self.backend.profile(room)
        self.assertEqual((member["stats"]["analyzedCount"], whole["stats"]["analyzedCount"]), (2, 504))
        self.assertEqual(member["traits"][0]["val"], 20)
        self.assertEqual(whole["traits"][0]["sampleCount"], 503)
        self.assertEqual(member["mood"]["sampleCount"], 2)
        self.assertEqual(whole["mood"]["sampleCount"], 503)
        self.assertIn({"word": "laterword", "count": 2}, member["keywords"])
        self.assertNotIn("beta", [entry["word"] for entry in member["keywords"]])
        self.assertNotIn("privateword", [entry["word"] for entry in whole["keywords"]])
        self.assertIsNone(whole["mbtiInference"])

    def test_failed_upgrade_is_error_until_manual_retry(self):
        self.source.add("friend", 50)
        self.analyzer.entered = threading.Event()
        self.analyzer.release = threading.Event()
        self.analyzer.fail = True
        try:
            first = self.backend.start("friend", "recent", 20)
            self.assertTrue(self.analyzer.entered.wait(timeout=5))
            upgraded = self.backend.start("friend", "history", 50)
            self.assertEqual(first["id"], upgraded["id"])
        finally:
            self.analyzer.release.set()
            self.backend.tasks.join()
        self.assertEqual(self.backend.analysis("friend")["job"]["status"], "error")
        self.assertEqual(self.backend.analysis("friend")["results"], {})
        retry = self.run_job("friend", "history", 50)
        self.assertNotEqual(retry["id"], first["id"])
        self.assertEqual((retry["status"], retry["processed"]), ("done", 50))

    def test_profile_filters_person_and_exposes_observed_fields(self):
        self.source.add("friend", 1, text="apple apple hello")
        self.source.add("friend", 1, start=1, sender="me", text="private own only")
        self.run_job("friend")
        profile = self.backend.profile("friend")
        self.assertEqual(profile["stats"], {"messageCount": 1, "textCount": 1,
                                            "analyzedCount": 1, "participantCount": 1})
        self.assertEqual(profile["emotion"][0]["label"], "平静")
        self.assertEqual(profile["mood"], {"label": "平静", "rawLabel": "neutral", "kaomoji": "(￣▽￣)",
                                           "sampleCount": 1, "scope": "analyzed-history"})
        self.assertIn({"word": "apple", "count": 2}, profile["keywords"])
        self.assertNotIn("private", [item["word"] for item in profile["keywords"]])
        self.assertEqual(profile["affinity"], 75)
        self.assertIsNone(profile["mbti"])

        room = "room@chatroom"
        self.source.add(room, 1, text="memberword memberword")
        self.source.add(room, 1, start=1, sender="someone", text="otherword otherword")
        self.source.add(room, 1, start=2, sender="me", text="ownword ownword")
        self.run_job(room)
        whole = self.backend.profile(room)
        member = self.backend.profile(room, "member")
        self.assertIsNone(whole["affinity"])
        self.assertIsNone(member["affinity"])
        self.assertEqual(member["stats"], {"messageCount": 1, "textCount": 1,
                                           "analyzedCount": 1, "participantCount": 1})
        self.assertEqual([item["word"] for item in member["keywords"]], ["memberword"])
        self.assertNotIn("ownword", [item["word"] for item in whole["keywords"]])

    def test_empty_distribution_is_error_and_self_without_relationship_is_valid(self):
        self.source.add("friend", 1, sender="me")
        self.run_job("friend")
        self.assertEqual(self.backend.analysis("friend")["job"]["status"], "done")
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 0)
        self.source.add("friend", 1, start=1)
        analyze = self.analyzer.analyze
        self.analyzer.analyze = lambda session, messages, target: {**analyze(session, messages, target), "emotion": []}
        self.assertEqual(self.run_job("friend")["status"], "error")
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 0)
        self.analyzer.analyze = analyze
        self.assertEqual(self.run_job("friend")["status"], "done")
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 1)

    def test_latest_new_emotion_does_not_fall_back(self):
        self.source.add("friend", 1)
        self.run_job("friend")
        analyze = self.analyzer.analyze

        def new_emotion(session, messages, target):
            result = analyze(session, messages, target)
            result["emotion"] = [{"label": "新的真实情绪", "rawLabel": "new-catalog-emotion", "probability": .9}]
            result["emotionLabel"] = "新的真实情绪"
            return result

        self.analyzer.analyze = new_emotion
        self.source.add("friend", 1, start=1)
        self.run_job("friend")
        self.assertEqual(self.backend.profile("friend")["mood"],
                         {"label": "新的真实情绪", "rawLabel": "new-catalog-emotion", "kaomoji": None,
                          "sampleCount": 2, "scope": "analyzed-history"})

    def test_personality_evidence_validation_and_thresholds(self):
        evidence = personality_evidence()
        self.assertEqual(validate_personality_evidence(evidence), evidence)
        self.assertIsNone(validate_personality_evidence(None))
        with self.assertRaises(RuntimeError):
            validate_personality_evidence({"EI": evidence["EI"]})
        invalid = personality_evidence()
        invalid["SN"]["S"] = -1
        with self.assertRaises(RuntimeError):
            validate_personality_evidence(invalid)
        self.assertEqual(infer_mbti([{"personalityEvidence": evidence}] * 99, "v1")["status"], "insufficient")
        complete = infer_mbti([{"personalityEvidence": evidence}] * 100, "v1")
        self.assertEqual(complete["type"], "ESTJ")
        self.assertEqual(complete["status"], "estimated")
        self.assertEqual((complete["eligibleMessages"], complete["supportedMessages"]), (100, 100))
        self.assertEqual(complete["axes"]["EI"]["evidenceCount"], 100)
        self.assertAlmostEqual(complete["axes"]["EI"]["leftShare"], .85 / .95)
        for axis in ("EI", "SN", "TF", "JP"):
            self.assertEqual(complete["axes"][axis]["evidenceCount"] +
                             complete["axes"][axis]["insufficientCount"], 100)
        missing = infer_mbti([{"personalityEvidence": None}] * 100, "v1")
        self.assertEqual(missing["supportedMessages"], 0)
        self.assertIsNone(missing["axes"]["EI"]["leftShare"])
        self.assertIsNone(missing["type"])

        sparse = personality_evidence()
        sparse["JP"] = {"J": .1, "P": .1, "insufficient": .8}
        partial = infer_mbti([{"personalityEvidence": evidence}] * 29 +
                             [{"personalityEvidence": sparse}] * 71, "v1")
        self.assertIsNone(partial["type"])
        self.assertIsNone(partial["axes"]["JP"]["leftShare"])
        self.assertEqual((partial["axes"]["JP"]["evidenceCount"],
                          partial["axes"]["JP"]["insufficientCount"]), (29, 71))
        balanced = personality_evidence()
        balanced["EI"] = {"E": .45, "I": .45, "insufficient": .1}
        ambiguous = infer_mbti([{"personalityEvidence": balanced}] * 100, "v1")
        self.assertEqual(ambiguous["axes"]["EI"]["leftShare"], .5)
        self.assertIsNone(ambiguous["type"])
        self.assertEqual(ambiguous["status"], "partial")

    def test_profile_excludes_self_and_other_group_members(self):
        self.analyzer.evidence_for = lambda message: personality_evidence("INFP" if message["senderId"] == "someone" else "ESTJ")
        self.source.add("friend", 99)
        self.source.add("friend", 100, start=99, sender="me")
        self.run_job("friend", "history", 199)
        profile = self.backend.profile("friend")
        self.assertEqual(profile["mbtiInference"]["eligibleMessages"], 99)
        self.assertIsNone(profile["mbti"])
        self.source.add("friend", 1, start=199)
        self.run_job("friend", "history", 200)
        self.assertEqual(self.backend.profile("friend")["mbti"], "ESTJ")

        room = "room@chatroom"
        self.source.add(room, 100, sender="member")
        self.source.add(room, 100, start=100, sender="someone")
        self.source.add(room, 100, start=200, sender="me")
        self.run_job(room, "history", 300)
        self.assertIsNone(self.backend.profile(room)["mbtiInference"])
        member = self.backend.profile(room, "member")
        other = self.backend.profile(room, "someone")
        self.assertEqual((member["mbti"], other["mbti"]), ("ESTJ", "INFP"))
        self.assertEqual((member["mbtiInference"]["eligibleMessages"],
                          other["mbtiInference"]["eligibleMessages"]), (100, 100))
        self.assertIsNone(self.backend.profile(room, "me")["mbti"])
        self.assertIsNone(member["affinity"])

    def test_analysis_version_isolation_incremental_and_restart(self):
        self.source.add("friend", 20)
        self.run_job("friend", limit=20)
        self.assertEqual(len(self.analyzer.calls), 20)
        self.analyzer.version = "synthetic-catalog+questions-v2"
        self.assertEqual(self.backend.analysis("friend")["affinityCount"], 0)
        self.assertEqual(self.backend.profile("friend")["mbtiInference"]["version"], self.analyzer.version)
        self.run_job("friend", limit=20)
        self.assertEqual(len(self.analyzer.calls), 40)
        self.source.add("friend", 1, start=20)
        self.run_job("friend", limit=21)
        self.assertEqual(len(self.analyzer.calls), 41)
        restarted = Backend(self.source, self.analyzer, self.store_factory)
        restarted.start("friend", "recent", 21)
        restarted.tasks.join()
        self.assertEqual(len(self.analyzer.calls), 41)
        self.analyzer.version = "synthetic-catalog+questions-v1"
        self.assertEqual(restarted.analysis("friend")["affinityCount"], 20)

    def test_invalid_personality_evidence_is_retryable(self):
        self.source.add("friend", 1)
        self.analyzer.evidence_for = lambda _message: {"EI": {"E": 1, "I": 0, "insufficient": 0}}
        self.assertEqual(self.run_job("friend")["status"], "error")
        self.assertEqual(self.backend.analysis("friend")["results"], {})
        self.analyzer.evidence_for = lambda _message: personality_evidence()
        self.assertEqual(self.run_job("friend")["status"], "done")
        self.assertEqual(self.backend.profile("friend")["mbtiInference"]["eligibleMessages"], 1)

class ForecastTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = SyntheticSource(Path(self.temp.name))
        self.analyzer = SyntheticAnalyzer()
        self.store_path = Path(self.temp.name) / "results.sqlite3"
        self.backend = Backend(self.source, self.analyzer,
                               lambda _account, _workdir: ResultStore(self.store_path))
        self.source.add("friend", 2)

    def test_account_message_draft_and_version_isolation_without_persistent_write(self):
        latest = self.source.rows["friend"][-1]["id"]
        first = self.backend.predict_reply("friend", "account-a", "ui-1", "你好", latest)
        self.assertEqual((first["user"], first["account"], first["requestId"]),
                         ("friend", "account-a", "ui-1"))
        self.assertEqual(len(first["candidates"]), 3)
        self.assertEqual(len(self.analyzer.forecast_calls), 1)
        self.assertFalse(self.store_path.exists(), "prediction must not create a result database")
        repeated = self.backend.predict_reply("friend", "account-a", "ui-2", "你好", latest)
        self.assertEqual(repeated["basisFingerprint"], first["basisFingerprint"])
        self.assertEqual(repeated["requestId"], "ui-2")
        self.assertEqual(len(self.analyzer.forecast_calls), 1)
        self.backend.predict_reply("friend", "account-a", "ui-3", "换个草稿", latest)
        self.assertEqual(len(self.analyzer.forecast_calls), 2)
        self.analyzer.version = "new-forecast-version"
        self.backend.predict_reply("friend", "account-a", "ui-4", "你好", latest)
        self.assertEqual(len(self.analyzer.forecast_calls), 3)
        self.source.rows["friend"][-1]["text"] = "edited with the same id"
        self.backend.predict_reply("friend", "account-a", "ui-5", "你好", latest)
        self.assertEqual(len(self.analyzer.forecast_calls), 4)
        self.source.add("friend", 1, start=2)
        with self.assertRaises(ForecastRequestError) as stale:
            self.backend.predict_reply("friend", "account-a", "ui-6", "你好", latest)
        self.assertEqual(stale.exception.code, "stale-message")
        self.source.account = "account-b"
        with self.assertRaises(ForecastRequestError) as mismatch:
            self.backend.predict_reply("friend", "account-a", "ui-7")
        self.assertEqual(mismatch.exception.code, "account-changed")

    def test_failure_and_mid_inference_edit_do_not_cache_success(self):
        self.analyzer.forecast_fail = True
        with self.assertRaisesRegex(RuntimeError, "synthetic forecast failure"):
            self.backend.predict_reply("friend", "account-a", "r1")
        self.assertEqual(len(self.backend.forecast_cache), 0)
        self.analyzer.forecast_hook = lambda: self.source.rows["friend"][-1].update(text="changed while inferring")
        with self.assertRaises(ForecastRequestError) as stale:
            self.backend.predict_reply("friend", "account-a", "r2")
        self.assertEqual(stale.exception.code, "stale-message")
        self.assertEqual(len(self.backend.forecast_cache), 0)
        self.analyzer.forecast_hook = None
        result = self.backend.predict_reply("friend", "account-a", "r3")
        self.assertEqual(result["requestId"], "r3")
        self.assertEqual(len(self.analyzer.forecast_calls), 3)
        with self.assertRaises(ForecastRequestError) as group:
            self.backend.predict_reply("room@chatroom", "account-a", "r4", member="someone")
        self.assertEqual(group.exception.code, "group-unsupported")

    def test_long_context_and_same_key_inflight(self):
        self.source.rows["friend"][0]["text"] = "旧" * 4001
        self.source.rows["friend"][-1]["text"] = "完整的最新消息"
        entered = threading.Event()
        release = threading.Event()
        self.analyzer.forecast_hook = lambda: (entered.set(), release.wait(timeout=5))
        results, errors = [], []

        def run(request_id):
            try:
                results.append(self.backend.predict_reply("friend", "account-a", request_id))
            except Exception as exc:
                errors.append(exc)

        first = threading.Thread(target=run, args=("r1",))
        second = threading.Thread(target=run, args=("r2",))
        first.start()
        self.assertTrue(entered.wait(timeout=5))
        second.start()
        release.set()
        first.join(timeout=5)
        second.join(timeout=5)
        self.assertFalse(errors)
        self.assertEqual(len(results), 2)
        self.assertEqual(len(self.analyzer.forecast_calls), 1)
        self.assertEqual(self.analyzer.forecast_calls[0][1], [self.source.rows["friend"][-1]["id"]])
        self.source.rows["friend"][-1]["text"] = "新" * 4001
        with self.assertRaises(ForecastRequestError) as too_long:
            self.backend.predict_reply("friend", "account-a", "r3")
        self.assertEqual(too_long.exception.code, "content-too-long")
        self.assertEqual(len(self.analyzer.forecast_calls), 1)

    def test_http_route_echoes_ids_and_failure_status(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.backend))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            def post(payload):
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port)
                conn.request("POST", "/api/predict-reply", json.dumps(payload),
                             {"Content-Type": "application/json", "Host": f"127.0.0.1:{server.server_port}"})
                response = conn.getresponse()
                status, body = response.status, json.loads(response.read())
                conn.close()
                return status, body

            good = {"user": "friend", "account": "account-a", "requestId": "ui-1", "draft": "你好"}
            status, body = post(good)
            self.assertEqual(status, 200)
            self.assertEqual((body["user"], body["account"], body["requestId"]),
                             ("friend", "account-a", "ui-1"))
            self.assertEqual(len(body["basisFingerprint"]), 64)
            status, body = post({**good, "expectedLastMessageId": "old"})
            self.assertEqual((status, body["error"], body["requestId"]), (409, "stale-message", "ui-1"))
            status, body = post({**good, "user": "room@chatroom", "member": "someone"})
            self.assertEqual((status, body["error"]), (422, "group-unsupported"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class FineLabelRevisionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = SyntheticSource(Path(self.temp.name))
        self.analyzer = SyntheticAnalyzer()
        self.store_factory = lambda account, _workdir: ResultStore(Path(self.temp.name) / (account + ".sqlite3"))
        self.backend = Backend(self.source, self.analyzer, self.store_factory)

    def run_job(self, user, limit):
        self.backend.start(user, "recent", limit)
        self.backend.tasks.join()

    def test_unmarked_fine_response_keeps_previous_label(self):
        self.source.add("friend", 1)
        account, workdir, store = self.backend._scoped_identity()
        version = self.analyzer.analysis_version()
        item = self.source.rows["friend"][0]
        store.save_fine(account, "friend", version, item, {"intentLabel": "旧细标签"})
        original_analyze = self.analyzer.analyze
        self.analyzer.analyze = lambda session, messages, target, **_kwargs: original_analyze(
            session, messages, target)

        with self.assertRaisesRegex(RuntimeError, "fine label schema mismatch"):
            self.backend._analyze_item(account, "friend", version, store, [item], item,
                                       (account, workdir), fine=True)
        self.assertEqual(store.fine_view(account, "friend", version,
                         [item["id"]])[item["id"]]["intentLabel"], "旧细标签")

    def test_grounded_intent_is_validated_before_fine_save(self):
        self.source.add("friend", 1)
        account, workdir, store = self.backend._scoped_identity()
        version = self.analyzer.analysis_version()
        item = self.source.rows["friend"][0]
        original_analyze = self.analyzer.analyze

        def response_with(value):
            return lambda session, messages, target, **_kwargs: {
                **original_analyze(session, messages, target),
                "labelSchema": "generic-v9", "groundedIntent": value,
            }

        for malformed in (
            {"label": "喊老板", "evidenceKind": "action_request"},
            {"label": "greet", "evidenceKind": "progress_statement"},
            {"label": "greet", "evidenceKind": "greeting_phrase", "probability": 1},
            {"label": "greet"},
        ):
            with self.subTest(malformed=malformed):
                self.analyzer.analyze = response_with(malformed)
                with self.assertRaisesRegex(RuntimeError, "invalid grounded intent"):
                    self.backend._analyze_item(account, "friend", version, store, [item], item,
                                               (account, workdir), fine=True)
                self.assertEqual(store.fine_view(account, "friend", version, [item["id"]]), {})

        self.analyzer.analyze = response_with({"label": "greet", "evidenceKind": "greeting_phrase"})
        self.backend._analyze_item(account, "friend", version, store, [item], item,
                                   (account, workdir), fine=True)
        saved = store.fine_view(account, "friend", version, [item["id"]])[item["id"]]
        self.assertEqual(saved["groundedIntent"],
                         {"label": "greet", "evidenceKind": "greeting_phrase"})
        self.assertEqual(saved["labelSchema"], "generic-v9")

        for label, evidence_kind in (
            ("confirm", "short_acknowledgement"),
            ("inspect", "first_person_inspection"),
            ("explain", "process_explanation"),
        ):
            with self.subTest(label=label):
                grounded = {"label": label, "evidenceKind": evidence_kind}
                self.analyzer.analyze = response_with(grounded)
                self.backend._analyze_item(account, "friend", version, store, [item], item,
                                           (account, workdir), fine=True)
                saved = store.fine_view(account, "friend", version, [item["id"]])[item["id"]]
                self.assertEqual(saved["groundedIntent"], grounded)

        self.analyzer.analyze = response_with(None)
        self.backend._analyze_item(account, "friend", version, store, [item], item,
                                   (account, workdir), fine=True)
        self.assertIsNone(store.fine_view(account, "friend", version, [item["id"]])[item["id"]]
                          ["groundedIntent"])

    def test_legacy_fine_length_skip_is_terminal_and_visible(self):
        self.source.add("friend", 1)
        account, workdir, store = self.backend._scoped_identity()
        version = self.analyzer.analysis_version()
        item = self.source.rows["friend"][0]
        store.save_fine(account, "friend", version, item, {"intentLabel": "旧细标签"})
        store.skip_fine(account, "friend", version, item, "ObservedTextTooLongError")
        self.assertEqual(store.fine_known(account, "friend", version, [item["id"]]), {item["id"]})
        self.assertEqual(store.fine_view(account, "friend", version, [item["id"]])[item["id"]],
                         {"state": "skipped", "reason": "ObservedTextTooLongError"})
        for _ in range(2):
            list(self.backend._run_fine_recent(account, "friend", version, store, {},
                                               (account, workdir), 1))
        self.assertEqual(self.analyzer.calls, [])
        self.assertEqual(self.backend.analysis("friend")["results"][item["id"]]["state"], "skipped")
        store.save_fine(account, "friend", version, item,
                         {"intentLabel": "新细标签", "labelSchema": "generic-v9",
                         "groundedIntent": None})
        self.assertEqual(store.fine_view(account, "friend", version, [item["id"]])[item["id"]]
                         ["intentLabel"], "新细标签")

    def test_recent_fine_relabels_legacy_without_changing_portrait_state(self):
        self.source.add("friend", 7)
        self.run_job("friend", limit=7)
        self.backend.profile("friend")
        account, workdir, store = self.backend._scoped_identity()
        version = self.analyzer.analysis_version()
        items = self.source.rows["friend"]
        saved = store.recent(account, "friend", version, limit=7)

        # Simulate records written by previous label schemes. Message 6
        # has a current fine override and must not be reanalyzed.
        with store.connect() as conn:
            for index in (4, 5):
                item = items[index]
                current = {**saved[item["id"]],
                           "labelSchema": "generic-v5" if index == 4 else "generic-v7"}
                conn.execute("UPDATE results_v2 SET result=? WHERE account=? AND session=? AND id=? AND version=?",
                             (json.dumps(current, ensure_ascii=False), account, "friend", item["id"], version))
        for index in (0, 5):
            item = items[index]
            store.save_fine(account, "friend", version, item,
                            {**saved[item["id"]], "intentLabel": "旧细标签"})
        store.save_fine(account, "friend", version, items[2],
                        {**saved[items[2]["id"]], "intentLabel": "旧细标签",
                          "labelSchema": "generic-v4", "groundedIntent": None})
        store.save_fine(account, "friend", version, items[6],
                        {**saved[items[6]["id"]], "intentLabel": "新细标签",
                          "labelSchema": "generic-v9", "groundedIntent": None})
        store.skip_fine(account, "friend", version, items[3], "ObservedTextTooLongError")
        store.save_fine("account-b", "friend", version, items[2], {"intentLabel": "其它账号"})
        store.save_fine(account, "other-session", version, items[2], {"intentLabel": "其它会话"})
        store.save_fine(account, "friend", "other-version", items[2], {"intentLabel": "其它版本"})

        preserved_tables = ("results_v2", "progress_v1", "summary_v1", "profile_state_v1")
        with store.connect() as conn:
            before = {table: conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                      for table in preserved_tables}
        self.analyzer.calls.clear()
        original_analyze = self.analyzer.analyze

        def generic_analyze(session, messages, target, **_kwargs):
            return {**original_analyze(session, messages, target),
                     "labelSchema": "generic-v9", "intentLabel": "新细标签",
                    "groundedIntent": {"label": "status_report", "evidenceKind": "progress_statement"}}

        self.analyzer.analyze = generic_analyze
        self.backend.batch_engine = Mock(snapshot=lambda *_args: None)
        scope = (account, workdir)
        job = {}
        list(self.backend._run_fine_recent(account, "friend", version, store, job, scope, 5))

        self.assertEqual((job["total"], job["processed"]), (5, 5))
        self.assertEqual({target for _session, target, _context in self.analyzer.calls},
                         {items[index]["id"] for index in (2, 4, 5)})
        self.assertEqual(store.fine_known(account, "friend", version,
                                          [item["id"] for item in items[2:]]),
                         {item["id"] for item in items[2:]})
        self.analyzer.calls.clear()
        list(self.backend._run_fine_recent(account, "friend", version, store, {}, scope, 5))
        self.assertEqual(self.analyzer.calls, [])
        current = self.backend.analysis("friend")["results"]
        for index in (2, 4, 5):
            self.assertEqual((current[items[index]["id"]]["labelSchema"],
                              current[items[index]["id"]]["intentLabel"]),
                              ("generic-v9", "新细标签"))
            self.assertEqual(current[items[index]["id"]]["groundedIntent"],
                             {"label": "status_report", "evidenceKind": "progress_statement"})
        self.assertEqual(current[items[3]["id"]],
                         {"state": "skipped", "reason": "ObservedTextTooLongError"})
        self.assertNotIn("labelSchema", store.fine_view(account, "friend", version,
                         [items[0]["id"]])[items[0]["id"]])
        self.assertEqual(store.fine_view("account-b", "friend", version,
                         [items[2]["id"]])[items[2]["id"]]["intentLabel"], "其它账号")
        self.assertEqual(store.fine_view(account, "other-session", version,
                         [items[2]["id"]])[items[2]["id"]]["intentLabel"], "其它会话")
        self.assertEqual(store.fine_view(account, "friend", "other-version",
                         [items[2]["id"]])[items[2]["id"]]["intentLabel"], "其它版本")
        with store.connect() as conn:
            after = {table: conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                     for table in preserved_tables}
        self.assertEqual(after, before)


class QuotedBackfillTests(unittest.TestCase):
    class Source(SyntheticSource):
        def quoted_history_page(self, user, ceiling, after=None, page_size=64, member=None):
            rows = [item for item in self.rows.get(user, []) if item.get("quotedCandidate") and
                    tuple(item["_sort"]) <= ceiling and
                    (after is None or tuple(item["_sort"]) > after) and
                    (member is None or item["senderId"] == member)]
            rows = sorted(rows, key=lambda item: item["_sort"])[:page_size]
            return rows, tuple(rows[-1]["_sort"]) if rows else None

        def preceding_text_context(self, user, before, limit=3):
            rows = [item for item in self.rows.get(user, []) if item["kind"] == "text" and
                    tuple(item["_sort"]) < before]
            return [{"id": item["id"], "side": item["side"], "text": item["text"]}
                    for item in sorted(rows, key=lambda item: item["_sort"])[-limit:]]

        def stats(self, user, member=None):
            rows = self.rows.get(user, [])
            selected = [item for item in rows if item["senderId"] == member] if member else rows
            members = [{"id": sender, "name": sender, "avatar": ""}
                       for sender in sorted({item["senderId"] for item in rows})]
            return len(selected), sum(item["kind"] == "text" for item in selected), members

    class Analyzer(SyntheticAnalyzer):
        def analyze_batch(self, session, payload, context):
            item = payload[0]
            self.calls.append((session, item["id"], [part["text"] for part in context]))
            if item["text"] == "quote" and getattr(self, "fail_quote_once", False):
                self.fail_quote_once = False
                raise RuntimeError("synthetic quote failure")
            score = {"first": .1, "quote": .5, "middle": .7, "last": .9}.get(item["text"], .5)
            result = None
            consumed = payload if getattr(self, "consume_all", False) else payload[:1]
            if any(part["target"] for part in consumed):
                result = {"emotion": [{"label": "平静", "rawLabel": "neutral", "probability": 1}],
                          "intent": [{"label": "提问", "probability": 1}],
                          "intentBroad": [{"label": "提问", "probability": 1}],
                          "score": score, "personalityEvidence": personality_evidence()}
            return {"consumed": [{"start": part["offset"], "end": len(part["text"]),
                                  "complete": True} for part in consumed],
                    "result": result, "durationMs": 1}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source = self.Source(Path(self.temp.name))
        self.analyzer = self.Analyzer()
        self.store_factory = lambda account, _workdir: ResultStore(Path(self.temp.name) / (account + ".sqlite3"))
        self.backend = Backend(self.source, self.analyzer, self.store_factory)

    def test_completed_personal_cursor_backfills_only_missing_quote_once(self):
        self.source.add("friend", 3)
        rows = self.source.rows["friend"]
        for item, text in zip(rows, ("first", "quote", "last")):
            item["text"] = text
        rows[1]["quotedCandidate"] = True
        rows[1]["kind"] = "other"
        self.backend.profile("friend")
        self.backend.tasks.join()
        before = self.backend.batch_engine.snapshot("account-a", "friend", self.analyzer.version,
                                                    self.store_factory("account-a", self.source.workdir))
        self.assertEqual((before["cursor"], before["state"]["count"]),
                         (tuple(rows[-1]["_sort"]), 2))
        rows[1]["kind"] = "text"
        self.analyzer.calls.clear()
        self.backend.profile("friend")
        self.backend.tasks.join()
        profile = self.backend.profile("friend")
        after = self.backend.batch_engine.snapshot("account-a", "friend", self.analyzer.version,
                                                   self.store_factory("account-a", self.source.workdir))
        self.assertEqual((profile["stats"]["analyzedCount"], after["cursor"]),
                         (3, before["cursor"]))
        self.assertEqual([target for _session, target, _context in self.analyzer.calls], [rows[1]["id"]])
        self.assertEqual(after["state"]["scoreWeighted"], .1 * 0 + .5 * 1 + .9 * 2)
        self.assertEqual(after["state"]["axes"]["EI"][2], 3)
        self.analyzer.calls.clear()
        restarted = Backend(self.source, self.analyzer, self.store_factory)
        restarted.profile("friend")
        restarted.tasks.join()
        self.assertEqual(self.analyzer.calls, [])

    def test_group_and_existing_member_receive_only_their_quote(self):
        room = "room@chatroom"
        self.source.add(room, 1, start=0, sender="member-a", text="first")
        self.source.add(room, 1, start=1, sender="member-a", text="quote")
        self.source.add(room, 1, start=2, sender="member-b", text="last")
        self.source.add(room, 1, start=3, sender="me", text="self quote")
        rows = self.source.rows[room]
        rows[1]["quotedCandidate"] = True
        rows[1]["kind"] = "other"
        rows[3]["quotedCandidate"] = True
        rows[3]["kind"] = "other"
        self.backend.profile(room)
        self.backend.tasks.join()
        self.backend.profile(room, "member-a")
        self.backend.tasks.join()
        store = self.store_factory("account-a", self.source.workdir)
        member_before = self.backend.batch_engine.snapshot("account-a", room, self.analyzer.version,
                                                           store, "member-a")
        self.assertEqual((member_before["cursor"], member_before["state"]["count"]),
                         (tuple(rows[-1]["_sort"]), 1))
        # The previous app version had no quoted-reply migration ledger.
        with store.connect() as conn:
            conn.execute("DELETE FROM quoted_backfill_v1")
        rows[1]["kind"] = "text"
        rows[3]["kind"] = "text"
        self.analyzer.calls.clear()
        self.backend.profile(room)
        self.backend.profile(room, "member-a")
        self.backend.tasks.join()
        member_after = self.backend.batch_engine.snapshot("account-a", room, self.analyzer.version,
                                                          store, "member-a")
        self.assertEqual(member_after["state"]["count"], 2,
                         self.backend.batch_engine.member_jobs.get(
                             ("account-a", str(store.path), room, self.analyzer.version, "member-a")))
        whole = self.backend.profile(room)
        member = self.backend.profile(room, "member-a")
        self.assertEqual(whole["stats"]["analyzedCount"], 4)
        self.assertEqual(member["stats"]["analyzedCount"], 2, member.get("job"))
        self.assertEqual([target for _session, target, _context in self.analyzer.calls],
                         [rows[1]["id"], rows[1]["id"]])

    def test_failed_quote_resumes_without_reanalyzing_prior_history(self):
        self.source.add("friend", 3)
        quote = self.source.rows["friend"][1]
        quote["text"] = "quote"
        quote["quotedCandidate"] = True
        quote["kind"] = "other"
        self.backend.profile("friend")
        self.backend.tasks.join()
        quote["kind"] = "text"
        self.analyzer.calls.clear()
        self.analyzer.fail_quote_once = True
        self.backend.profile("friend")
        self.backend.tasks.join()
        self.assertEqual(self.backend.analysis("friend")["job"]["status"], "error")
        restarted = Backend(self.source, self.analyzer, self.store_factory)
        restarted.profile("friend")
        restarted.tasks.join()
        profile = restarted.profile("friend")
        self.assertEqual(profile["stats"]["analyzedCount"], 3)
        self.assertEqual([target for _session, target, _context in self.analyzer.calls],
                         [quote["id"], quote["id"]])

    def test_many_old_quotes_share_bounded_model_batches(self):
        room = "room@chatroom"
        self.source.add(room, 268, sender="member-a")
        rows = self.source.rows[room]
        for item in rows:
            item["quotedCandidate"] = True
            item["kind"] = "other"
        self.backend.profile(room)
        self.backend.profile(room, "member-a")
        self.backend.tasks.join()
        self.assertEqual(self.analyzer.calls, [])
        store = self.store_factory("account-a", self.source.workdir)
        with store.connect() as conn:
            conn.execute("DELETE FROM quoted_backfill_v1")
        for item in rows:
            item["kind"] = "text"
        self.analyzer.consume_all = True
        self.backend.profile(room)
        self.backend.profile(room, "member-a")
        self.backend.tasks.join()
        self.assertEqual(self.backend.profile(room)["stats"]["analyzedCount"], 268)
        self.assertEqual(self.backend.profile(room, "member-a")["stats"]["analyzedCount"], 268)
        self.assertEqual(len(self.analyzer.calls), 50)

    def test_one_quote_batch_keeps_interleaved_old_score_order(self):
        self.source.add("friend", 5)
        rows = self.source.rows["friend"]
        for item, text in zip(rows, ("first", "quote", "middle", "quote", "last")):
            item["text"] = text
        for index in (1, 3):
            rows[index]["quotedCandidate"] = True
            rows[index]["kind"] = "other"
        self.backend.profile("friend")
        self.backend.tasks.join()
        previous = self.backend.batch_engine.snapshot("account-a", "friend", self.analyzer.version,
                                                      self.store_factory("account-a", self.source.workdir))
        for index in (1, 3):
            rows[index]["kind"] = "text"
        self.analyzer.consume_all = True
        self.analyzer.calls.clear()
        self.backend.profile("friend")
        self.backend.tasks.join()
        current = self.backend.batch_engine.snapshot("account-a", "friend", self.analyzer.version,
                                                     self.store_factory("account-a", self.source.workdir))
        self.assertEqual(len(self.analyzer.calls), 1)
        self.assertEqual(current["cursor"], previous["cursor"])
        self.assertEqual(current["state"]["scoreWeighted"],
                         .1 * 0 + .5 * 1 + .7 * 2 + .5 * 3 + .9 * 4)
        self.assertEqual(current["state"]["moodCount"], 5)


if __name__ == "__main__":
    unittest.main()
