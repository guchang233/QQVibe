"""End-to-end tests for the QQ source over the ntqq-reader sidecar.

The sidecar's HTTP contract is reproduced by the fake in test_ntqq_reader, so
these tests drive the real chain:

    QQSource -> NtMsgDb -> ReaderClient -> HTTP -> sqlite3

Nothing here needs a QQ installation, a passphrase from a running QQ, the Rust
toolchain, or NapCat. The previous version of this file spun up a fake OneBot
server instead, which is no longer a code path the source can take.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from history_browser import browse as browse_history, search as search_history  # noqa: E402
import ntqq_reader as nrt  # noqa: E402
from qq_source import GROUP_SUFFIX, QQSource  # noqa: E402
from test_ntqq_reader import FakeSidecar, TempDir  # noqa: E402

SELF_UIN = "10001"
FRIEND_UIN = "20001"
GROUP_ID = "888001"
GROUP_MEMBERS = {
    SELF_UIN: "我自己",
    FRIEND_UIN: "小明",
    "20002": "群友阿红",
}
FRIEND_TOTAL = 450
GROUP_TOTAL = 60
BASE_TIME = 1_700_000_000

# Message-body field numbers, from ntdb_unwrap's message.proto. The blob in
# column 40800 is a Message whose messages field repeats the SingleMessage, so
# the body has to be wrapped the same way QQ stores it.
FIELD_MESSAGES = 40800
FIELD_MESSAGE_TYPE = 45002
FIELD_MESSAGE_TEXT = 45101
FIELD_IMAGE_URL = 45804

TYPE_IMAGE = 2


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def _int_field(number: int, value: int) -> bytes:
    return _varint((number << 3) | 0) + _varint(value)


def _bytes_field(number: int, raw: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(raw)) + raw


def image_body(url: str) -> bytes:
    """A Message carrying a single SingleMessage of type image."""
    single = (_int_field(FIELD_MESSAGE_TYPE, TYPE_IMAGE) +
              _bytes_field(FIELD_IMAGE_URL, url.encode()))
    return _bytes_field(FIELD_MESSAGES, single)


class _LocalBackend:
    """The whole contract QQSource asks of a local backend.

    The real Coordinator discovers the nt_msg.db directory and starts the
    sidecar process; here the client is already attached to a fake, so all that
    is left is handing back the account and a database reader.
    """

    def __init__(self, client, uin):
        self._client = client
        self._uin = uin

    def ensure(self):
        return self._uin, nrt.NtMsgDb(self._client)


def seed_friend(sidecar):
    for index in range(FRIEND_TOTAL):
        mine = index % 3 == 0
        sidecar.seed_c2c(
            msg_id=5000 + index,
            peer=int(FRIEND_UIN),
            sender=int(SELF_UIN if mine else FRIEND_UIN),
            seconds=BASE_TIME + index * 2,
            text=f"我的回复{index} 好的" if mine else f"朋友消息{index} 你好",
            direction=1 if mine else 0,
            nick=GROUP_MEMBERS[SELF_UIN] if mine else GROUP_MEMBERS[FRIEND_UIN],
        )


def seed_group(sidecar):
    senders = [SELF_UIN, FRIEND_UIN, "20002"]
    for index in range(GROUP_TOTAL):
        sender = senders[index % 3]
        sidecar.seed_group(
            msg_id=9000 + index,
            group=int(GROUP_ID),
            seq=index + 1,
            sender=int(sender),
            seconds=BASE_TIME + 100_000 + index,
            text=f"群里说话{index}",
            direction=1 if sender == SELF_UIN else 0,
            card=GROUP_MEMBERS[sender],
            nick=GROUP_MEMBERS[sender],
        )
    # Give a fifth of the group history an image body, so the renderer's
    # image path is covered rather than only inferred.
    for index in range(0, GROUP_TOTAL, 5):
        sidecar.conn.execute(
            'UPDATE group_msg_table SET "40800"=? WHERE "40001"=?',
            (image_body("https://example.com/x.png"), 9000 + index))
    sidecar.conn.commit()


class QQSourceSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = TempDir()
        # The source materializes messages into a workdir keyed by account. The
        # default lives in the system temp directory and keeps its shards across
        # runs, so without this the totals asserted below would depend on
        # whatever last wrote a shard for this uin.
        cls.root = cls.store.path / "accounts"
        cls.root.mkdir(parents=True, exist_ok=True)
        module = sys.modules["qq_source"]
        cls._saved_root = module.WORKDIR_ROOT
        module.WORKDIR_ROOT = cls.root

        cls.sidecar = FakeSidecar(cls.store.path)
        seed_friend(cls.sidecar)
        seed_group(cls.sidecar)
        cls.client = nrt.ReaderClient(cls.store.path, "test-passphrase",
                                      base_url=cls.sidecar.url)
        cls.client.start()
        cls.source = QQSource(local_backend=_LocalBackend(cls.client, SELF_UIN))

    @classmethod
    def tearDownClass(cls):
        cls.source.close()
        cls.client.close()
        cls.sidecar.stop()
        sys.modules["qq_source"].WORKDIR_ROOT = cls._saved_root
        cls.store.close()

    def test_01_identity(self):
        account, workdir = self.source.verified_identity(messages=True)
        self.assertEqual(account, SELF_UIN)
        self.assertTrue(workdir.is_dir())

    def test_02_sessions(self):
        data = self.source.sessions()
        self.assertEqual(data["account"], SELF_UIN)
        self.assertTrue(data["messagesReady"])
        users = [item["username"] for item in data["sessions"]]
        self.assertIn(FRIEND_UIN, users)
        self.assertIn(GROUP_ID + GROUP_SUFFIX, users)

        group = next(item for item in data["sessions"]
                     if item["username"] == GROUP_ID + GROUP_SUFFIX)
        self.assertTrue(group["isGroup"])
        # A group's display name is its id: the sidecar reads names for people,
        # and nt_msg.db carries no group title of its own.
        self.assertEqual(group["name"], GROUP_ID)
        self.assertIn("群里说话59", group["preview"])

        friend = next(item for item in data["sessions"] if item["username"] == FRIEND_UIN)
        # Resolved from the newest 40093 on that conversation.
        self.assertEqual(friend["name"], GROUP_MEMBERS[FRIEND_UIN])

    def test_03_messages_window(self):
        window = self.source.messages(FRIEND_UIN, 80)
        self.assertEqual(len(window), 80)
        self.assertTrue(all(a["time"] <= b["time"] for a, b in zip(window, window[1:])))
        self.assertIn("朋友消息449 你好", window[-1]["text"])
        self.assertIn("我的回复447 好的", window[-3]["text"])
        sides = {item["side"] for item in window}
        self.assertEqual(sides, {"self", "other"})
        self.assertTrue(window.has_more_before)
        self.assertEqual(window[-1]["senderId"], FRIEND_UIN)
        self.assertEqual(window[-3]["senderId"], SELF_UIN)

    def test_04_highwater_and_history_walk(self):
        user = FRIEND_UIN
        highwater = self.source.history_highwater(user)
        self.assertEqual(highwater[1], "message__message_0.db")
        page, cursor = self.source.history_page(user, highwater, None, page_size=256)
        self.assertEqual(len(page), 256)
        self.assertTrue(all(a["time"] <= b["time"] for a, b in zip(page, page[1:])))
        self.assertEqual(page[0]["text"], "我的回复0 好的")
        total = list(page)
        while cursor is not None:
            page, cursor = self.source.history_page(user, highwater, cursor, page_size=256)
            if page:
                total.extend(page)
        self.assertEqual(len(total), FRIEND_TOTAL)
        ids = [item["id"] for item in total]
        self.assertEqual(len(ids), len(set(ids)))

    def test_05_browse_and_search(self):
        account, _workdir = self.source.identity()
        page = browse_history(self.source, account, FRIEND_UIN, limit=30)
        self.assertEqual(len(page["messages"]), 30)
        self.assertTrue(page["hasMoreBefore"])
        oldest = page["oldestCursor"]
        older = browse_history(self.source, account, FRIEND_UIN, before=oldest, limit=30)
        self.assertEqual(len(older["messages"]), 30)
        seen = {item["id"] for item in page["messages"]}
        self.assertFalse(seen & {item["id"] for item in older["messages"]})
        around = browse_history(self.source, account, FRIEND_UIN,
                                around=older["newestCursor"], limit=20)
        self.assertEqual(around["focusId"],
                         [item["id"] for item in older["messages"]][-1])
        found = search_history(self.source, account, FRIEND_UIN, query="朋友消息1")
        self.assertTrue(found["messages"])
        self.assertTrue(all("朋友消息1" in item["text"] for item in found["messages"]))

    def test_06_group_profile(self):
        user = GROUP_ID + GROUP_SUFFIX
        count, _text_count, members = self.source.stats(user)
        self.assertEqual(count, GROUP_TOTAL)
        names = {member["name"] for member in members}
        self.assertLessEqual({GROUP_MEMBERS[FRIEND_UIN], GROUP_MEMBERS["20002"]}, names)
        overview = self.source.profile_overview(user)
        self.assertEqual(overview["count"], GROUP_TOTAL)
        window = self.source.messages(user, 30)
        kinds = {item["kind"] for item in window}
        self.assertIn("image", kinds)
        self.assertIn("text", kinds)

    def test_07_preceding_context(self):
        user = FRIEND_UIN
        window = self.source.messages(user, 10)
        context = self.source.preceding_text_context(user, tuple(window[0]["_sort"]), 3)
        self.assertEqual(len(context), 3)
        self.assertTrue(all(item["text"] for item in context))

    def test_08_texts_for_refs(self):
        user = FRIEND_UIN
        window = self.source.messages(user, 5)
        refs = [(item["_sort"][1], item["_sort"][0], item["id"]) for item in window[:2]]
        texts = self.source.texts_for_refs(user, refs, with_ids=True)
        self.assertEqual(len(texts), 2)
        self.assertEqual({stable for stable, _text in texts},
                         {item["id"] for item in window[:2]})


if __name__ == "__main__":
    unittest.main()
