"""Tests for the Rust-sidecar-backed QQ reader.

The sidecar's HTTP contract is reproduced by a fake server backed by a real
``sqlite3`` database, so the whole Python path -- SQL, protobuf bodies, page
cursors, error mapping -- is exercised on any machine, without the Rust
toolchain and without a QQ installation.

Run directly, as the project's other bridge tests are:
    python bridge/test_ntqq_reader.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ntqq_reader as nrt  # noqa: E402

BASE_TIME = 1_700_000_000


# ---------------------------------------------------------------------------
# protobuf helpers: build the `40800` bodies the client has to decode
# ---------------------------------------------------------------------------

def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(out)


def ld(field: int, payload: bytes) -> bytes:
    return varint((field << 3) | 2) + varint(len(payload)) + payload


def vi(field: int, value: int) -> bytes:
    return varint(field << 3) + varint(value)


def body(message_type: int, **strings) -> bytes:
    """A ``Message`` wrapping one ``SingleMessage``, as QQNT stores it."""
    single = vi(45002, message_type)
    for field, name in ((45101, "text"), (45804, "image"), (45402, "file"),
                        (47602, "emoji"), (48214, "notice")):
        if name in strings:
            single += ld(field, strings[name].encode("utf-8"))
    return ld(40800, single)


# ---------------------------------------------------------------------------
# a fake sidecar: same contract, real sqlite3 underneath
# ---------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    server_version = "fake-ntqq-reader/0.1"

    def log_message(self, *_args):  # keep the test output readable
        pass

    def _send(self, status: int, payload: dict) -> None:
        blob = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def do_GET(self):  # noqa: N802 - required name
        if self.path.split("?")[0] != "/health":
            self._send(404, {"error": "not found"})
            return
        self._send(200, {
            "ok": True,
            "service": "ntqq-reader",
            "version": "0.1.0",
            "db_dir": str(self.server.db_dir),
            "allow_write_open": False,
            "databases": [{"name": "nt_msg", "file": "nt_msg.db", "exists": True, "bytes": 1}],
            "open": [{"name": "nt_msg", "mode": "read_only"}],
        })

    def do_POST(self):  # noqa: N802 - required name
        if self.path.split("?")[0] != "/query":
            self._send(404, {"error": "not found"})
            return
        if self.server.token and self.headers.get("Authorization") != f"Bearer {self.server.token}":
            self._send(401, {"error": "unauthorized"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            request = json.loads(self.rfile.read(length).decode("utf-8"))
        except ValueError as exc:
            self._send(400, {"error": f"invalid request: {exc}"})
            return
        sql = str(request.get("sql") or "")
        if not sql.lstrip().lower().startswith(("select", "with", "explain")):
            self._send(400, {"error": "only SELECT / WITH / EXPLAIN statements are allowed"})
            return
        with self.server.lock:
            try:
                cursor = self.server.conn.execute(sql, tuple(request.get("params") or []))
                columns = [item[0] for item in (cursor.description or [])]
                rows = [[value.hex() if isinstance(value, bytes) else value for value in row]
                        for row in cursor.fetchall()]
            except sqlite3.Error as exc:
                self._send(400, {"error": f"query: {exc}"})
                return
        self._send(200, {"columns": columns, "rows": rows})


class FakeSidecar:
    """A running fake sidecar plus the database behind it."""

    def __init__(self, db_dir: Path, token: str = ""):
        self.db_dir = Path(db_dir)
        self.db_path = self.db_dir / "nt_msg.db"
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.executescript(
            'CREATE TABLE c2c_msg_table ("40001" INTEGER PRIMARY KEY, "40003" INTEGER,'
            ' "40010" INTEGER, "40013" INTEGER, "40030" INTEGER, "40033" INTEGER,'
            ' "40050" INTEGER, "40093" TEXT, "40800" BLOB);'
            'CREATE TABLE group_msg_table ("40001" INTEGER PRIMARY KEY, "40003" INTEGER,'
            ' "40010" INTEGER, "40013" INTEGER, "40030" INTEGER, "40033" INTEGER,'
            ' "40050" INTEGER, "40090" TEXT, "40093" TEXT, "40800" BLOB);'
        )
        self.conn.commit()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.db_dir = self.db_dir
        self.server.conn = self.conn
        self.server.lock = threading.Lock()
        self.server.token = token
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}"

    def seed_c2c(self, msg_id, peer, sender, seconds, text, direction=0, nick="nick"):
        self.conn.execute(
            'INSERT INTO c2c_msg_table VALUES (?,?,?,?,?,?,?,?,?)',
            (msg_id, 0, 1, direction, peer, sender, seconds, nick, body(1, text=text)))
        self.conn.commit()

    def seed_group(self, msg_id, group, seq, sender, seconds, text, direction=0,
                   card="card", nick="nick"):
        self.conn.execute(
            'INSERT INTO group_msg_table VALUES (?,?,?,?,?,?,?,?,?,?)',
            (msg_id, seq, 2, direction, group, sender, seconds, card, nick,
             body(1, text=text)))
        self.conn.commit()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.conn.close()


class TempDir:
    def __init__(self, prefix="ntqq-reader-test-"):
        self._dir = tempfile.TemporaryDirectory(prefix=prefix)

    @property
    def path(self) -> Path:
        return Path(self._dir.name)

    def close(self):
        self._dir.cleanup()


# ---------------------------------------------------------------------------
# protobuf body decoding
# ---------------------------------------------------------------------------

class MessageBodyTests(unittest.TestCase):
    def test_text_message(self):
        flag, text, image, system = nrt.parse_message_body(body(1, text="hello there"))
        self.assertEqual((flag, text, image, system), ("text", "hello there", "", False))

    def test_image_message_carries_the_url(self):
        flag, _text, image, system = nrt.parse_message_body(body(2, image="https://x/y.jpg"))
        self.assertEqual(flag, "image")
        self.assertEqual(image, "https://x/y.jpg")
        self.assertFalse(system)

    def test_notice_message_is_system(self):
        flag, text, _image, system = nrt.parse_message_body(body(8, notice="recalled"))
        self.assertTrue(system)
        self.assertEqual(text, "")

    def test_nested_reply_is_walked(self):
        inner = vi(45002, 1) + ld(45101, b"quoted")
        payload = ld(40800, vi(45002, 1) + ld(45101, b"reply text") + ld(47423, inner))
        flag, text, _image, system = nrt.parse_message_body(payload)
        self.assertEqual((flag, text, system), ("text", "reply text", False))

    def test_accepts_hex_and_bytes_alike(self):
        encoded = body(1, text="hex path")
        self.assertEqual(nrt.parse_message_body(encoded)[1], "hex path")
        self.assertEqual(nrt.parse_message_body(encoded.hex())[1], "hex path")

    def test_garbage_degrades_to_empty_text(self):
        for value in (b"", None, b"\xff\xff\xff", "zz", b"\x0a\xff"):
            with self.subTest(value=value):
                flag, text, image, system = nrt.parse_message_body(value)
                self.assertEqual((flag, text, image, system), ("text", "", "", False))

    def test_unknown_message_type_falls_back_to_card(self):
        flag, _text, _image, system = nrt.parse_message_body(body(999))
        self.assertEqual(flag, "card")
        self.assertFalse(system)

    def test_file_message_yields_its_name(self):
        _flag, text, _image, _system = nrt.parse_message_body(body(3, file="report.pdf"))
        self.assertEqual(text, "report.pdf")


# ---------------------------------------------------------------------------
# passphrase and database discovery
# ---------------------------------------------------------------------------

class PassphraseTests(unittest.TestCase):
    def test_reads_the_environment_variable(self):
        provider = nrt.NapCatPassphrase({nrt.PASSPHRASE_ENV: "  secret pass  "})
        self.assertEqual(provider.resolve(), "secret pass")
        self.assertTrue(provider.available())

    def test_falls_back_to_the_appdata_file(self):
        directory = TempDir()
        try:
            target = directory.path / "QQVibe"
            target.mkdir()
            (target / nrt.NapCatPassphrase.FILENAME).write_text("from file\n", encoding="utf-8")
            provider = nrt.NapCatPassphrase({"APPDATA": str(directory.path)})
            self.assertEqual(provider.resolve(), "from file")
        finally:
            directory.close()

    def test_environment_wins_over_the_file(self):
        directory = TempDir()
        try:
            target = directory.path / "QQVibe"
            target.mkdir()
            (target / nrt.NapCatPassphrase.FILENAME).write_text("from file\n", encoding="utf-8")
            provider = nrt.NapCatPassphrase(
                {"APPDATA": str(directory.path), nrt.PASSPHRASE_ENV: "from env"})
            self.assertEqual(provider.resolve(), "from env")
        finally:
            directory.close()

    def test_missing_passphrase_names_the_variable(self):
        directory = TempDir()
        try:
            provider = nrt.NapCatPassphrase({"APPDATA": str(directory.path)})
            with self.assertRaises(nrt.NtqqKeyMissing) as caught:
                provider.resolve()
            self.assertIn(nrt.PASSPHRASE_ENV, str(caught.exception))
            self.assertFalse(provider.available())
        finally:
            directory.close()


class DiscoveryTests(unittest.TestCase):
    def _store(self, root: Path, uin: str) -> Path:
        nt_db = root / uin / "nt_qq" / "nt_db"
        nt_db.mkdir(parents=True)
        (nt_db / "nt_msg.db").write_bytes(b"x")
        return nt_db

    def test_finds_a_relocated_store(self):
        base = TempDir()
        try:
            root = base.path / "Documents" / "Tencent Files"
            expected = self._store(root, "57262494")
            with mock.patch.dict(os.environ, {"USERPROFILE": str(base.path)}, clear=False):
                found = [str(path) for path in nrt.nt_db_dirs()]
            self.assertIn(str(expected), found)
        finally:
            base.close()

    def test_coordinator_picks_the_most_recently_written_store(self):
        base = TempDir()
        try:
            older = self._store(base.path, "11111111")
            newer = self._store(base.path, "22222222")
            now = time.time()
            os.utime(older / "nt_msg.db", (now - 600, now - 600))
            os.utime(newer / "nt_msg.db", (now, now))
            self.assertEqual(nrt.Coordinator._newest_db_dir([older, newer]), newer)
        finally:
            base.close()

    def test_missing_store_raises_lookup_error(self):
        base = TempDir()
        try:
            coordinator = nrt.Coordinator(passphrase="x", db_dir=base.path)
            with self.assertRaises(LookupError):
                coordinator.ensure()
        finally:
            base.close()


# ---------------------------------------------------------------------------
# the reader client and the `ntdb` role
# ---------------------------------------------------------------------------

class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.store = TempDir()
        self.sidecar = FakeSidecar(self.store.path)
        self.sidecar.seed_c2c(5001, 10001, 10001, BASE_TIME, "oldest c2c")
        self.sidecar.seed_c2c(5002, 10001, 20002, BASE_TIME + 10, "newer c2c")
        self.sidecar.seed_c2c(5003, 30003, 30003, BASE_TIME + 20, "other peer")
        self.sidecar.seed_group(9001, 888001, 10, 20002, BASE_TIME + 1, "group one",
                                card="card-a")
        self.sidecar.seed_group(9002, 888001, 11, 20003, BASE_TIME + 2, "group two",
                                card="card-b")
        self.client = nrt.ReaderClient(self.store.path, "pass", base_url=self.sidecar.url)
        self.client.start()
        self.db = nrt.NtMsgDb(self.client)

    def tearDown(self):
        self.client.close()
        self.sidecar.stop()
        self.store.close()

    def test_health_is_exposed(self):
        self.assertTrue(self.client.health["ok"])
        self.assertEqual(self.client.health["open"][0]["mode"], "read_only")

    def test_no_process_is_spawned_when_attaching(self):
        self.assertIsNone(self.client._process)
        self.assertTrue(self.client._attach)

    def test_table_columns_introspects_at_runtime(self):
        columns = self.client.table_columns("c2c_msg_table")
        self.assertIn(nrt.COL_CONTENT, columns)
        self.assertIn(nrt.COL_PEER, columns)
        self.assertNotIn("40090", columns)  # group-only column

    def test_sessions_covers_both_tables(self):
        rows = sorted(self.db.sessions(), key=lambda row: (row[1], row[0]))
        self.assertEqual(
            rows,
            [("10001", False, BASE_TIME + 10, 2),
             ("30003", False, BASE_TIME + 20, 1),
             ("888001", True, BASE_TIME + 2, 2)],
        )

    def test_sender_display_returns_the_latest_nickname(self):
        self.assertEqual(self.db.sender_display("10001", False), "nick")

    def test_sender_display_is_empty_for_an_unknown_peer(self):
        self.assertEqual(self.db.sender_display("424242", False), "")

    def test_newest_rows_is_newest_first_and_carries_the_body(self):
        rows = self.db.newest_rows("10001", False, 10)
        self.assertEqual([row[0] for row in rows], [5002, 5001])
        self.assertEqual(len(rows[0]), 8)
        flag, text, _image, _system = nrt.parse_message_body(rows[0][7])
        self.assertEqual((flag, text), ("text", "newer c2c"))

    def test_newest_rows_respects_the_limit(self):
        self.assertEqual(len(self.db.newest_rows("10001", False, 1)), 1)

    def test_older_rows_pages_backwards_by_group_sequence(self):
        # The group anchor is (40003, 40001); the page must be strictly older.
        rows = self.db.older_rows("888001", True, 11, 9002, 10)
        self.assertEqual([row[0] for row in rows], [9001])
        self.assertEqual(self.db.older_rows("888001", True, 10, 9001, 10), [])

    def test_older_rows_pages_backwards_by_message_id_for_c2c(self):
        rows = self.db.older_rows("10001", False, 0, 5002, 10)
        self.assertEqual([row[0] for row in rows], [5001])

    def test_active_peer_reports_the_busiest_conversation(self):
        self.assertEqual(self.db.active_peer(), "30003")

    def test_a_write_statement_is_refused(self):
        with self.assertRaises(nrt.NtqqReaderError) as caught:
            self.client.query("nt_msg", "DELETE FROM group_msg_table")
        self.assertIn("SELECT", str(caught.exception))

    def test_a_bad_query_surfaces_as_a_reader_error(self):
        with self.assertRaises(nrt.NtqqReaderError):
            self.client.query("nt_msg", "SELECT nope FROM c2c_msg_table")

    def test_unreachable_sidecar_is_reported(self):
        client = nrt.ReaderClient(self.store.path, "pass", base_url="http://127.0.0.1:1")
        with mock.patch.object(nrt, "START_TIMEOUT", 0.5):
            with self.assertRaises(nrt.NtqqReaderError):
                client.start()

    def test_bearer_token_is_required_when_configured(self):
        # Its own store: reuse would collide with setUp's tables.
        store = TempDir()
        token_sidecar = FakeSidecar(store.path, token="t0ken")
        try:
            anonymous = nrt.ReaderClient(store.path, "pass", base_url=token_sidecar.url)
            with self.assertRaises(nrt.NtqqReaderError) as caught:
                anonymous.query("nt_msg", "SELECT 1")
            self.assertIn("unauthorized", str(caught.exception))
            authorised = nrt.ReaderClient(store.path, "pass",
                                          base_url=token_sidecar.url, token="t0ken")
            self.assertTrue(authorised.query("nt_msg", "SELECT 1")["columns"])
        finally:
            token_sidecar.stop()
            store.close()

    def test_missing_sidecar_binary_is_reported_clearly(self):
        with mock.patch.object(nrt, "default_exe", return_value=None):
            os.environ.pop(nrt.SIDECAR_URL_ENV, None)
            with self.assertRaises(nrt.NtqqReaderError) as caught:
                nrt.ReaderClient(self.store.path, "pass")
        self.assertIn("ntqq-reader", str(caught.exception))


class CoordinatorTests(unittest.TestCase):
    def test_ensure_returns_the_uin_and_a_working_db(self):
        base = TempDir()
        # Build the store where the coordinator expects to find it, so the
        # database file is never moved out from under an open handle (which
        # Windows forbids).
        root = base.path / "111" / "nt_qq" / "nt_db"
        root.mkdir(parents=True)
        sidecar = FakeSidecar(root)
        try:
            sidecar.seed_c2c(1, 10001, 10001, BASE_TIME, "hello")
            coordinator = nrt.Coordinator(passphrase="pass", db_dir=root,
                                         base_url=sidecar.url)
            uin, db = coordinator.ensure()
            self.assertEqual(uin, "111")
            self.assertEqual(len(db.newest_rows("10001", False, 5)), 1)
            # ensure() is memoised: the same handle comes back.
            self.assertIs(coordinator.ensure()[1], db)
        finally:
            sidecar.stop()
            base.close()

    def test_refresh_interval_defaults_and_can_be_overridden(self):
        coordinator = nrt.Coordinator(passphrase="x")
        self.assertEqual(coordinator.refresh_interval(), nrt.NtMsgDb.REFRESH_SECONDS)
        with mock.patch.dict(os.environ, {"QQVIBE_NT_REFRESH_SECONDS": "7"}):
            self.assertEqual(coordinator.refresh_interval(), 7.0)
        with mock.patch.dict(os.environ, {"QQVIBE_NT_REFRESH_SECONDS": "not-a-number"}):
            self.assertEqual(coordinator.refresh_interval(), nrt.NtMsgDb.REFRESH_SECONDS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
