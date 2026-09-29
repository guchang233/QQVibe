"""Unknown upstream message elements must not abort a whole chat preload batch."""

import http.client
import json
import threading
import unittest
from http.server import ThreadingHTTPServer

import qq_client
from real_backend import QQSource
from real_http import make_handler


def message(msg_id, *, seq, text, kind="text", type_="textElement", side="other", sender=None):
    return {"msgId": str(msg_id), "msgTime": 1000 + seq, "msgSeq": seq, "side": side,
            "kind": kind, "type": type_, "text": text,
            "senderId": sender or "peer", "senderName": sender or "peer", "avatar": "",
            "sort": [1000 + seq, "qq", seq]}


class Client:
    """Injected-reader stub returning fixture rows per peer."""

    def __init__(self):
        self.account = "synthetic-account"
        self.rows = {}

    def add(self, peer, row):
        self.rows.setdefault(peer, []).append(row)

    def self_info(self):
        return {"account": self.account, "uid": "me", "uin": "10001", "nickname": "Self"}

    def contacts(self):
        return {"friends": [], "groups": []}

    def sessions(self):
        return {"self": {"username": "me", "name": "Self"}, "sessions": [],
                "account": self.account, "messagesReady": True}

    def messages(self, peer, *, is_group=False, limit=80, offset=0, order="latest"):
        rows = sorted(self.rows.get(peer, []), key=lambda item: item["sort"])
        window = rows[-(limit + offset):len(rows) - offset if offset else None]
        return {"messages": window[-limit:], "hasMoreBefore": len(rows) > limit + offset}

    def reset(self):
        pass


class QqMessageWindowTests(unittest.TestCase):
    def test_unknown_element_is_a_nontext_placeholder(self):
        client = Client()
        client.add("known", message(1, seq=1, text="你好", sender="known"))
        client.add("odd", message(1, seq=1, text="[42]", kind="other",
                                  type_="unknownElement", sender="odd"))
        source = QQSource(client=client)
        windows = source.message_windows(["known", "odd"], 80,
                                         expected_account="synthetic-account")
        self.assertEqual(windows["known"][0]["kind"], "text")
        self.assertEqual(windows["known"][0]["text"], "你好")
        self.assertEqual(windows["odd"][0]["kind"], "other")
        self.assertEqual(windows["odd"][0]["text"], "[42]")
        self.assertIs(windows.has_more_before["odd"], False)

        for seq in range(2, 83):
            client.add("known", message(seq, seq=seq, text="后续消息", sender="known"))
        expanded = source.message_windows(["known", "odd"], 80,
                                          expected_account="synthetic-account")
        self.assertIs(expanded.has_more_before["known"], True)
        self.assertEqual(len(expanded["known"]), 80)
        self.assertIs(source.messages("known", 80).has_more_before, True)

    def test_273_session_http_preload_continues_after_first_64(self):
        users = [f"contact{index:03d}" for index in range(273)]
        client = Client()
        for index, user in enumerate(users, 1):
            if index == 65:
                client.add(user, message(1, seq=1, text="[42]", kind="other",
                                         type_="unknownElement", sender=user))
            else:
                client.add(user, message(1, seq=1, text="你好", sender=user))
        source = QQSource(client=client)

        class BatchBackend:
            def message_windows(self, account, batch):
                windows = source.message_windows(batch, 80, expected_account=account)
                return {"account": account, "windows": [
                    {"user": user, "messages": [
                        {key: value for key, value in item.items() if not key.startswith("_")}
                        for item in windows[user]], "hasMoreBefore": windows.has_more_before[user]}
                    for user in batch]}

        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(BatchBackend()))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for offset in range(0, len(users), 64):
                batch = users[offset:offset + 64]
                body = json.dumps({"account": "synthetic-account", "users": batch}).encode()
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=10)
                try:
                    conn.request("POST", "/api/messages/batch", body,
                                 {"Content-Type": "application/json"})
                    response = conn.getresponse()
                    payload = json.loads(response.read())
                finally:
                    conn.close()
                self.assertEqual(response.status, 200, (offset, payload))
                self.assertEqual([item["user"] for item in payload["windows"]], batch)
                if offset == 64:
                    unknown = payload["windows"][0]["messages"][0]
                    self.assertEqual((unknown["kind"], unknown["text"]), ("other", "[42]"))
                    self.assertIs(payload["windows"][0]["hasMoreBefore"], False)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()