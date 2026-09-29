"""Reader checks must remain correct without repeating ownership work per page."""
import unittest

import qq_client
from real_backend import MessagesUnavailableError, QQSource


class Client:
    """Injected-reader stub that counts the calls each read makes."""

    def __init__(self, ready=True):
        self.account = "synthetic"
        self.ready = ready
        self.self_info_calls = 0
        self.history_page_calls = 0
        self.profile_metadata_calls = 0

    def self_info(self):
        if not self.ready:
            raise qq_client.ReaderUnavailable("partial reader")
        self.self_info_calls += 1
        return {"account": self.account, "uid": self.account, "uin": "10001",
                "nickname": "Self"}

    def contacts(self):
        return {"friends": [], "groups": []}

    def senders(self, _peer, is_group=False):
        return []

    def history_page(self, _peer, *, ceiling=None, after=None, limit=1000, is_group=False):
        self.history_page_calls += 1
        return {"messages": [], "nextAfter": None}

    def profile_metadata(self, _peer, member=None, is_group=False):
        self.profile_metadata_calls += 1
        return {"counts": {}, "total": 0, "count": 0, "textCount": 0}

    def reset(self):
        pass


class PortraitReaderTests(unittest.TestCase):
    def test_each_history_page_validates_once_and_reuses_its_reader(self):
        client = Client()
        source = QQSource(client=client)
        self.assertEqual(source.history_page("friend", (1, "qq", 1)), ([], None))
        self.assertEqual(client.self_info_calls, 1)
        # The next page must still validate the current reader.
        self.assertEqual(source.history_page("friend", (2, "qq", 2)), ([], None))
        self.assertEqual(client.self_info_calls, 2)

    def test_partial_reader_never_reads_message_rows(self):
        for operation in (lambda source: source.history_page("friend", (1, "qq", 1)),
                          lambda source: source.history_page("friend", None),
                          lambda source: source.profile_overview("room@chatroom")):
            client = Client(ready=False)
            source = QQSource(client=client)
            with self.assertRaises(MessagesUnavailableError):
                operation(source)
            self.assertEqual(client.history_page_calls, 0)
            self.assertEqual(client.profile_metadata_calls, 0)

    def test_group_overview_validates_once(self):
        client = Client()
        source = QQSource(client=client)
        result = source.profile_overview("room@chatroom")
        self.assertEqual(result["count"], 0)
        self.assertEqual(client.self_info_calls, 1)
        self.assertEqual(client.profile_metadata_calls, 1)


if __name__ == "__main__":
    unittest.main()