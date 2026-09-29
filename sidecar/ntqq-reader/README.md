# ntqq-reader

A small read-only HTTP bridge over QQNT's SQLCipher databases.

QQNT stores chat data in SQLCipher-encrypted files that start with a 1024-byte
custom header. [`ntdb_unwrap`](https://github.com/artiga033/ntdb_unwrap) ships an
SQLite VFS (`offset_vfs`) that transparently skips that header, so an
SQLCipher-enabled SQLite can open the **original** file in place — SQLCipher then
decrypts individual pages on demand. No plaintext copy is ever written to disk,
which matters because a real `nt_msg.db` can exceed 5 GB.

This binary wraps that mechanism in a loopback HTTP service so the Python bridge
never links against SQLCipher itself.

## Usage

```
ntqq-reader --db-dir "E:\Documents\Tencent Files\<uin>\nt_qq\nt_db" \
            --pkey <passphrase> \
            --listen 127.0.0.1:19552 \
            --token <token>
```

| Flag | Meaning |
| --- | --- |
| `--db-dir` | Directory containing `nt_msg.db` (required) |
| `--pkey` | SQLCipher passphrase for those databases (required) |
| `--listen` | Listen address, default `127.0.0.1:19552` |
| `--token` | When non-empty, require `Authorization: Bearer <token>` |
| `--allow-write-open` | Drop the write guard. Last resort; see *Safety* |

## API

### `GET /health`

```json
{
  "ok": true,
  "service": "ntqq-reader",
  "version": "0.1.0",
  "db_dir": "...",
  "allow_write_open": false,
  "databases": [{"name": "nt_msg", "file": "nt_msg.db", "exists": true, "bytes": 5619241984}],
  "open": [{"name": "nt_msg", "mode": "read_only"}]
}
```

`databases` is an inventory of the allowlist, so the caller can pick the right
account directory before issuing any query. `open` reports the guard actually in
force per connection — `read_only`, `read_write_query_only`, or (only with
`--allow-write-open`) `read_write`.

### `POST /query`

```json
{"db": "nt_msg", "sql": "SELECT * FROM group_msg_table WHERE \"40027\" = ? LIMIT 10", "params": [123456]}
```

Response:

```json
{"columns": ["40001", "40030"], "rows": [[1, 123456]]}
```

SQLite `BLOB` values — which is where the protobuf message body lives in column
`40800` — are returned as lowercase hex strings, so the caller can decode them
without relying on JSON byte semantics.

Errors are `{"error": "..."}` with status 400 (bad request, rejected statement)
or 401 (missing/incorrect bearer token).

Column names are the numeric QQNT ones. They are **not** frozen here on purpose:
the bridge stays a thin pass-through and the caller introspects the schema at
runtime, so a QQ update that adds a column cannot break the bridge.

## Safety

The database belongs to a running QQ client and holds years of the user's chat
history, so the defaults are deliberately conservative.

- Connections are opened `SQLITE_OPEN_READ_ONLY`. If SQLite refuses that — which
  it can for a WAL database whose `-shm` index is held by a live client — the
  bridge retries with a read-write handle pinned to `PRAGMA query_only = ON`, so
  SQLite itself rejects writes. Both fallback paths are covered by tests, and
  `/health` reports which one is in force. `--allow-write-open` is the only way
  to get a handle that may actually write, and it warns on startup.
- Only a fixed allowlist of files under `--db-dir` is reachable. Paths are never
  taken from the request, so a traversal string is simply an unknown database.
- Only `SELECT` / `WITH` / `EXPLAIN` statements are accepted; `rusqlite` also
  refuses multi-statement input.
- Binds to loopback by default; `--token` adds a bearer check on every route.
- Nothing is cached on disk: the pages SQLCipher decrypts live in memory only.

## Building and testing

The dependency tree cannot be built everywhere — `ntdb_unwrap`'s `build.rs`
drives rust-protobuf's code generator and therefore needs the `protoc` binary,
and `rusqlite`'s vendored SQLCipher compiles OpenSSL from source, which needs
Perl on Windows. CI (`.github/workflows/sidecar.yml`) installs both and runs the
full suite on `windows-latest`.

```
cargo test --release -- --test-threads=1 --nocapture
```

`--test-threads=1` is required: `register_offset_vfs` installs the VFS into a
process-wide static, so parallel tests would race it.

The suite covers the parts that cannot be checked by inspection:

| Test | What it pins down |
| --- | --- |
| `offset_vfs_skips_the_ntqq_header` | SQLite sees the file 1024 bytes shorter |
| `read_only_roundtrip_over_offset_vfs` | read-only open + decrypt + blob→hex |
| `unknown_hmac_algorithm_is_auto_detected` | SHA256 → SHA1 fallback |
| `wrong_key_is_rejected` | a bad key fails loudly |
| `guarded_connection_observes_a_live_wal_writer` | concurrent WAL reads work |
| `guarded_connection_refuses_writes_at_the_sqlite_level` | the guard holds |
| `only_allowlisted_databases_are_reachable` | traversal is unreachable |
| `write_http_fixture_when_requested` | generates the CI fixture |

CI then starts the built binary against a fixture database and talks to it over
HTTP, so the JSON contract the Python bridge consumes is verified end to end.

## Licence

MIT. Decryption is provided by [`ntdb_unwrap`](https://github.com/artiga033/ntdb_unwrap)
(MIT) and SQLCipher, bundled through `rusqlite`.
