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

## API

### `GET /health`

```json
{"ok": true, "service": "ntqq-reader", "db_dir": "...", "databases": ["nt_msg", "..."], "open": []}
```

### `POST /query`

```json
{"db": "nt_msg", "sql": "SELECT * FROM group_msg_table WHERE \"40027\" = ? LIMIT 10", "params": [123456]}
```

Response:

```json
{"columns": ["40001", "40030"], "rows": [[1, 123456]]}
```

SQLite `BLOB` values are returned as lowercase hex strings, so the caller can
decode the protobuf message body without relying on JSON byte semantics.

## Safety

- Every connection is opened `SQLITE_OPEN_READ_ONLY`. The live QQ database
  cannot be modified through this process.
- Only a fixed allowlist of files under `--db-dir` is reachable; paths are never
  taken from the request.
- Only `SELECT` / `WITH` / `EXPLAIN` statements are accepted, and `rusqlite`
  rejects multi-statement input.
- Binds to loopback by default.

## Licence

MIT. Decryption is provided by [`ntdb_unwrap`](https://github.com/artiga033/ntdb_unwrap)
(MIT) and SQLCipher, bundled through `rusqlite`.
