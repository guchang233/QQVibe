# ntqq-reader 第三方来源与许可证

`sidecar/ntqq-reader` 是本项目自行实现的只读 HTTP 桥接进程（MIT，见 `Cargo.toml`）。它本身不含解密逻辑，全部困难部分来自下列上游 crate；本文件说明这份源码与构建产物直接使用的组件。

| 组件 | 锁定版本 | 许可证 | 用途 |
| --- | --- | --- | --- |
| [`ntdb_unwrap`](https://github.com/artiga033/ntdb_unwrap) | 0.3.3 | MIT | 解密 QQNT 的 SQLCipher 数据库并导出可读副本；`db::try_decrypt_db` 套用 page_size 4096 / kdf_iter 4000 / aes-256-cbc / PBKDF2_HMAC_SHA512，并在 HMAC_SHA256 与 HMAC_SHA1 之间自动回退 |
| `sqlite_ext_ntqq_db`（`ntdb_unwrap` 传递依赖） | 0.2.0 | MIT | 注册隐藏 QQNT 1024 字节自定义头的 SQLite VFS（`offset_vfs`） |
| `rusqlite` | 0.38 | MIT | SQLite 绑定；启用 `bundled-sqlcipher-vendored-openssl`，仅用于让 SQLCipher PRAGMA 可被接受 |
| SQLCipher / OpenSSL | 随 `libsqlite3-sys` / `openssl-src` 锁定 | BSD-style / Apache-2.0 | 静态链接进发布二进制，许可证文本随 crate 一同分发 |
| `tiny_http`、`serde`、`serde_json`、`env_logger` | 见 `Cargo.lock` | MIT / Apache-2.0 | 回环 HTTP 服务与请求/响应序列化 |

构建产物 `ntqq-reader.exe` 是自包含的：上述依赖均静态链接，不要求目标机器安装 OpenSSL 或 SQLCipher。
