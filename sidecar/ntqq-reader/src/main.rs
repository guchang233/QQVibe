//! ntqq-reader - read-only local HTTP bridge over QQNT's SQLCipher databases.
//!
//! QQNT keeps its chat data in SQLCipher-encrypted files that carry a 1024-byte
//! custom header. `ntdb_unwrap` provides an SQLite VFS (`offset_vfs`) that skips
//! that header, so an SQLCipher-enabled SQLite can open the original file in
//! place: SQLCipher decrypts individual pages on demand and no plaintext copy is
//! ever written to disk.
//!
//! This binary wraps that mechanism in a tiny loopback HTTP service so the
//! Python bridge never has to touch SQLCipher itself.
//!
//! Safety properties, all deliberate:
//!   * every connection is opened against the live QQ database either read-only
//!     or with `PRAGMA query_only = ON`, so nothing this process does can modify
//!     the user's chat history;
//!   * only a fixed allowlist of database files under `--db-dir` is reachable;
//!   * only `SELECT` / `WITH` / `EXPLAIN` statements are accepted, and the token
//!     is compared before any work happens.
//!
//! Usage:
//!   ntqq-reader --db-dir <nt_db dir> --pkey <key> [--listen 127.0.0.1:19552] [--token <t>]

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Mutex;

use ntdb_unwrap::db::{register_offset_vfs, try_decrypt_db, OFFSET_VFS_NAME};
use ntdb_unwrap::ntqq::DBDecryptInfo;
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;
use serde_json::{json, Value};
use tiny_http::{Header, Method, Request, Response, Server};

const DEFAULT_LISTEN: &str = "127.0.0.1:19552";
const BODY_LIMIT: u64 = 8 * 1024 * 1024;

const USAGE: &str = "\
ntqq-reader - read-only HTTP bridge over QQNT SQLCipher databases

USAGE:
    ntqq-reader --db-dir <DIR> --pkey <KEY> [OPTIONS]

OPTIONS:
    --db-dir <DIR>       Directory holding nt_msg.db and friends (required)
    --pkey <KEY>         SQLCipher passphrase for those databases (required)
    --listen <ADDR>      Listen address [default: 127.0.0.1:19552]
    --token <TOKEN>      Require `Authorization: Bearer <TOKEN>` when non-empty
    --allow-write-open   Drop the query_only guard as a last resort. The
                         connection may then write; do not use unless the
                         guarded modes are refused by SQLCipher.
    -h, --help           Print this help

ENDPOINTS:
    GET  /health         Service state, database inventory, per-connection mode
    POST /query          {\"db\":\"nt_msg\",\"sql\":\"SELECT ...\",\"params\":[]}
                         -> {\"columns\":[...],\"rows\":[[...],...]}
                         BLOBs are returned as lowercase hex strings.
";

// -- argument parsing --------------------------------------------------------------------------

struct Args {
    db_dir: PathBuf,
    pkey: String,
    listen: String,
    token: String,
    allow_write_open: bool,
}

impl Args {
    fn parse() -> Result<Args, String> {
        let mut db_dir = None;
        let mut pkey = None;
        let mut listen = DEFAULT_LISTEN.to_string();
        let mut token = String::new();
        let mut allow_write_open = false;
        let mut argv = std::env::args().skip(1);
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--db-dir" => db_dir = Some(PathBuf::from(value_of(&mut argv, "--db-dir")?)),
                "--pkey" => pkey = Some(value_of(&mut argv, "--pkey")?),
                "--listen" => listen = value_of(&mut argv, "--listen")?,
                "--token" => token = value_of(&mut argv, "--token")?,
                "--allow-write-open" => allow_write_open = true,
                "-h" | "--help" => return Err("help requested".to_string()),
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(Args {
            db_dir: db_dir.ok_or("--db-dir is required")?,
            pkey: pkey.ok_or("--pkey is required")?,
            listen,
            token,
            allow_write_open,
        })
    }
}

fn value_of(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    argv.next().ok_or_else(|| format!("{flag} needs a value"))
}

// -- database access ---------------------------------------------------------------------------

/// Logical name -> file name. Anything outside this list is unreachable.
fn database_file(name: &str) -> Option<&'static str> {
    match name {
        "nt_msg" => Some("nt_msg.db"),
        "group_info" => Some("group_info.db"),
        "profile_info" => Some("profile_info.db"),
        "files_in_chat" => Some("files_in_chat.db"),
        "recent_contact" => Some("recent_contact.db"),
        "settings" => Some("settings.db"),
        _ => None,
    }
}

const DATABASES: [&str; 6] = [
    "nt_msg",
    "group_info",
    "profile_info",
    "files_in_chat",
    "recent_contact",
    "settings",
];

/// How a connection ended up guarding against writes.
///
/// A plain `SQLITE_OPEN_READ_ONLY` handle is the strongest guarantee, but
/// SQLite refuses read-only access to a WAL database when it cannot set up (or
/// attach to) the `-shm` index -- and QQ keeps `nt_msg.db` in WAL mode while it
/// runs. So we try the strong mode first and fall back to a read-write handle
/// with `PRAGMA query_only = ON`, which makes SQLite itself reject every write
/// statement. Both modes are reported by `/health` so the caller can tell which
/// one is in force.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum OpenMode {
    ReadOnly,
    ReadWriteQueryOnly,
    ReadWrite,
}

impl OpenMode {
    fn as_str(self) -> &'static str {
        match self {
            OpenMode::ReadOnly => "read_only",
            OpenMode::ReadWriteQueryOnly => "read_write_query_only",
            OpenMode::ReadWrite => "read_write",
        }
    }
}

fn open_flags(mode: OpenMode) -> OpenFlags {
    let base = OpenFlags::SQLITE_OPEN_NO_MUTEX;
    match mode {
        OpenMode::ReadOnly => base | OpenFlags::SQLITE_OPEN_READ_ONLY,
        _ => base | OpenFlags::SQLITE_OPEN_READ_WRITE,
    }
}

/// Open `name` under `db_dir`, skipping the QQNT header, applying the key, and
/// installing the strongest write guard that SQLite will accept.
///
/// Shared by the server and the tests so both exercise the same code path.
fn open_database(
    db_dir: &Path,
    name: &str,
    pkey: &str,
    allow_write_open: bool,
) -> Result<(Connection, OpenMode), String> {
    let file = database_file(name).ok_or_else(|| format!("unknown database: {name}"))?;
    let path = db_dir.join(file);
    if !path.is_file() {
        return Err(format!("database file not found: {}", path.display()));
    }

    if allow_write_open {
        let conn = connect(&path, OpenMode::ReadWrite, pkey)?;
        return Ok((conn, OpenMode::ReadWrite));
    }

    // Strongest guard first.
    match connect(&path, OpenMode::ReadOnly, pkey) {
        Ok(conn) => Ok((conn, OpenMode::ReadOnly)),
        Err(read_only_error) => {
            // WAL databases held by a running client can refuse a read-only
            // handle; retry with a read-write handle pinned to query_only.
            match connect(&path, OpenMode::ReadWriteQueryOnly, pkey) {
                Ok(conn) => Ok((conn, OpenMode::ReadWriteQueryOnly)),
                Err(query_only_error) => Err(format!(
                    "cannot open {name} in either guarded mode: \
                     read-only: {read_only_error}; \
                     read-write+query_only: {query_only_error}"
                )),
            }
        }
    }
}

fn connect(path: &Path, mode: OpenMode, pkey: &str) -> Result<Connection, String> {
    // The offset VFS lives in ntdb_unwrap; it detects the "QQ_NT DB" magic and
    // shifts every file operation past the 1024-byte header.
    let conn = Connection::open_with_flags_and_vfs(path, open_flags(mode), OFFSET_VFS_NAME)
        .map_err(|err| format!("open {}: {err}", path.display()))?;
    try_decrypt_db(
        &conn,
        DBDecryptInfo {
            key: pkey.to_string(),
            ..Default::default()
        },
    )
    .map_err(|err| format!("decrypt: {err:?}"))?;
    if mode == OpenMode::ReadWriteQueryOnly {
        conn.pragma_update(None, "query_only", true)
            .map_err(|err| format!("enable query_only: {err}"))?;
    }
    Ok(conn)
}

struct State {
    db_dir: PathBuf,
    pkey: String,
    token: String,
    allow_write_open: bool,
    conns: Mutex<HashMap<String, (Connection, OpenMode)>>,
}

impl State {
    /// Run `f` against the named database, opening (and caching) it if needed.
    ///
    /// The mutex is held across the whole closure: `rusqlite::Connection` is not
    /// `Sync`, and holding the lock keeps concurrent requests from interleaving
    /// statements on one connection.
    fn with_connection<T>(
        &self,
        name: &str,
        f: impl FnOnce(&Connection) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut conns = self
            .conns
            .lock()
            .map_err(|_| "connection cache poisoned".to_string())?;
        if !conns.contains_key(name) {
            let opened = open_database(&self.db_dir, name, &self.pkey, self.allow_write_open)?;
            conns.insert(name.to_string(), opened);
        }
        let (conn, _) = conns.get(name).expect("inserted above");
        f(conn)
    }
}

// -- request handling --------------------------------------------------------------------------

#[derive(Deserialize)]
struct QueryRequest {
    db: String,
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = match Args::parse() {
        Ok(args) => args,
        Err(message) => {
            if message == "help requested" {
                print!("{USAGE}");
                exit(0);
            }
            eprintln!("{message}");
            eprint!("{USAGE}");
            exit(2);
        }
    };

    if let Err(code) = register_offset_vfs() {
        eprintln!("failed to register the offset vfs (sqlite code {code})");
        exit(1);
    }

    if args.allow_write_open {
        eprintln!("WARNING: --allow-write-open is set; the query_only guard is disabled");
    }

    let state = State {
        db_dir: args.db_dir,
        pkey: args.pkey,
        token: args.token,
        allow_write_open: args.allow_write_open,
        conns: Mutex::new(HashMap::new()),
    };

    let server = match Server::http(&args.listen) {
        Ok(server) => server,
        Err(err) => {
            eprintln!("cannot listen on {}: {err}", args.listen);
            exit(1);
        }
    };
    println!("ntqq-reader listening on {}", args.listen);
    println!("database directory: {}", state.db_dir.display());
    println!(
        "write guard: {}",
        if state.allow_write_open {
            "disabled (--allow-write-open)"
        } else {
            "read-only, falling back to read-write + query_only"
        }
    );

    for request in server.incoming_requests() {
        handle(request, &state);
    }
}

fn handle(request: Request, state: &State) {
    let path = request
        .url()
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .to_string();
    let method = request.method().clone();

    if !state.token.is_empty() && !authorized(&request, &state.token) {
        respond_json(request, 401, &json!({ "error": "unauthorized" }));
        return;
    }

    match (method, path.as_str()) {
        (Method::Get, "/health") => {
            let databases: Vec<Value> = DATABASES
                .iter()
                .map(|name| {
                    let file = database_file(name).expect("allowlisted");
                    let full = state.db_dir.join(file);
                    json!({
                        "name": name,
                        "file": file,
                        "exists": full.is_file(),
                        "bytes": std::fs::metadata(&full).map(|m| m.len()).unwrap_or(0),
                    })
                })
                .collect();
            let open: Vec<Value> = state
                .conns
                .lock()
                .map(|conns| {
                    conns
                        .iter()
                        .map(|(name, (_, mode))| json!({ "name": name, "mode": mode.as_str() }))
                        .collect()
                })
                .unwrap_or_default();
            let payload = json!({
                "ok": true,
                "service": "ntqq-reader",
                "version": env!("CARGO_PKG_VERSION"),
                "db_dir": state.db_dir.display().to_string(),
                "allow_write_open": state.allow_write_open,
                "databases": databases,
                "open": open,
            });
            respond_json(request, 200, &payload);
        }
        (Method::Post, "/query") => handle_query(request, state),
        _ => respond_json(request, 404, &json!({ "error": "not found" })),
    }
}

fn authorized(request: &Request, token: &str) -> bool {
    let expected = format!("Bearer {token}");
    request
        .headers()
        .iter()
        .any(|header| header.field.equiv("Authorization") && header.value.as_str() == expected)
}

fn handle_query(mut request: Request, state: &State) {
    let mut body = String::new();
    // Bind the read result before branching. An `if let` scrutinee keeps its
    // temporaries alive for the whole block, so reading straight inside the
    // condition would hold the mutable borrow of `request` across the
    // `respond_json(request, ..)` that moves it.
    let read_result = request
        .as_reader()
        .take(BODY_LIMIT)
        .read_to_string(&mut body);
    if let Err(err) = read_result {
        respond_json(
            request,
            400,
            &json!({ "error": format!("cannot read body: {err}") }),
        );
        return;
    }
    let parsed: QueryRequest = match serde_json::from_str(&body) {
        Ok(parsed) => parsed,
        Err(err) => {
            respond_json(
                request,
                400,
                &json!({ "error": format!("invalid request: {err}") }),
            );
            return;
        }
    };
    if let Err(err) = ensure_read_only(&parsed.sql) {
        respond_json(request, 400, &json!({ "error": err }));
        return;
    }
    let params = match parsed
        .params
        .iter()
        .map(json_to_sql)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(params) => params,
        Err(err) => {
            respond_json(request, 400, &json!({ "error": err }));
            return;
        }
    };

    let outcome = state.with_connection(&parsed.db, |conn| run_query(conn, &parsed.sql, params));
    match outcome {
        Ok(payload) => respond_json(request, 200, &payload),
        Err(err) => respond_json(request, 400, &json!({ "error": err })),
    }
}

/// Reject anything that is not a read. `rusqlite::Connection::prepare` already
/// refuses multi-statement input, so a leading-keyword check is enough here.
fn ensure_read_only(sql: &str) -> Result<(), String> {
    let head = sql.trim_start().to_ascii_lowercase();
    if head.starts_with("select") || head.starts_with("with") || head.starts_with("explain") {
        return Ok(());
    }
    Err("only SELECT / WITH / EXPLAIN statements are allowed".to_string())
}

fn run_query(conn: &Connection, sql: &str, params: Vec<SqlValue>) -> Result<Value, String> {
    let mut stmt = conn.prepare(sql).map_err(|err| format!("prepare: {err}"))?;
    let columns: Vec<String> = stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(|err| format!("query: {err}"))?;

    let mut out: Vec<Value> = Vec::new();
    while let Some(row) = rows.next().map_err(|err| format!("read row: {err}"))? {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            let value: SqlValue = row
                .get(index)
                .map_err(|err| format!("column {index}: {err}"))?;
            values.push(sql_to_json(value));
        }
        out.push(Value::Array(values));
    }
    Ok(json!({ "columns": columns, "rows": out }))
}

fn json_to_sql(value: &Value) -> Result<SqlValue, String> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(flag) => Ok(SqlValue::Integer(i64::from(*flag))),
        Value::Number(number) => {
            if let Some(int) = number.as_i64() {
                Ok(SqlValue::Integer(int))
            } else if let Some(float) = number.as_f64() {
                Ok(SqlValue::Real(float))
            } else {
                Err(format!("unsupported number: {number}"))
            }
        }
        Value::String(text) => Ok(SqlValue::Text(text.clone())),
        _ => Err("parameters must be null, boolean, number or string".to_string()),
    }
}

fn sql_to_json(value: SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(int) => json!(int),
        SqlValue::Real(float) => json!(float),
        SqlValue::Text(text) => Value::String(text),
        // Projected message blobs are protobuf; hand them out as hex so the Python
        // side can decode them without relying on JSON byte semantics.
        SqlValue::Blob(bytes) => Value::String(to_hex(&bytes)),
    }
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn respond_json(request: Request, status: u16, payload: &Value) {
    let header = Header::from_bytes("Content-Type", "application/json; charset=utf-8")
        .expect("static header is valid");
    let response = Response::from_string(payload.to_string())
        .with_status_code(status)
        .with_header(header);
    if let Err(err) = request.respond(response) {
        eprintln!("failed to send response: {err}");
    }
}

// -- tests -------------------------------------------------------------------------------------
//
// These are the only way to prove the "1024-byte offset VFS + SQLCipher" chain
// actually works, because the toolchain that builds this crate (vendored
// OpenSSL, SQLCipher) is not available on the development machine. They run in
// CI via `cargo test --release`.

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "qqvibe-sidecar-test-key";

    /// Cipher settings that mirror a real QQNT database, so the reader is
    /// exercised against the same page size / KDF iteration count / HMAC
    /// algorithm that QQ uses.
    fn cipher_info(hmac: &str) -> DBDecryptInfo {
        DBDecryptInfo {
            key: KEY.to_string(),
            cipher_hmac_algorithm: Some(hmac.to_string()),
        }
    }

    /// Create an encrypted database and seed it with a table shaped like
    /// `c2c_msg_table` (numeric column names, a protobuf blob column).
    fn seed_encrypted_db(path: &Path, hmac: &str, wal: bool) {
        let conn = Connection::open(path).expect("create database");
        let pragmas = cipher_info(hmac).display_pragma_stmts().to_string();
        conn.execute_batch(&pragmas).expect("apply cipher settings");
        if wal {
            conn.pragma_update(None, "journal_mode", "WAL")
                .expect("enable wal");
        }
        conn.execute_batch(
            "CREATE TABLE c2c_msg_table (\
                 \"40001\" INTEGER PRIMARY KEY, \
                 \"40020\" TEXT, \
                 \"40030\" INTEGER, \
                 \"40050\" INTEGER, \
                 \"40800\" BLOB);\
             INSERT INTO c2c_msg_table VALUES \
                 (1, 'u_me',   10001, 1700000000, X'0a05'),\
                 (2, 'u_peer', 10002, 1700000001, X'00ff7f');",
        )
        .expect("seed rows");
    }

    /// Prepend the 1024-byte QQNT header (magic `QQ_NT DB` at offset 32) so the
    /// offset VFS recognises the file and applies its 1024-byte offset.
    fn add_ntqq_header(path: &Path) {
        let body = std::fs::read(path).expect("read database");
        assert!(body.len() > 1024, "seeded database is too small");
        let mut out = vec![0u8; 1024];
        out[32..40].copy_from_slice(b"QQ_NT DB");
        out.extend_from_slice(&body);
        std::fs::write(path, out).expect("write database");
    }

    fn body_len(path: &Path) -> u64 {
        std::fs::metadata(path).expect("stat").len()
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> TempDir {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let mut path = std::env::temp_dir();
            path.push(format!(
                "ntqq-reader-test-{tag}-{}-{nanos}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("create temp dir");
            TempDir(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The offset VFS must hide the 1024-byte header from SQLite. Asking SQLite
    /// for its own view of the file size is the observable proof.
    #[test]
    fn offset_vfs_skips_the_ntqq_header() {
        register_offset_vfs().expect("register offset vfs");
        let dir = TempDir::new("offset");
        let db = dir.path().join("nt_msg.db");
        seed_encrypted_db(&db, "HMAC_SHA1", false);
        let plain_len = body_len(&db);
        add_ntqq_header(&db);
        assert_eq!(body_len(&db), plain_len + 1024, "header must be prepended");
        assert_eq!(
            plain_len % 4096,
            0,
            "an sqlcipher database is a whole number of 4096-byte pages"
        );

        let (conn, mode) =
            open_database(dir.path(), "nt_msg", KEY, false).expect("open via offset vfs");
        assert_eq!(
            mode,
            OpenMode::ReadOnly,
            "a plain database must open read-only"
        );
        let page_size: i64 = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .expect("page_size");
        let page_count: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .expect("page_count");
        assert_eq!(
            (page_count * page_size) as u64,
            plain_len,
            "sqlite must see the database without its 1024-byte header"
        );
    }

    /// Read-only open over the offset VFS, in the default rollback-journal mode.
    #[test]
    fn read_only_roundtrip_over_offset_vfs() {
        register_offset_vfs().expect("register offset vfs");
        let dir = TempDir::new("readonly");
        let db = dir.path().join("nt_msg.db");
        seed_encrypted_db(&db, "HMAC_SHA1", false);
        add_ntqq_header(&db);

        let (conn, mode) = open_database(dir.path(), "nt_msg", KEY, false).expect("open read-only");
        assert_eq!(mode, OpenMode::ReadOnly);
        let payload = run_query(
            &conn,
            "SELECT \"40001\", \"40020\", \"40050\", \"40800\" FROM c2c_msg_table ORDER BY \"40001\"",
            Vec::new(),
        )
        .expect("query");

        assert_eq!(
            payload["columns"],
            json!(["40001", "40020", "40050", "40800"])
        );
        let rows = payload["rows"].as_array().expect("rows array");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], json!("u_me"));
        assert_eq!(rows[0][2], json!(1700000000i64));
        // Blobs must come back hex-encoded so the Python side can decode them.
        assert_eq!(rows[0][3], json!("0a05"));
        assert_eq!(rows[1][3], json!("00ff7f"));
    }

    /// `try_decrypt_db` with `cipher_hmac_algorithm = None` must fall back from
    /// HMAC_SHA256 to HMAC_SHA1. Real QQNT databases use SHA1, so this fallback
    /// is what makes the reader work without being told which one to use.
    #[test]
    fn unknown_hmac_algorithm_is_auto_detected() {
        register_offset_vfs().expect("register offset vfs");
        let dir = TempDir::new("hmac");
        let db = dir.path().join("nt_msg.db");
        seed_encrypted_db(&db, "HMAC_SHA1", false);
        add_ntqq_header(&db);

        let conn = Connection::open_with_flags_and_vfs(
            &db,
            open_flags(OpenMode::ReadOnly),
            OFFSET_VFS_NAME,
        )
        .expect("open");
        try_decrypt_db(
            &conn,
            DBDecryptInfo {
                key: KEY.to_string(),
                cipher_hmac_algorithm: None,
            },
        )
        .expect("hmac fallback should find SHA1");
        let count: i64 = conn
            .query_row("SELECT count(*) FROM c2c_msg_table", [], |row| row.get(0))
            .expect("count");
        assert_eq!(count, 2);
    }

    /// A wrong key must fail loudly rather than returning empty results.
    #[test]
    fn wrong_key_is_rejected() {
        register_offset_vfs().expect("register offset vfs");
        let dir = TempDir::new("wrongkey");
        let db = dir.path().join("nt_msg.db");
        seed_encrypted_db(&db, "HMAC_SHA1", false);
        add_ntqq_header(&db);

        let err = open_database(dir.path(), "nt_msg", "not-the-key", false)
            .expect_err("a wrong key must not open the database");
        assert!(err.contains("decrypt"), "unexpected error shape: {err}");
    }

    /// The real-world case: QQ is running and writing in WAL mode while we read.
    /// A guarded connection must see the writer's committed rows, and the guard
    /// must never be `read_write`.
    #[test]
    fn guarded_connection_observes_a_live_wal_writer() {
        register_offset_vfs().expect("register offset vfs");
        let dir = TempDir::new("wal");
        let db = dir.path().join("nt_msg.db");
        seed_encrypted_db(&db, "HMAC_SHA1", false);
        add_ntqq_header(&db);

        // Impersonate the running QQ client: read-write, WAL, keyed.
        let writer = Connection::open_with_flags_and_vfs(
            &db,
            open_flags(OpenMode::ReadWrite),
            OFFSET_VFS_NAME,
        )
        .expect("open as writer");
        writer
            .execute_batch(&cipher_info("HMAC_SHA1").display_pragma_stmts().to_string())
            .expect("key the writer");
        writer
            .pragma_update(None, "journal_mode", "WAL")
            .expect("enable wal");
        writer
            .execute(
                "INSERT INTO c2c_msg_table VALUES (3, 'u_live', 10003, 1700000002, X'0b0c')",
                [],
            )
            .expect("writer inserts");

        let (conn, mode) =
            open_database(dir.path(), "nt_msg", KEY, false).expect("open while the writer is live");
        eprintln!("WAL + live writer resolved to open mode: {}", mode.as_str());
        assert_ne!(
            mode,
            OpenMode::ReadWrite,
            "the guard must not be silently dropped"
        );

        let payload =
            run_query(&conn, "SELECT count(*) FROM c2c_msg_table", Vec::new()).expect("query");
        assert_eq!(
            payload["rows"][0][0],
            json!(3),
            "the guarded connection must see the writer's committed row"
        );
    }

    /// Whichever guarded mode is chosen, SQLite itself must refuse writes.
    #[test]
    fn guarded_connection_refuses_writes_at_the_sqlite_level() {
        // stderr is unbuffered, so these markers survive a process that has to be
        // killed: a hang is then located in the CI log instead of guessed at.
        eprintln!("[guard] registering the offset vfs");
        register_offset_vfs().expect("register offset vfs");
        let dir = TempDir::new("guard");
        let db = dir.path().join("nt_msg.db");
        eprintln!("[guard] seeding a rollback-journal database");
        seed_encrypted_db(&db, "HMAC_SHA1", false);
        add_ntqq_header(&db);

        eprintln!("[guard] opening through the write guard");
        let (conn, mode) = open_database(dir.path(), "nt_msg", KEY, false).expect("open");
        eprintln!("[guard] opened in mode {}", mode.as_str());
        let attempted = conn.execute("DELETE FROM c2c_msg_table", []);
        eprintln!("[guard] DELETE returned {attempted:?}");
        assert!(
            attempted.is_err(),
            "mode {} allowed a DELETE; the write guard is broken",
            mode.as_str()
        );
    }

    /// Anything that is not a read must be refused before it reaches SQLite.
    #[test]
    fn writes_are_refused_by_the_statement_guard() {
        assert!(ensure_read_only("SELECT 1").is_ok());
        assert!(ensure_read_only("  with x as (select 1) select * from x").is_ok());
        assert!(ensure_read_only("EXPLAIN SELECT 1").is_ok());
        assert!(ensure_read_only("DELETE FROM c2c_msg_table").is_err());
        assert!(ensure_read_only("UPDATE c2c_msg_table SET \"40020\" = 'x'").is_err());
        assert!(ensure_read_only("DROP TABLE c2c_msg_table").is_err());
        assert!(ensure_read_only("PRAGMA key = 'x'").is_err());
    }

    /// The file allowlist is the only thing standing between a query and an
    /// arbitrary file on disk, so it gets its own test.
    #[test]
    fn only_allowlisted_databases_are_reachable() {
        for name in DATABASES {
            assert!(
                database_file(name).is_some(),
                "{name} should be allowlisted"
            );
        }
        for name in ["../../etc/passwd", "nt_msg.db", "..", "", "arbitrary"] {
            assert!(
                database_file(name).is_none(),
                "{name} must not be reachable"
            );
        }
    }

    /// Blob and scalar encodings must survive the trip to JSON.
    #[test]
    fn json_encoding_is_lossless_for_blobs() {
        assert_eq!(sql_to_json(SqlValue::Null), Value::Null);
        assert_eq!(sql_to_json(SqlValue::Integer(-7)), json!(-7));
        assert_eq!(sql_to_json(SqlValue::Text("hi".into())), json!("hi"));
        assert_eq!(sql_to_json(SqlValue::Blob(vec![])), json!(""));
        assert_eq!(
            sql_to_json(SqlValue::Blob(vec![0xde, 0xad, 0x00])),
            json!("dead00")
        );
        assert_eq!(json_to_sql(&json!(5)).expect("int"), SqlValue::Integer(5));
        assert_eq!(
            json_to_sql(&json!("x")).expect("text"),
            SqlValue::Text("x".into())
        );
        assert!(json_to_sql(&json!({ "a": 1 })).is_err());
    }

    /// `--help` output must document the required flags.
    #[test]
    fn usage_text_mentions_the_required_flags() {
        assert!(USAGE.contains("--db-dir"));
        assert!(USAGE.contains("--pkey"));
        assert!(USAGE.contains("--listen"));
        assert!(USAGE.contains("--token"));
    }

    // -- fixture generation for the CI end-to-end check ------------------------------------------

    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            out.push(if value == 0 { byte } else { byte | 0x80 });
            if value == 0 {
                return out;
            }
        }
    }

    /// Length-delimited protobuf field.
    fn ld(field: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = varint((field << 3) | 2);
        out.extend(varint(payload.len() as u64));
        out.extend_from_slice(payload);
        out
    }

    /// Varint protobuf field.
    fn vi(field: u64, value: u64) -> Vec<u8> {
        let mut out = varint(field << 3);
        out.extend(varint(value));
        out
    }

    /// A `Message` body as QQNT stores it in column `40800`: a repeated
    /// `SingleMessage` (field 40800) whose `messageType` (45002) is 1 (text) and
    /// whose `messageText` (45101) carries the payload. These field numbers come
    /// from ntdb_unwrap's own `src/protos/message.proto`.
    fn text_message_body(text: &str) -> Vec<u8> {
        let mut single = vi(45002, 1);
        single.extend(ld(45101, text.as_bytes()));
        ld(40800, &single)
    }

    /// Create a database with the two message tables the bridge reads, populated
    /// with protobuf message bodies.
    fn seed_nt_msg_fixture(path: &Path, key: &str) {
        let conn = Connection::open(path).expect("create fixture");
        conn.execute_batch(
            &cipher_info_of(key, "HMAC_SHA1")
                .display_pragma_stmts()
                .to_string(),
        )
        .expect("key the fixture");
        // Column names are the real numeric ones used by QQNT; the set here is the
        // subset the bridge reads. c2c and group differ in 40090 (group card) and
        // in the peer columns, which is exactly the shape the bridge must handle.
        conn.execute_batch(
            "CREATE TABLE c2c_msg_table (\
                 \"40001\" INTEGER PRIMARY KEY, \"40003\" INTEGER, \"40010\" INTEGER, \
                 \"40011\" INTEGER, \"40012\" INTEGER, \"40013\" INTEGER, \
                 \"40020\" TEXT, \"40021\" TEXT, \"40027\" INTEGER, \"40030\" INTEGER, \
                 \"40033\" INTEGER, \"40041\" INTEGER, \"40050\" INTEGER, \
                 \"40058\" INTEGER, \"40093\" TEXT, \"40800\" BLOB);\
             CREATE TABLE group_msg_table (\
                 \"40001\" INTEGER PRIMARY KEY, \"40003\" INTEGER, \"40010\" INTEGER, \
                 \"40011\" INTEGER, \"40012\" INTEGER, \"40013\" INTEGER, \
                 \"40020\" TEXT, \"40021\" TEXT, \"40027\" INTEGER, \"40030\" INTEGER, \
                 \"40033\" INTEGER, \"40041\" INTEGER, \"40050\" INTEGER, \
                 \"40058\" INTEGER, \"40090\" TEXT, \"40093\" TEXT, \"40800\" BLOB);",
        )
        .expect("create tables");

        let base: i64 = 1_700_000_000;
        for (index, (sender, text)) in [
            (10001i64, "hello there"),
            (20001, "hi back"),
            (10001, "how are you"),
        ]
        .into_iter()
        .enumerate()
        {
            conn.execute(
                "INSERT INTO c2c_msg_table VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rusqlite::params![
                    6000i64 + index as i64,
                    0i64,
                    1i64,
                    2i64,
                    1i64,
                    1i64,
                    sender.to_string(),
                    "20001",
                    20001i64,
                    20001i64,
                    sender,
                    0i64,
                    base + index as i64,
                    base + index as i64,
                    format!("nick {sender}"),
                    text_message_body(text),
                ],
            )
            .expect("insert c2c row");
        }
        for index in 0..3i64 {
            conn.execute(
                "INSERT INTO group_msg_table VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                rusqlite::params![
                    9000i64 + index,
                    2000i64 + index,
                    2i64,
                    2i64,
                    1i64,
                    0i64,
                    "20001",
                    "888001",
                    888001i64,
                    888001i64,
                    20002i64,
                    0i64,
                    base + index,
                    base + index,
                    format!("card {}", index + 2),
                    format!("nick {}", index + 2),
                    text_message_body(&format!("group message {index}")),
                ],
            )
            .expect("insert group row");
        }
    }

    fn cipher_info_of(key: &str, hmac: &str) -> DBDecryptInfo {
        DBDecryptInfo {
            key: key.to_string(),
            cipher_hmac_algorithm: Some(hmac.to_string()),
        }
    }

    /// When `QQVIBE_FIXTURE_DIR` is set, materialise a real encrypted
    /// QQNT-shaped database there so CI can point the built binary at it and
    /// exercise the HTTP API end to end.
    ///
    /// The fixture is produced by the same SQLCipher that will read it, through
    /// `ntdb_unwrap`'s own PRAGMA statements. That is deliberate: a fixture
    /// written by a hand-rolled cipher could be wrong in exactly the ways that
    /// matter and still look plausible.
    ///
    /// `QQVIBE_FIXTURE_PKEY` selects the passphrase (`KEY` when unset); the
    /// workflow sets both variables and reuses the same passphrase for
    /// `--pkey`, so the value is never duplicated in two places.
    ///
    /// With the variable unset this test is a no-op, which is what a plain
    /// `cargo test` wants.
    #[test]
    fn write_http_fixture_when_requested() {
        let dir = match std::env::var("QQVIBE_FIXTURE_DIR") {
            Ok(dir) if !dir.is_empty() => PathBuf::from(dir),
            _ => return,
        };
        let key = std::env::var("QQVIBE_FIXTURE_PKEY").unwrap_or_else(|_| KEY.to_string());

        register_offset_vfs().expect("register offset vfs");
        std::fs::create_dir_all(&dir).expect("create fixture dir");
        let db = dir.join("nt_msg.db");
        let _ = std::fs::remove_file(&db);
        seed_nt_msg_fixture(&db, &key);
        let plain_len = body_len(&db);
        add_ntqq_header(&db);

        // Prove the fixture is readable through the exact production path before
        // handing it to the HTTP test, so a broken fixture fails here with a
        // precise message instead of as a confusing HTTP error.
        let (conn, mode) = open_database(&dir, "nt_msg", &key, false).expect("reopen fixture");
        let c2c: i64 = conn
            .query_row("SELECT count(*) FROM c2c_msg_table", [], |row| row.get(0))
            .expect("count c2c");
        let group: i64 = conn
            .query_row("SELECT count(*) FROM group_msg_table", [], |row| row.get(0))
            .expect("count group");
        assert_eq!((c2c, group), (3, 3), "fixture row counts");
        assert_eq!(
            body_len(&db),
            plain_len + 1024,
            "the fixture must carry the 1024-byte QQNT header"
        );
        println!(
            "fixture ready: {} ({} plain bytes, opened as {})",
            db.display(),
            plain_len,
            mode.as_str()
        );
    }
}
