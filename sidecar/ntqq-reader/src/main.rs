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
//!   * every connection is opened `SQLITE_OPEN_READ_ONLY`, so the live QQ
//!     database cannot be modified through this process;
//!   * only a fixed allowlist of database files under `--db-dir` is reachable;
//!   * only `SELECT` / `WITH` statements are accepted, and the token is
//!     compared before any work happens.
//!
//! Usage:
//!   ntqq-reader --db-dir <nt_db dir> --pkey <key> [--listen 127.0.0.1:19552] [--token <t>]

use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
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
    --db-dir <DIR>     Directory holding nt_msg.db and friends (required)
    --pkey <KEY>       SQLCipher passphrase for those databases (required)
    --listen <ADDR>    Listen address [default: 127.0.0.1:19552]
    --token <TOKEN>    Require `Authorization: Bearer <TOKEN>` when non-empty
    -h, --help         Print this help
";

// -- argument parsing --------------------------------------------------------------------------

struct Args {
    db_dir: PathBuf,
    pkey: String,
    listen: String,
    token: String,
}

impl Args {
    fn parse() -> Result<Args, String> {
        let mut db_dir = None;
        let mut pkey = None;
        let mut listen = DEFAULT_LISTEN.to_string();
        let mut token = String::new();
        let mut argv = std::env::args().skip(1);
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--db-dir" => db_dir = Some(PathBuf::from(value_of(&mut argv, "--db-dir")?)),
                "--pkey" => pkey = Some(value_of(&mut argv, "--pkey")?),
                "--listen" => listen = value_of(&mut argv, "--listen")?,
                "--token" => token = value_of(&mut argv, "--token")?,
                "-h" | "--help" => return Err("help requested".to_string()),
                other => return Err(format!("unknown argument: {other}")),
            }
        }
        Ok(Args {
            db_dir: db_dir.ok_or("--db-dir is required")?,
            pkey: pkey.ok_or("--pkey is required")?,
            listen,
            token,
        })
    }
}

fn value_of(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    argv.next().ok_or_else(|| format!("{flag} needs a value"))
}

// -- database registry -------------------------------------------------------------------------

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

struct State {
    db_dir: PathBuf,
    pkey: String,
    token: String,
    conns: Mutex<HashMap<String, Connection>>,
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
        let mut conns = self.conns.lock().map_err(|_| "connection cache poisoned".to_string())?;
        if !conns.contains_key(name) {
            let conn = open_database(&self.db_dir, name, &self.pkey)?;
            conns.insert(name.to_string(), conn);
        }
        let conn = conns.get(name).expect("inserted above");
        f(conn)
    }
}

fn open_database(db_dir: &PathBuf, name: &str, pkey: &str) -> Result<Connection, String> {
    let file = database_file(name).ok_or_else(|| format!("unknown database: {name}"))?;
    let path = db_dir.join(file);
    if !path.is_file() {
        return Err(format!("database file not found: {}", path.display()));
    }
    // READ_ONLY is the whole point: the live database is never written to.
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags_and_vfs(&path, flags, OFFSET_VFS_NAME)
        .map_err(|err| format!("open {}: {err}", path.display()))?;
    try_decrypt_db(
        &conn,
        DBDecryptInfo {
            key: pkey.to_string(),
            ..Default::default()
        },
    )
    .map_err(|err| format!("decrypt {}: {err:?}", path.display()))?;
    Ok(conn)
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

    let state = State {
        db_dir: args.db_dir,
        pkey: args.pkey,
        token: args.token,
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
            let payload = json!({
                "ok": true,
                "service": "ntqq-reader",
                "db_dir": state.db_dir.display().to_string(),
                "databases": DATABASES,
                "open": state
                    .conns
                    .lock()
                    .map(|c| c.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default(),
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

fn handle_query(request: Request, state: &State) {
    let mut body = String::new();
    if let Err(err) = request
        .as_reader()
        .take(BODY_LIMIT)
        .read_to_string(&mut body)
    {
        respond_json(request, 400, &json!({ "error": format!("cannot read body: {err}") }));
        return;
    }
    let parsed: QueryRequest = match serde_json::from_str(&body) {
        Ok(parsed) => parsed,
        Err(err) => {
            respond_json(request, 400, &json!({ "error": format!("invalid request: {err}") }));
            return;
        }
    };
    if let Err(err) = ensure_read_only(&parsed.sql) {
        respond_json(request, 400, &json!({ "error": err }));
        return;
    }
    let params = match parsed.params.iter().map(json_to_sql).collect::<Result<Vec<_>, _>>() {
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
    Err("only SELECT / WITH statements are allowed".to_string())
}

fn run_query(conn: &Connection, sql: &str, params: Vec<SqlValue>) -> Result<Value, String> {
    let mut stmt = conn.prepare(sql).map_err(|err| format!("prepare: {err}"))?;
    let columns: Vec<String> = stmt.column_names().iter().map(|name| name.to_string()).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(|err| format!("query: {err}"))?;

    let mut out: Vec<Value> = Vec::new();
    while let Some(row) = rows.next().map_err(|err| format!("read row: {err}"))? {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            let value: SqlValue = row.get(index).map_err(|err| format!("column {index}: {err}"))?;
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
