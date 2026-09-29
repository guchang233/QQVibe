"use strict";
/**
 * Loopback JSON-RPC surface for the injected QQNT reader.
 *
 * The reader lives inside QQ's main process; the Python bridge is a different
 * process, so it reaches the reader over `127.0.0.1` with a per-launch token.
 * Only read methods are exposed and the listener never leaves the loopback
 * interface.
 */
const http = require("http");
const crypto = require("crypto");
const { QqntReader } = require("./reader");
const { log, writeRuntime, writeConfig } = require("./paths");

const HOST = "127.0.0.1";
const TOKEN_HEADER = "x-qqvibe-token";
const MAX_BODY = 4 * 1024 * 1024;

function bool(value) {
  return value === true || value === "1" || value === 1;
}

function createReader() {
  return new QqntReader();
}

/** The RPC method table; every entry is read-only. */
function buildMethods(reader) {
  const safe = (fn) => async (params) => fn(params || {});
  return {
    ping: safe(async () => ({ ok: true })),
    self: safe(async () => reader.selfInfo()),
    contacts: safe(async () => reader.contacts()),
    sessions: safe(async () => reader.sessions()),
    messages: safe(async ({ peer, isGroup, limit, offset, order }) =>
      reader.messages(String(peer || ""), {
        isGroup: bool(isGroup),
        limit: limit,
        offset: offset,
        order: order || "latest",
      }),
    ),
    historyHighwater: safe(async ({ peer, isGroup }) =>
      reader.historyHighwater(String(peer || ""), bool(isGroup)),
    ),
    historyPage: safe(async ({ peer, isGroup, ceiling, after, limit }) =>
      reader.historyPage(String(peer || ""), {
        isGroup: bool(isGroup),
        ceiling: ceiling || null,
        after: after || null,
        limit: limit,
      }),
    ),
    precededText: safe(async ({ peer, isGroup, before, limit }) =>
      reader.precededText(String(peer || ""), before, limit, bool(isGroup)),
    ),
    historyBefore: safe(async ({ peer, isGroup, before, limit }) =>
      reader.historyBefore(String(peer || ""), before, limit, bool(isGroup)),
    ),
    historyAfter: safe(async ({ peer, isGroup, after, limit }) =>
      reader.historyAfter(String(peer || ""), after, limit, bool(isGroup)),
    ),
    messageAt: safe(async ({ peer, isGroup, position }) =>
      reader.messageAt(String(peer || ""), position, bool(isGroup)),
    ),
    historySearch: safe(async ({ peer, isGroup, before, startMs, endMs, query, limit }) =>
      reader.historySearch(String(peer || ""), {
        isGroup: bool(isGroup),
        before: before || null,
        startMs: startMs === undefined ? null : startMs,
        endMs: endMs === undefined ? null : endMs,
        query: query || "",
        limit: limit,
      }),
    ),
    senders: safe(async ({ peer, isGroup }) =>
      reader.senders(String(peer || ""), bool(isGroup)),
    ),
    textsByIds: safe(async ({ peer, isGroup, ids }) =>
      reader.textsByIds(String(peer || ""), ids, bool(isGroup)),
    ),
    quotedHistoryPage: safe(async ({ peer, isGroup, ceiling, after, limit, member }) =>
      reader.quotedHistoryPage(String(peer || ""), {
        isGroup: bool(isGroup),
        ceiling: ceiling || null,
        after: after || null,
        limit: limit,
        member: member || null,
      }),
    ),
    profileMetadata: safe(async ({ peer, isGroup, member }) =>
      reader.profileMetadata(String(peer || ""), member || null, bool(isGroup)),
    ),
    image: safe(async ({ ref }) => reader.imageData(ref || {})),
  };
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    request.on("data", (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY) {
        reject(new Error("request too large"));
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    request.on("error", reject);
  });
}

function send(response, status, payload) {
  const body = Buffer.from(JSON.stringify(payload), "utf8");
  response.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": body.length,
    "Cache-Control": "no-store",
  });
  response.end(body);
}

/**
 * Start the loopback server. Returns a promise resolved once it is listening,
 * with `{ server, port, token }`.
 */
function startServer(reader, { preferredPort = 0 } = {}) {
  const token = crypto.randomBytes(32).toString("hex");
  const methods = buildMethods(reader);

  const server = http.createServer(async (request, response) => {
    try {
      if (request.method === "GET" && request.url === "/health") {
        let account = null;
        try {
          const self = await reader.selfInfo();
          account = self.account;
        } catch (_error) {
          account = null;
        }
        send(response, 200, { ok: true, account, ready: Boolean(account) });
        return;
      }
      if (request.method !== "POST" || request.headers[TOKEN_HEADER] !== token) {
        send(response, 403, { ok: false, error: { code: "forbidden" } });
        return;
      }
      const body = await readBody(request);
      let payload;
      try {
        payload = JSON.parse(body || "{}");
      } catch (_error) {
        send(response, 400, { ok: false, error: { code: "bad-json" } });
        return;
      }
      const method = methods[payload.method];
      if (!method) {
        send(response, 404, { ok: false, error: { code: "unknown-method", message: payload.method } });
        return;
      }
      try {
        const result = await method(payload.params);
        send(response, 200, { ok: true, result });
      } catch (error) {
        send(response, 200, {
          ok: false,
          error: { code: "reader-error", message: error && error.message ? error.message : String(error) },
        });
      }
    } catch (error) {
      send(response, 500, {
        ok: false,
        error: { code: "server-error", message: error && error.message ? error.message : String(error) },
      });
    }
  });

  return new Promise((resolve, reject) => {
    server.on("error", reject);
    server.listen(preferredPort, HOST, () => {
      const port = server.address().port;
      resolve({ server, port, token });
    });
  });
}

/** Boot the reader, publish its descriptor, and serve until the process exits. */
async function main() {
  const reader = createReader();
  const self = await reader.selfInfo().catch(() => ({}));
  const { server, port, token } = await startServer(reader);
  const descriptor = {
    pid: process.pid,
    port,
    token,
    account: self.account || null,
    startedAt: new Date().toISOString(),
    version: 1,
  };
  writeRuntime(descriptor);
  if (self.account) {
    const config = require("./paths").readConfig() || {};
    writeConfig({ ...config, account: self.account, lastPort: port });
  }
  log("reader listening on", `${HOST}:${port}`, "account:", self.account || "(pending)");
  const shutdown = () => {
    log("reader shutting down");
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500).unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
  process.on("exit", () => {
    try {
      writeRuntime({ ...descriptor, stopped: true });
    } catch (_error) {
      // best effort
    }
  });
  return { reader, server, descriptor };
}

module.exports = { startServer, createReader, buildMethods, main };