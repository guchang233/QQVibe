"use strict";
/**
 * Injected loader: runs inside QQ's Electron main process.
 *
 * QQ's own `resources/app/app_launcher/index.js` is patched (by
 * `bridge/qqnt_install.py`) to `require()` this file once, right before QQ
 * starts its normal startup. Everything here is read-only: it starts a loopback
 * RPC server exposing chat reads and never calls a mutating kernel method.
 */
try {
  const { main } = require("./src/index.js");
  main().catch((error) => {
    try {
      require("./src/paths").log("loader failed:", error && error.stack ? error.stack : error);
    } catch (_ignored) {
      // Never break QQ startup because the reader failed to boot.
    }
  });
} catch (error) {
  try {
    // The loader must never prevent QQ from starting.
    // eslint-disable-next-line no-console
    console.error("[qqvibe-reader] load failed", error);
  } catch (_ignored) {
    // ignore
  }
}