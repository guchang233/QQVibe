"use strict";
/**
 * Entry point for the injected QQNT reader.
 *
 * `loadQqnt.js` (copied next to QQ's own `app_launcher/index.js`) requires this
 * module from inside QQ's Electron main process. Starting the reader is
 * deliberately side-effect free on import: `main()` is only called when the
 * loader asks for it.
 */
const paths = require("./paths");
const { QqntReader, stableId, compareSort, SHARD } = require("./reader");
const { startServer, main } = require("./server");
const elements = require("./elements");

module.exports = {
  paths,
  QqntReader,
  stableId,
  compareSort,
  SHARD,
  startServer,
  main,
  elements,
};