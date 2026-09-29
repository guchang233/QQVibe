"use strict";
/**
 * Shared filesystem/runtime paths for the injected QQNT reader.
 *
 * The reader runs inside QQ's Electron main process and therefore must not
 * depend on the project's working directory. Every writable location is either
 * passed through the environment at launch time or derived from this file's
 * location, never from `process.cwd()`.
 */
const fs = require("fs");
const os = require("os");
const path = require("path");

// This module is copied into QQ's own application folder, so its `__dirname`
// points at `<install>/resources/app/qqvibe-reader/src`. The host project root
// (where the Python bridge and the GUI live) is recorded at install time in
// `host.json`, because QQ is started by the user and cannot pass us an
// environment variable.
const HOST_FILE = path.resolve(__dirname, "..", "host.json");

function hostConfig() {
  try {
    return JSON.parse(fs.readFileSync(HOST_FILE, "utf8"));
  } catch (_error) {
    return null;
  }
}

/** Absolute QQ install root: `<app>/qqvibe-reader/src` -> two levels above `app`. */
function installedRoot() {
  return path.resolve(__dirname, "..", "..", "..", "..");
}

const PROJECT_ROOT = (hostConfig() && hostConfig().hostRoot) || path.resolve(__dirname, "..", "..");
const DEFAULT_RUNTIME_DIR =
  (hostConfig() && hostConfig().runtimeDir) || path.join(PROJECT_ROOT, ".local", "qqnt");

function runtimeDir() {
  return process.env.QQVIBE_RUNTIME_DIR || DEFAULT_RUNTIME_DIR;
}

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
  return target;
}

function configPath() {
  return path.join(runtimeDir(), "config.json");
}

function runtimePath() {
  return path.join(runtimeDir(), "runtime.json");
}

function logPath() {
  return path.join(runtimeDir(), "qqnt-reader.log");
}

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch (_error) {
    return null;
  }
}

function writeJson(file, value) {
  ensureDir(path.dirname(file));
  const temporary = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  fs.renameSync(temporary, file);
}

/** Persisted install/launch configuration (QQ install dir, token, port hint). */
function readConfig() {
  return readJson(configPath());
}

function writeConfig(config) {
  writeJson(configPath(), config);
}

/** Live reader descriptor written by the injected loader on every startup. */
function readRuntime() {
  return readJson(runtimePath());
}

function writeRuntime(value) {
  writeJson(runtimePath(), value);
}

let logStream = null;

function log(...parts) {
  const line = `[${new Date().toISOString()}] ${parts.map(String).join(" ")}\n`;
  try {
    if (logStream === null) {
      ensureDir(runtimeDir());
      logStream = fs.createWriteStream(logPath(), { flags: "a" });
    }
    logStream.write(line);
  } catch (_error) {
    // Logging must never break the embedded reader.
  }
}

/** Default install roots for QQNT on Windows, newest layouts first. */
function candidateInstallDirs() {
  const roots = [];
  const env = process.env;
  if (env.QQVIBE_QQ_INSTALL) {
    roots.push(env.QQVIBE_QQ_INSTALL);
  }
  const home = os.homedir();
  const localAppData = env.LOCALAPPDATA || path.join(home, "AppData", "Local");
  const programFiles = env.ProgramFiles || "C:\\Program Files";
  const programFilesX86 = env["ProgramFiles(x86)"] || "C:\\Program Files (x86)";
  roots.push(
    path.join(programFiles, "Tencent", "QQNT"),
    path.join(programFilesX86, "Tencent", "QQNT"),
    path.join(localAppData, "Programs", "Tencent", "QQNT"),
    path.join(localAppData, "Tencent", "QQNT"),
    "C:\\Program Files\\Tencent\\QQNT",
    "D:\\Program Files\\Tencent\\QQNT",
    "C:\\Tencent\\QQNT",
  );
  return [...new Set(roots)];
}

/** A directory is a QQNT install root when it owns the Electron app entry. */
function isInstallDir(candidate) {
  if (!candidate) {
    return false;
  }
  return (
    fs.existsSync(path.join(candidate, "QQ.exe")) &&
    fs.existsSync(path.join(candidate, "resources", "app", "app_launcher", "index.js"))
  );
}

function findInstallDir() {
  // When running injected, the enclosing install is authoritative.
  const inside = installedRoot();
  if (isInstallDir(inside)) {
    return inside;
  }
  const configured = readConfig();
  if (configured && isInstallDir(configured.qqInstallDir)) {
    return configured.qqInstallDir;
  }
  for (const candidate of candidateInstallDirs()) {
    if (isInstallDir(candidate)) {
      return candidate;
    }
  }
  return null;
}

function appDir(installDir) {
  return path.join(installDir, "resources", "app");
}

function launcherPath(installDir) {
  return path.join(appDir(installDir), "app_launcher", "index.js");
}

function launcherBackupPath(installDir) {
  return path.join(appDir(installDir), "app_launcher", "index.js.qqvibe-backup");
}

function wrapperPath(installDir) {
  const candidates = [
    path.join(appDir(installDir), "wrapper.node"),
    path.join(installDir, "wrapper.node"),
    path.join(installDir, "resources", "wrapper.node"),
  ];
  return candidates.find((candidate) => fs.existsSync(candidate)) || null;
}

/** Directory our loader is copied into inside QQ's own app folder. */
function injectedDir(installDir) {
  return path.join(appDir(installDir), "qqvibe-reader");
}

function injectedLoaderPath(installDir) {
  return path.join(injectedDir(installDir), "loadQqnt.js");
}

module.exports = {
  PROJECT_ROOT,
  hostConfig,
  installedRoot,
  runtimeDir,
  ensureDir,
  configPath,
  runtimePath,
  logPath,
  readConfig,
  writeConfig,
  readRuntime,
  writeRuntime,
  log,
  candidateInstallDirs,
  isInstallDir,
  findInstallDir,
  appDir,
  launcherPath,
  launcherBackupPath,
  wrapperPath,
  injectedDir,
  injectedLoaderPath,
};