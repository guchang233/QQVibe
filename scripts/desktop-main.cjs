"use strict";

const { app, dialog } = require("electron");
const { execFile } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const root = app.isPackaged ? path.join(process.resourcesPath, "client") : path.resolve(__dirname, "..");
const bundledPython = path.join(root, "runtime", "python", "python.exe");
const bundledNode = path.join(root, "runtime", "node", "node.exe");
const projectPython = path.join(root, ".venv", "Scripts", "python.exe");
const python = process.env.QQVIBE_PYTHON || (fs.existsSync(bundledPython) ? bundledPython :
  fs.existsSync(projectPython) ? projectPython : "python");
const node = process.env.QQVIBE_NODE || (fs.existsSync(bundledNode) ? bundledNode : "node");
const launcher = path.join(root, "scripts", "start-real-client.py");
let profileReady = false;
// True only when this launch created the bridge (not when it reused a ready one).
let bridgeCreated = false;
try {
  // Set this before app ready so Electron's single-instance lock is per installation.
  const userData = path.join(root, ".local", "real-client-shell");
  fs.mkdirSync(userData, { recursive: true });
  app.setPath("userData", userData);
  profileReady = true;
} catch (_) {
  // Report through the normal startup failure path once Electron is ready.
}

// Ask the launcher to stop the bridge only when this launch created it. The launcher
// re-verifies the recorded PID creation time, install identity and control token, so a
// reused or foreign service is never touched. A JSON-invalid result is matched leniently
// for "created":true so a corrupt stdout cannot orphan a bridge we just started.
function stopOwnedBridge(callback) {
  const owned = bridgeCreated;
  if (!owned) {
    callback();
    return;
  }
  const environment = {
    ...process.env, QQVIBE_CLIENT_ROOT: root, QQVIBE_PYTHON: python,
  };
  try {
    execFile(python, [launcher, "--stop-owned-bridge", "--json"], {
      cwd: root, env: environment, windowsHide: true, timeout: 30000, maxBuffer: 65536,
    }, () => callback());
  } catch (_) {
    callback();
  }
}

function fail() {
  dialog.showErrorBox("QQVibe 启动失败", "本地服务未就绪，请检查运行文件是否完整。");
  app.quit();
}

function failAfterCleanup() {
  stopOwnedBridge(() => fail());
}

app.whenReady().then(() => {
  if (!profileReady || !fs.existsSync(launcher) ||
      (app.isPackaged && (!fs.existsSync(python) || !fs.existsSync(node)))) {
    fail();
    return;
  }
  const environment = {
    ...process.env,
    QQVIBE_CLIENT_ROOT: root,
    QQVIBE_PYTHON: python,
    QQVIBE_NODE: node,
    PATH: (path.isAbsolute(node) ? path.dirname(node) + path.delimiter : "") + (process.env.PATH || ""),
  };
  // Desktop launches derive their port from this installation, even when a
  // terminal or parent process exported an older client's CHATUI_PORT.
  delete environment.CHATUI_PORT;
  execFile(python, [launcher, "--no-open", "--json"], {
    cwd: root, env: environment, windowsHide: true, timeout: 45000, maxBuffer: 65536,
  }, (error, stdout) => {
    if (error) {
      // The launcher stops a bridge it started before failing, so do not stop one here.
      fail();
      return;
    }
    let result;
    try {
      result = JSON.parse(stdout);
      bridgeCreated = result.created === true;
      const match = /^http:\/\/127\.0\.0\.1:(\d{1,5})$/.exec(result.url);
      if (result.version !== "real-ui-1" || !match || !/^[a-f0-9]{64}$/.test(result.instanceId) ||
          Number(match[1]) < 1 || Number(match[1]) > 65535) throw new Error("Invalid launcher result");
      Object.assign(process.env, environment, {
        CHATUI_PORT: match[1], QQVIBE_INSTANCE_ID: result.instanceId,
        QQVIBE_BRIDGE_CREATED: bridgeCreated ? "1" : "0",
      });
    } catch (_) {
      if (!bridgeCreated) bridgeCreated = /"created"\s*:\s*true/.test(String(stdout || ""));
      failAfterCleanup();
      return;
    }
    process.argv.push("--client-url", `${result.url}/`);
    try {
      require("./real-client-shell.cjs");
    } catch (_) {
      failAfterCleanup();
    }
  });
}).catch(fail);
