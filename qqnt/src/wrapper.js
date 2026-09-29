"use strict";
/**
 * QQNT kernel-service access, extracted from the way NapCat binds to QQ's own
 * native combined module (`wrapper.node`). We deliberately do not vendor or run
 * NapCat itself: this is a minimal, read-only re-implementation of the same idea.
 *
 * QQNT is an Electron application. Its main process loads `resources/app/wrapper.node`,
 * a C++ node addon that exposes the `NodeIKernel*Service` family used by QQ's own UI.
 * Because our loader runs in that same main process, `require()` of that addon gives
 * us the very same service objects. Only read methods are ever called.
 */
const { wrapperPath, log } = require("./paths");

let wrapper = null;

function loadWrapper(qqInstallDir) {
  if (wrapper) {
    return wrapper;
  }
  const resolved = wrapperPath(qqInstallDir);
  if (!resolved) {
    throw new Error("QQNT wrapper.node not found for the selected install directory");
  }
  // QQNT loads its own addon with a bare require; a second require returns the
  // cached module and therefore the same native service graph.
  wrapper = require(resolved);
  log("wrapper.node loaded:", resolved);
  return wrapper;
}

function getWrapper() {
  if (!wrapper) {
    throw new Error("QQNT wrapper is not loaded yet");
  }
  return wrapper;
}

/**
 * Resolve one kernel service defensively. QQNT has renamed accessor spellings
 * across versions, so several candidates are accepted before failing.
 */
function service(names) {
  const instance = getWrapper();
  for (const name of names) {
    const value = instance[name];
    if (typeof value === "function") {
      const candidate = value.call(instance);
      if (candidate) {
        return candidate;
      }
    }
    if (value && typeof value === "object") {
      return value;
    }
  }
  throw new Error(`QQNT service unavailable: ${names.join("/")}`);
}

const services = {
  msg: () => service(["getMsgService", "NodeIKernelMsgService"]),
  profile: () => service(["getProfileService", "NodeIKernelProfileService"]),
  buddy: () => service(["getBuddyService", "NodeIKernelBuddyService"]),
  group: () => service(["getGroupService", "NodeIKernelGroupService"]),
  recent: () => service(["getRecentContactService", "NodeIKernelRecentContactService"]),
  avatar: () => service(["getAvatarService", "NodeIKernelAvatarService"]),
  richMedia: () => service(["getRichMediaService", "NodeIKernelRichMediaService"]),
  file: () => service(["getFileService", "NodeIKernelFileService"]),
  misc: () => service(["getNodeMiscService", "getMiscService", "NodeIKernelNodeMiscService"]),
};

/** Call a possibly-promise-returning kernel method and normalize the result. */
async function invoke(target, method, ...args) {
  if (target === null || target === undefined) {
    throw new Error(`kernel object missing for ${method}`);
  }
  const fn = target[method];
  if (typeof fn !== "function") {
    throw new Error(`kernel method unavailable: ${method}`);
  }
  return await fn.apply(target, args);
}

/** Try methods in order; the first one that resolves without throwing wins. */
async function invokeAny(targets, method, ...args) {
  const errors = [];
  for (const [label, target] of targets) {
    try {
      return await invoke(target, method, ...args);
    } catch (error) {
      errors.push(`${label}.${method}: ${error && error.message ? error.message : error}`);
    }
  }
  throw new Error(errors.join(" | "));
}

module.exports = { loadWrapper, getWrapper, services, invoke, invokeAny };