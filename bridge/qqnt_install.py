"""Install, start and stop the injected QQNT reader.

QQNT is an Electron application. Its main process loads the native kernel
module `resources/app/wrapper.node`, which owns every chat read the QQ UI itself
performs. NapCat exploits this by injecting code into that main process; this
module does the same thing without vendoring NapCat: it copies the `qqnt/`
reader next to QQ's own launcher and patches that launcher to `require()` it.

Nothing here sends, recalls or mutates messages; the installed reader only ever
calls QQNT's read methods.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
READER_SRC = ROOT / "qqnt"
RUNTIME_DIR = ROOT / ".local" / "qqnt"
RUNTIME_FILE = RUNTIME_DIR / "runtime.json"
CONFIG_FILE = RUNTIME_DIR / "config.json"
LOG_FILE = RUNTIME_DIR / "qqnt-reader.log"

MARKER_BEGIN = "/* QQVIBE-READER-BEGIN */"
MARKER_END = "/* QQVIBE-READER-END */"
INJECTED_NAME = "qqvibe-reader"

_SNIPPET = (
    MARKER_BEGIN
    + "\n"
    + 'try { require(__dirname + "/../' + INJECTED_NAME + '/loadQqnt.js"); }'
    + " catch (error) { try { console.error('[qqvibe-reader]', error); } catch (_e) {} }\n"
    + MARKER_END
    + "\n"
)


class QqntInstallError(RuntimeError):
    pass


def _env_paths():
    env = os.environ
    home = Path.home()
    local_appdata = Path(env.get("LOCALAPPDATA") or home / "AppData" / "Local")
    program_files = Path(env.get("ProgramFiles") or r"C:\Program Files")
    program_files_x86 = Path(env.get("ProgramFiles(x86)") or r"C:\Program Files (x86)")
    return [
        local_appdata / "Programs" / "Tencent" / "QQNT",
        local_appdata / "Tencent" / "QQNT",
        program_files / "Tencent" / "QQNT",
        program_files_x86 / "Tencent" / "QQNT",
        Path(r"C:\Program Files\Tencent\QQNT"),
        Path(r"D:\Program Files\Tencent\QQNT"),
        Path(r"C:\Tencent\QQNT"),
    ]


def _is_install_dir(candidate: Path | None) -> bool:
    if candidate is None:
        return False
    return ((candidate / "QQ.exe").is_file() and
            (candidate / "resources" / "app" / "app_launcher" / "index.js").is_file())


def read_config() -> dict:
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_config(config: dict) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def find_install_dir() -> Path | None:
    """Locate the QQNT install root, honouring an explicit override first."""
    override = os.environ.get("QQVIBE_QQ_INSTALL")
    candidates = ([Path(override)] if override else []) + _env_paths()
    configured = read_config().get("qqInstallDir")
    if configured:
        candidates.insert(0, Path(configured))
    for candidate in candidates:
        if _is_install_dir(candidate):
            return candidate.resolve()
    return None


def _app_dir(install_dir: Path) -> Path:
    return install_dir / "resources" / "app"


def _launcher_path(install_dir: Path) -> Path:
    return _app_dir(install_dir) / "app_launcher" / "index.js"


def _backup_path(install_dir: Path) -> Path:
    return _app_dir(install_dir) / "app_launcher" / "index.js.qqvibe-backup"


def _injected_dir(install_dir: Path) -> Path:
    return _app_dir(install_dir) / INJECTED_NAME


def is_installed(install_dir: Path | None = None) -> bool:
    install = install_dir or find_install_dir()
    if install is None:
        return False
    launcher = _launcher_path(install)
    if not launcher.is_file():
        return False
    try:
        return MARKER_BEGIN in launcher.read_text(encoding="utf-8")
    except OSError:
        return False


def install(install_dir: Path | None = None) -> Path:
    """Copy the reader into QQ and patch its launcher. Idempotent."""
    install = install_dir or find_install_dir()
    if install is None:
        raise QqntInstallError("未找到 QQNT 安装目录，请设置 QQVIBE_QQ_INSTALL 指向 contains QQ.exe 的目录")
    if not READER_SRC.is_dir():
        raise QqntInstallError(f"读取器源码缺失: {READER_SRC}")

    target = _injected_dir(install)
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(READER_SRC, target, ignore=shutil.ignore_patterns("node_modules", "*.log"))
    # Record where the host project (Python bridge + GUI) lives, and where the
    # injected reader should publish its loopback descriptor.
    (target / "host.json").write_text(
        json.dumps({"hostRoot": str(ROOT), "runtimeDir": str(RUNTIME_DIR)},
                   ensure_ascii=False, indent=2),
        encoding="utf-8")

    launcher = _launcher_path(install)
    backup = _backup_path(install)
    source = launcher.read_text(encoding="utf-8")
    if MARKER_BEGIN not in source and not backup.exists():
        backup.write_text(source, encoding="utf-8")
    patched = _strip_block(source)
    launcher.write_text(_SNIPPET + patched, encoding="utf-8")

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    config = read_config()
    config.update({"qqInstallDir": str(install), "installedAt": int(time.time())})
    write_config(config)
    return install


def uninstall(install_dir: Path | None = None) -> None:
    install = install_dir or find_install_dir()
    if install is None:
        return
    launcher = _launcher_path(install)
    backup = _backup_path(install)
    if launcher.is_file():
        if backup.is_file():
            launcher.write_text(backup.read_text(encoding="utf-8"), encoding="utf-8")
            backup.unlink(missing_ok=True)
        else:
            launcher.write_text(_strip_block(launcher.read_text(encoding="utf-8")),
                                encoding="utf-8")
    shutil.rmtree(_injected_dir(install), ignore_errors=True)


def _strip_block(source: str) -> str:
    pattern = re.compile(re.escape(MARKER_BEGIN) + r".*?" + re.escape(MARKER_END) + r"\s*",
                         re.DOTALL)
    return pattern.sub("", source)


def read_descriptor() -> dict | None:
    """Read the loopback descriptor published by the injected reader."""
    try:
        data = json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or data.get("stopped"):
        return None
    if type(data.get("port")) is not int or not isinstance(data.get("token"), str):
        return None
    return data


def stale_descriptor() -> bool:
    """True when a descriptor exists but its publishing process is gone."""
    data = read_descriptor()
    if data is None:
        return False
    return not process_alive(int(data.get("pid") or 0))


def qq_process_ids() -> list[int]:
    """List running QQ main-process ids without importing heavy dependencies."""
    try:
        import psutil
    except ImportError:
        return _tasklist_qq()
    pids = []
    for process in psutil.process_iter(["pid", "name"]):
        try:
            if str((process.info or {}).get("name") or "").casefold() == "qq.exe":
                pids.append(int(process.info["pid"]))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return pids


def _tasklist_qq() -> list[int]:
    if os.name != "nt":
        return []
    try:
        result = subprocess.run(["tasklist", "/FI", "IMAGENAME eq QQ.exe", "/FO", "CSV", "/NH"],
                                capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return []
    pids = []
    for line in result.stdout.splitlines():
        parts = [part.strip('"') for part in line.split('","')]
        if len(parts) >= 2 and parts[0].casefold() == "qq.exe":
            try:
                pids.append(int(parts[1]))
            except ValueError:
                continue
    return pids


def process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        import psutil
    except ImportError:
        if os.name == "nt":
            return pid in _tasklist_qq()
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True
    return psutil.pid_exists(pid)


def launch_qq(install_dir: Path | None = None) -> None:
    """Start QQ so the injected reader loads. Never restarts a running QQ."""
    if qq_process_ids():
        return
    install = install_dir or find_install_dir()
    if install is None:
        raise QqntInstallError("未找到 QQNT 安装目录，无法启动 QQ")
    exe = install / "QQ.exe"
    if not exe.is_file():
        raise QqntInstallError(f"未找到 QQ.exe: {exe}")
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    subprocess.Popen([str(exe)], cwd=str(install), close_fds=True,
                     creationflags=creationflags)


def ensure_injected(wait_seconds: float = 40.0, restart_hint=None) -> dict | None:
    """Install (if needed), make sure QQ is running, and wait for the descriptor.

    Returns the reader descriptor, or None when the reader never published one
    within `wait_seconds`.
    """
    install = find_install_dir()
    if install is None:
        return None
    if not is_installed(install):
        install(install)
    descriptor = read_descriptor()
    if descriptor and process_alive(int(descriptor.get("pid") or 0)):
        # A live reader already answered; QQ is up and injected.
        return descriptor
    launch_qq(install)
    deadline = time.monotonic() + max(0.0, wait_seconds)
    while time.monotonic() < deadline:
        descriptor = read_descriptor()
        if descriptor and process_alive(int(descriptor.get("pid") or 0)):
            return descriptor
        time.sleep(0.4)
    if restart_hint is not None:
        restart_hint()
    return read_descriptor()


def log_tail(lines: int = 40) -> str:
    try:
        content = LOG_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(content[-lines:])


def main(argv=None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="QQNT reader installer")
    parser.add_argument("command", choices=("install", "uninstall", "status", "log"))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.command == "install":
            install_dir = install()
            payload = {"installed": True, "installDir": str(install_dir)}
        elif args.command == "uninstall":
            uninstall()
            payload = {"installed": False}
        elif args.command == "log":
            print(log_tail())
            return 0
        else:
            install_dir = find_install_dir()
            payload = {"installDir": str(install_dir) if install_dir else None,
                       "installed": is_installed(install_dir),
                       "descriptor": read_descriptor(),
                       "qqRunning": bool(qq_process_ids())}
    except QqntInstallError as error:
        if args.json:
            print(json.dumps({"ok": False, "error": str(error)}, ensure_ascii=False))
        else:
            print(f"QQNT 读取器操作失败: {error}", file=sys.stderr)
        return 1
    print(json.dumps(payload, ensure_ascii=False) if args.json else payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())