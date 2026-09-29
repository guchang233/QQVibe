"""Stage reviewed public client inputs into a new runtime stage.

Two things are refused outright: anything the allowlists do not name, and a
client without the ntqq-reader sidecar. The bridge cannot read QQ data without
that binary, so a package that lacks it is not a release.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

# The folder list and the file names come from the bridge module rather than
# being repeated here: bridge/ntqq_reader.py is what resolves the binary at
# runtime, so staging cannot drift from the lookup. Only *where the binary is
# read from* is decided in this file.
_READER_SPEC = importlib.util.spec_from_file_location(
    "ntqq_reader", ROOT / "bridge" / "ntqq_reader.py")
if _READER_SPEC is None or _READER_SPEC.loader is None:
    raise RuntimeError("the sidecar locator is unavailable")
reader = importlib.util.module_from_spec(_READER_SPEC)
_READER_SPEC.loader.exec_module(reader)

#: Where the reader lands inside the client stage. The stage root is copied to
#: ``resources/client``, and the bridge resolves ``<client>/resources/ntqq-reader``,
#: so this is the one path that makes a packaged build find its own binary.
#: The release is Windows-only, hence EXE_NAMES[0].
SIDECAR_TARGET = Path(reader.PACKAGED_FOLDER) / reader.EXE_NAMES[0]

SCRIPTS = (
    "start-real-client.py", "start-real-client.cmd", "desktop-main.cjs",
    "real-client-shell.cjs", "real-client-preload.cjs", "real-client-recovery.cjs",
    "real-client-update.cjs", "real-client-update-proxy.cjs", "real-client-model.cjs",
    "real-client-update-controller.cjs",
    "real-client-update-helper.cjs", "real-client-update-extract.py",
    "update-signing.pub", "model-files.json", "model-asset.json",
)
BRIDGE = (
    "account_api.py", "account_store.py", "conversation_selection.py",
    "analysis_server.ts", "batch_engine.py",
    "batch_state.py", "chat_server.py", "history_browser.py",
    "instance_identity.py", "model_source.py", "model_bundle.py",
    "local_model_source.py", "model_install.py", "profile_signals.py", "profile_state.py",
    "backend_contracts.py", "backend_service.py", "message_results.py", "message_contracts.py",
    "message_input.py", "portrait_contracts.py", "api_tasks.py", "node_analysis.py",
    "result_store.py",
    "real_backend.py", "real_http.py",
    "qq_source.py", "ntqq_reader.py",
    "windows_file_owners.py",
)
LAYA = (
    "agent.ts", "calibration.ts", "catalog.ts", "context.ts", "expression.ts",
    "forecast.ts", "general-intent.ts", "grounded-intent.ts", "index.ts", "LICENSE",
    "message-batch.ts", "NOTICE", "options.ts", "personality.ts", "prompt.ts",
    "pyjson.ts", "questions.ts", "runner.ts", "scoring.ts", "social-cues.ts",
    "social-intents.ts", "style.ts", "tokenizer.ts", "types.ts",
)
MODEL_FILES = (
    "model.onnx", "onnx_config.json", "rl_agent_config.json", "README.md",
    "tokenizer/tokenizer.json", "tokenizer/tokenizer_config.json",
)
# One manifest pins staging, local selection and the separately downloaded model.
_model_manifest = json.loads((ROOT / "scripts/model-files.json").read_text(encoding="utf-8"))
MODEL_PINS = {name: (entry["bytes"], entry["sha256"])
              for name, entry in _model_manifest["files"].items()}
if _model_manifest.get("schema") != 1 or set(MODEL_FILES) != set(MODEL_PINS):
    raise RuntimeError("pinned model manifest differs from stage allowlist")
PUBLIC_FILES = (
    "LICENSE", "THIRD_PARTY_NOTICES.md", "README.md", "chatui/index.html",
    "chatui/app.js", "chatui/message-labels.js", "chatui/message-insight-adapters.js", "chatui/view-state.js", "chatui/style.css", "chatui/kaomoji.js",
    "chatui/data/analysis-catalog.json", "chatui/assets/wechatvibe-icon.png",
    "chatui/assets/wechatvibe-icon.ico", "electron/analysis.ts",
    "electron/model-connectors.ts", "electron/api-insights.ts",
    "electron/api-message-insights.ts", "electron/api-portrait.ts", "electron/api-analysis-json.ts",
    "electron/local-message-insights.ts",
    "shared/contracts.ts", "shared/message-input.ts", "src/lib/labels.ts",
)


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def inventory(root: Path) -> dict[str, Path]:
    if root.is_symlink() or getattr(root, "is_junction", lambda: False)():
        raise ValueError(f"linked staging root: {root}")
    found = {}
    if not root.exists():
        return found
    if not root.is_dir():
        raise ValueError(f"staging root is not a directory: {root}")
    for base, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            item = Path(base) / name
            if item.is_symlink() or getattr(item, "is_junction", lambda: False)():
                raise ValueError(f"link in staging tree: {item}")
            if not item.is_dir() and not item.is_file():
                raise ValueError(f"unexpected staging entry: {item}")
            if item.is_file():
                relative = item.relative_to(root).as_posix()
                folded = relative.casefold()
                if folded in found:
                    raise ValueError(f"case-colliding staging file: {relative}")
                found[folded] = item
    return found


def verify_directories(root: Path, files: set[str]) -> None:
    expected = set()
    for filename in files:
        parts = Path(filename).parts
        expected.update(Path(*parts[:index]).as_posix().casefold()
                        for index in range(1, len(parts)))
    actual = set()
    for base, directories, _ in os.walk(root, followlinks=False):
        for name in directories:
            actual.add(((Path(base) / name).relative_to(root)).as_posix().casefold())
    if actual != expected:
        raise ValueError("stage has unexpected or missing directories")


def verify_runtime_stage(output: Path) -> set[str]:
    manifest = output.parent / "runtime-manifest.json"
    existing = inventory(output)
    if not manifest.exists():
        if existing:
            raise ValueError("nonempty stage lacks runtime manifest")
        if output.exists():
            verify_directories(output, set())
        return set()
    rows = json.loads(manifest.read_text(encoding="utf-8")).get("files")
    if not isinstance(rows, list):
        raise ValueError("runtime manifest lacks exact file inventory")
    expected = {}
    for row in rows:
        relative = row["file"]
        path = Path(relative)
        if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
            raise ValueError("unsafe runtime inventory path")
        folded = relative.casefold()
        if folded in expected or not (relative.startswith("runtime/") or relative.startswith("node_modules/")):
            raise ValueError(f"unexpected runtime inventory path: {relative}")
        expected[folded] = row
    if set(existing) != set(expected):
        raise ValueError("runtime stage has unexpected or missing content")
    verify_directories(output, set(expected))
    for folded, row in expected.items():
        path = existing[folded]
        if path.stat().st_size != row["bytes"] or digest(path) != row["sha256"]:
            raise ValueError(f"runtime stage hash differs: {row['file']}")
    return set(expected)


def find_sidecar_exe(source: Path) -> Path | None:
    """The reader binary under ``source``, searched exactly as the bridge does.

    ``--source-root`` is searched rather than the checkout this script lives in,
    so staging a synthetic tree never picks up a real local build.
    """
    for folder in reader.SEARCH_FOLDERS:
        for name in reader.EXE_NAMES:
            candidate = source / folder / name
            if candidate.is_file():
                return candidate
    return None


def resolve_sidecar(source: Path, requested: Path | None) -> Path:
    """The reader binary to package, verified to actually be one.

    Precedence follows the model directory: explicit flag, then environment,
    then a cargo build output under the source root.
    """
    if requested is not None:
        candidate: Path | None = Path(requested)
    else:
        override = os.environ.get(reader.SIDECAR_EXE_ENV)
        candidate = Path(override) if override else find_sidecar_exe(source)
    if candidate is None:
        raise ValueError(
            "the ntqq-reader sidecar was not found; build it with "
            "`cargo build --release` under sidecar/ntqq-reader, download the "
            f"CI artifact and point {reader.SIDECAR_EXE_ENV} at it, or pass "
            "--sidecar-exe")
    if not candidate.is_file():
        raise ValueError(f"sidecar executable is missing: {candidate}")
    if candidate.stat().st_size < 2:
        raise ValueError(f"sidecar executable is empty: {candidate}")
    with candidate.open("rb") as stream:
        header = stream.read(2)
    if header != b"MZ":
        # A cargo target directory also holds .d and .rlib build noise. Staging
        # the wrong file would only show up as a launch failure in the package.
        raise ValueError(f"sidecar executable is not a PE image: {candidate}")
    return candidate.resolve()


def public_mappings(source: Path, models: Path,
                    sidecar: Path) -> list[tuple[Path, Path, Path]]:
    project_files = [Path(name) for name in PUBLIC_FILES]
    project_files += [Path("scripts") / name for name in SCRIPTS]
    project_files += [Path("bridge") / name for name in BRIDGE]
    project_files += [Path("electron/laya") / name for name in LAYA]
    mappings = [(source / relative, relative, source) for relative in project_files]
    mappings += [(models / Path(name), Path(".models/laya") / name, models)
                 for name in MODEL_FILES]
    # The allowlist root for the exe is its own directory: the path was named
    # explicitly (or found by the bridge's own search), not swept up from a tree.
    mappings += [(sidecar, SIDECAR_TARGET, sidecar.parent)]
    return mappings


def stage_public(source: Path, models: Path, output: Path,
                 sidecar: Path | None = None) -> dict:
    source, models, output = source.resolve(), models.resolve(), output.absolute()
    if output == source or source.is_relative_to(output) or output == models or models.is_relative_to(output):
        raise ValueError("staging output must not replace a source directory")
    if (output.parent / "client-files.json").exists():
        raise ValueError("client stage already has a manifest; create a new build directory")
    executable = resolve_sidecar(source, sidecar)
    runtime_files = verify_runtime_stage(output)
    mappings = public_mappings(source, models, executable)
    names = [relative.as_posix().casefold() for _, relative, _ in mappings]
    if len(names) != len(set(names)) or set(names) & runtime_files:
        raise ValueError("duplicate or colliding client staging path")
    version = json.loads((source / "package.json").read_text(encoding="utf-8"))["version"]
    if not isinstance(version, str) or not version:
        raise ValueError("source package version is invalid")
    prepared = []
    for original, relative, allowed_root in mappings:
        if (original.is_symlink() or getattr(original, "is_junction", lambda: False)() or
                not original.is_file() or not original.resolve().is_relative_to(allowed_root)):
            raise ValueError(f"missing or unsafe public input: {relative.as_posix()}")
        length, checksum = original.stat().st_size, digest(original)
        if relative.parts[:2] == (".models", "laya"):
            model_name = Path(*relative.parts[2:]).as_posix()
            if model_name in MODEL_PINS and (length, checksum) != MODEL_PINS[model_name]:
                raise ValueError(f"pinned model hash differs: {model_name}")
        prepared.append((original, relative, length, checksum))
    output.mkdir(parents=True, exist_ok=True)
    rows = []
    for original, relative, length, checksum in prepared:
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(original, target)
        if target.stat().st_size != length or digest(target) != checksum or digest(original) != checksum:
            raise ValueError(f"client input changed while staging: {relative.as_posix()}")
        rows.append({"file": relative.as_posix(), "bytes": length, "sha256": checksum})
    metadata = output / "package.json"
    metadata.write_text(json.dumps({"name": "wechatvibe-runtime", "version": version,
                                    "private": True, "type": "module"}, indent=2) + "\n",
                        encoding="utf-8")
    rows.append({"file": "package.json", "bytes": metadata.stat().st_size,
                 "sha256": digest(metadata)})
    expected = runtime_files | {row["file"].casefold() for row in rows}
    if set(inventory(output)) != expected:
        raise ValueError("client stage has unexpected or missing content")
    verify_directories(output, expected)
    result = {"sourceVersion": version, "sourcePackageSha256": digest(source / "package.json"),
              "sidecar": SIDECAR_TARGET.as_posix(), "files": rows}
    (output.parent / "client-files.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--models-dir", type=Path)
    parser.add_argument("--sidecar-exe", type=Path,
                        help="ntqq-reader binary to package; defaults to the release "
                             f"build under sidecar/ntqq-reader or ${reader.SIDECAR_EXE_ENV}")
    parser.add_argument("--output", type=Path, required=True,
                        help="new stage created by the clean build driver")
    args = parser.parse_args(argv)
    source = args.source_root.resolve()
    models = (args.models_dir or source / ".models/laya").resolve()
    try:
        result = stage_public(source, models, args.output, args.sidecar_exe)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        parser.exit(1, f"Client stage rejected: {error}\n")
    print(json.dumps({"publicFiles": len(result["files"]),
                      "bytes": sum(row["bytes"] for row in result["files"]),
                      "sourceVersion": result["sourceVersion"],
                      "sidecar": result["sidecar"],
                      "userDataCopied": False, "output": str(args.output.absolute())}, ensure_ascii=True))


if __name__ == "__main__":
    main()
