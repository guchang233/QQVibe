#!/usr/bin/env python3
"""Read-only local chat UI entry point; no chat database is opened on import."""
from __future__ import annotations


def main():
    # Embedded Python's isolated path omits the script directory. Import only
    # the bridge modules shipped beside this trusted entry point.
    import sys
    from pathlib import Path
    bridge_dir = str(Path(__file__).resolve().parent)
    if bridge_dir not in sys.path:
        sys.path.insert(0, bridge_dir)
    from real_http import main as serve
    return serve()


if __name__ == "__main__":
    raise SystemExit(main())
