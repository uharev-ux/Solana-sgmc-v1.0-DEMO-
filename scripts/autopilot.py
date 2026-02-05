"""
Autopilot: compileall, smoke_test, strategy_selfcheck.
Run from project root: python scripts/autopilot.py [--db path]
"""

import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))


def main() -> int:
    import argparse
    from dexscreener_screener import config

    p = argparse.ArgumentParser(description="Autopilot: compileall, smoke_test, strategy_selfcheck")
    p.add_argument("--db", default=config.DEFAULT_DB, help="SQLite DB for strategy_selfcheck")
    args = p.parse_args()
    db_path = args.db

    failed = []

    print("[autopilot] compileall ...")
    r = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", str(root / "dexscreener_screener"), str(root / "scripts")],
        cwd=str(root),
        capture_output=True,
        timeout=60,
    )
    if r.returncode != 0:
        failed.append("compileall")
        print("  FAIL: compileall exit code %s" % r.returncode)
    else:
        print("  OK")

    print("[autopilot] smoke_test ...")
    r = subprocess.run(
        [sys.executable, str(root / "scripts" / "smoke_test.py")],
        cwd=str(root),
        capture_output=True,
        timeout=120,
    )
    if r.returncode != 0:
        failed.append("smoke_test")
        print("  FAIL: smoke_test exit code %s" % r.returncode)
        if r.stdout:
            print(r.stdout.decode(errors="replace"))
        if r.stderr:
            print(r.stderr.decode(errors="replace"))
    else:
        print("  OK")

    print("[autopilot] strategy_selfcheck ...")
    r = subprocess.run(
        [sys.executable, str(root / "scripts" / "strategy_selfcheck.py"), "--db", str(db_path)],
        cwd=str(root),
        capture_output=True,
        timeout=60,
    )
    if r.returncode != 0:
        failed.append("strategy_selfcheck")
        print("  FAIL: strategy_selfcheck exit code %s" % r.returncode)
        if r.stdout:
            print(r.stdout.decode(errors="replace"))
        if r.stderr:
            print(r.stderr.decode(errors="replace"))
    else:
        print("  OK")

    if failed:
        print("[autopilot] FAILED: %s" % ", ".join(failed))
        return 1
    print("[autopilot] All checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
