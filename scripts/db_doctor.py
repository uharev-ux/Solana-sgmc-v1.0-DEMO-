"""
DB doctor: path, size, mtime, key tables, row counts, top-5 signal_events, trigger status distribution.
Usage: python scripts/db_doctor.py --db path.sqlite
"""

import argparse
import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))


def main() -> int:
    from dexscreener_screener import config
    from dexscreener_screener.storage import Database

    p = argparse.ArgumentParser(description="DB doctor: summary and diagnostics")
    p.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path")
    args = p.parse_args()
    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = root / db_path

    if not db_path.exists():
        print("DB not found:", db_path)
        return 1

    size = db_path.stat().st_size
    mtime = db_path.stat().st_mtime
    from time import strftime, localtime
    mtime_str = strftime("%Y-%m-%d %H:%M:%S", localtime(mtime))

    print("--- DB ---")
    print("path:", db_path)
    print("size:", size, "bytes")
    print("mtime:", mtime_str)

    db = Database(str(db_path))
    cur = db._conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    required = {"tokens", "pairs", "snapshots", "signal_events", "signal_trigger_evaluations"}
    missing = required - set(tables)
    if missing:
        print("WARN: missing tables:", sorted(missing))
    else:
        print("tables: OK")

    key_tables = ["tokens", "pairs", "snapshots", "signal_events", "signal_trigger_evaluations"]
    print("--- counts ---")
    for t in key_tables:
        if t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            n = cur.fetchone()[0]
            print("  %s: %s" % (t, n))
        else:
            print("  %s: (missing)" % t)

    if "signal_events" in tables:
        print("--- top-5 signal_events (id, created_at, pair/token) ---")
        cur.execute("""
            SELECT id, signal_ts, pair_address
            FROM signal_events
            ORDER BY id DESC
            LIMIT 5
        """)
        rows = cur.fetchall()
        for r in rows:
            print("  id=%s signal_ts=%s pair=%s" % (r[0], r[1], (r[2] or "")[:44]))
        if not rows:
            print("  (none)")

    if "signal_trigger_evaluations" in tables:
        print("--- trigger status distribution ---")
        cur.execute("SELECT status, COUNT(*) FROM signal_trigger_evaluations GROUP BY status ORDER BY status")
        for r in cur.fetchall():
            print("  %s: %s" % (r[0], r[1]))

    db.close()
    print("--- done ---")
    return 0


if __name__ == "__main__":
    sys.exit(main())
