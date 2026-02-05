"""
DB schema check: temp DB, init via storage/sqlite, verify tables and key columns.
Run from project root: python scripts/db_check.py
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

DB_PATH = _root / "debug_tmp_schema.sqlite"

def main() -> int:
    if DB_PATH.exists():
        DB_PATH.unlink()
    try:
        from dexscreener_screener.storage import Database
        db = Database(str(DB_PATH))
        conn = db._conn
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = {r[0] for r in cur.fetchall()}
        required = {"tokens", "pairs", "snapshots", "signal_events", "signal_trigger_evaluations"}
        missing = required - tables
        if missing:
            print("DB_CHECK: FAIL missing tables:", missing)
            db.close()
            DB_PATH.unlink(missing_ok=True)
            return 1
        # signal_trigger_evaluations: signal_id, status, evaluated_at
        cur.execute("PRAGMA table_info(signal_trigger_evaluations)")
        cols = {r[1].lower() for r in cur.fetchall()}
        for c in ("signal_id", "status", "evaluated_at"):
            if c.lower() not in cols:
                print("DB_CHECK: FAIL signal_trigger_evaluations missing column:", c)
                db.close()
                DB_PATH.unlink(missing_ok=True)
                return 1
        # Summary
        for t in sorted(tables):
            cur.execute(f"PRAGMA table_info({t})")
            info = cur.fetchall()
            print("  %s: %s columns" % (t, len(info)))
        db.close()
        DB_PATH.unlink(missing_ok=True)
        print("DB_CHECK: OK")
        return 0
    except Exception as e:
        if DB_PATH.exists():
            DB_PATH.unlink(missing_ok=True)
        print("DB_CHECK: FAIL", e)
        return 1

if __name__ == "__main__":
    sys.exit(main())
