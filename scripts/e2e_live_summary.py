"""Print E2E_LIVE SQL summary for a DB. Usage: python scripts/e2e_live_summary.py <db_path>"""
import sqlite3
import sys
from pathlib import Path

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/e2e_live_summary.py <db_path>", file=sys.stderr)
        sys.exit(1)
    db_path = Path(sys.argv[1])
    if not db_path.exists():
        print("E2E_LIVE summary: DB not found", db_path, file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    out = []
    try:
        cur.execute("SELECT COUNT(*) FROM signal_events")
        out.append("signal_events: %s" % cur.fetchone()[0])
    except Exception:
        out.append("signal_events: N/A")
    try:
        cur.execute("SELECT status, COUNT(*) FROM signal_trigger_evaluations GROUP BY status")
        rows = cur.fetchall()
        out.append("signal_trigger_evaluations by status: %s" % dict(rows))
    except Exception:
        out.append("signal_trigger_evaluations: N/A")
    for t in ("snapshots", "pairs", "tokens"):
        try:
            cur.execute("SELECT COUNT(*) FROM %s" % t)
            out.append("%s: %s" % (t, cur.fetchone()[0]))
        except Exception:
            out.append("%s: N/A" % t)
    conn.close()
    print("--- E2E_LIVE summary ---")
    for line in out:
        print(line)
    print("---")

if __name__ == "__main__":
    main()
