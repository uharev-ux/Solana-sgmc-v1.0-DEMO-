"""Check that required tables exist in DB; WARN on orphaned records (pairs without tokens, snapshots without pairs). Exit 0 if all present, 1 otherwise. Usage: python scripts/prune_schema_check.py <db_path>"""
import sqlite3
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        sys.exit(1)
    db_path = Path(sys.argv[1])
    if not db_path.exists():
        sys.exit(1)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = {r[0] for r in cur.fetchall()}
    required = {"tokens", "pairs", "snapshots", "signal_events", "signal_trigger_evaluations"}
    missing = required - tables
    if missing:
        conn.close()
        sys.exit(1)

    # Orphan checks: pairs without tokens, snapshots without pairs (soft WARN)
    if "pairs" in tables and "tokens" in tables:
        cur.execute("""
            SELECT COUNT(*) FROM pairs p
            WHERE NOT EXISTS (SELECT 1 FROM tokens t WHERE t.address = p.base_address OR t.address = p.quote_address)
        """)
        pairs_no_tokens = cur.fetchone()[0]
        if pairs_no_tokens > 0:
            print("WARN: %s pair(s) have no matching token(s)" % pairs_no_tokens)
    if "snapshots" in tables and "pairs" in tables:
        cur.execute("""
            SELECT COUNT(*) FROM snapshots s
            WHERE NOT EXISTS (SELECT 1 FROM pairs p WHERE p.pair_address = s.pair_address)
        """)
        snapshots_no_pairs = cur.fetchone()[0]
        if snapshots_no_pairs > 0:
            print("WARN: %s snapshot(s) have no matching pair(s)" % snapshots_no_pairs)

    conn.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
