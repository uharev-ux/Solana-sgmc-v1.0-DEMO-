"""
Smoke tests for DexScreener Screener v1.
Run from project root: python scripts/smoke_test.py
No pytest required.
"""

import json
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dexscreener_screener.client import DexScreenerClient
from dexscreener_screener.models import PairSnapshot, from_api_pair
from dexscreener_screener.pipeline import Collector
from dexscreener_screener.storage import Database

# Real Solana pair address (from DexScreener tokens API)
KNOWN_PAIR = "3nMFwZXwY1s1M5s8vYAHqd4wGs4iSxXE4LRoUMMYqEgF"
KNOWN_TOKEN = "So11111111111111111111111111111111111111112"
TEST_DB = "test_smoke.sqlite"
TEST_JSON = "test_smoke_out.json"
TEST_CSV = "test_smoke_out.csv"


def smoke_client_and_model() -> bool:
    """Call API for one pair, normalize to PairSnapshot."""
    print("Smoke: DexScreenerClient + from_api_pair ...")
    client = DexScreenerClient(timeout_sec=15, max_retries=2, rate_limit_rps=2)
    raw = client.get_pairs_by_pair_addresses([KNOWN_PAIR])
    if not raw:
        print("  FAIL: no pairs returned")
        return False
    pair_dict = raw[0]
    if "pairAddress" not in pair_dict or "baseToken" not in pair_dict:
        print("  FAIL: unexpected API response shape")
        return False
    ts = int(__import__("time").time() * 1000)
    snapshot = from_api_pair(pair_dict, ts)
    if not snapshot.pair_address or not isinstance(snapshot, PairSnapshot):
        print("  FAIL: PairSnapshot invalid")
        return False
    print("  OK: PairSnapshot created, pair_address=" + (snapshot.pair_address[:16] + "..."))
    return True


def smoke_db_and_collect() -> bool:
    """Collect one pair into SQLite, check tables."""
    print("Smoke: Database + collect one pair ...")
    for f in [TEST_DB, TEST_JSON, TEST_CSV]:
        if Path(f).exists():
            Path(f).unlink()

    db = Database(TEST_DB)
    client = DexScreenerClient(timeout_sec=15, max_retries=2, rate_limit_rps=2)
    collector = Collector(client, db)
    processed, errors = collector.collect_for_pairs([KNOWN_PAIR])
    db.close()

    conn = __import__("sqlite3").connect(TEST_DB)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    if not {"tokens", "pairs", "snapshots"}.issubset(tables):
        print("  FAIL: missing tables", tables)
        conn.close()
        return False
    cur.execute("SELECT COUNT(*) FROM pairs")
    n_pairs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM snapshots")
    n_snap = cur.fetchone()[0]
    conn.close()
    if n_pairs < 1 or n_snap < 1:
        print("  FAIL: pairs=%s snapshots=%s" % (n_pairs, n_snap))
        return False
    print("  OK: pairs=%s snapshots=%s" % (n_pairs, n_snap))
    return True


def smoke_prune_dry_run() -> bool:
    """Prune --dry-run on empty and filled DB."""
    print("Smoke: prune --dry-run ...")
    from dexscreener_screener.cli import cmd_prune

    # Test on empty DB
    empty_db = "test_prune_empty.sqlite"
    if Path(empty_db).exists():
        Path(empty_db).unlink()
    Database(empty_db).close()
    class EmptyArgs:
        db = empty_db
        max_age_hours = 24
        dry_run = True
        vacuum = False
    if cmd_prune(EmptyArgs()) != 0:
        print("  FAIL: prune --dry-run on empty DB returned non-zero")
        Path(empty_db).unlink(missing_ok=True)
        return False
    Path(empty_db).unlink(missing_ok=True)

    # Test on filled DB (from smoke_db_and_collect)
    if not Path(TEST_DB).exists():
        smoke_db_and_collect()
    class Args:
        db = TEST_DB
        max_age_hours = 24
        dry_run = True
        vacuum = False
    if cmd_prune(Args()) != 0:
        print("  FAIL: prune --dry-run on filled DB returned non-zero")
        return False
    print("  OK: prune --dry-run completed (empty + filled)")
    return True


def smoke_prune_real() -> bool:
    """Real prune_by_pair_age and verify no old pairs remain."""
    print("Smoke: prune_by_pair_age real + verify ...")
    import sqlite3

    if not Path(TEST_DB).exists():
        smoke_db_and_collect()

    # Insert an old pair (25 hours ago) with snapshot and tokens
    now_ms = int(__import__("time").time() * 1000)
    old_ms = now_ms - int(25 * 3600 * 1000)
    db = Database(TEST_DB)
    cur = db._conn.cursor()
    cur.execute("SELECT pair_address, base_address, quote_address FROM pairs LIMIT 1")
    row = cur.fetchone()
    old_pair = "OLD_PAIR_" + str(old_ms)
    old_base = "OLD_BASE_" + str(old_ms)[:20]
    old_quote = "OLD_QUOTE_" + str(old_ms)[:20]
    if row:
        old_base = row["base_address"]
        old_quote = row["quote_address"]
    cur.execute(
        "INSERT OR REPLACE INTO pairs (pair_address, pair_created_at_ms, base_address, quote_address) VALUES (?, ?, ?, ?)",
        (old_pair, old_ms, old_base, old_quote),
    )
    cur.execute(
        "INSERT INTO snapshots (pair_address, snapshot_ts, pair_created_at_ms) VALUES (?, ?, ?)",
        (old_pair, now_ms, old_ms),
    )
    cur.execute("INSERT OR IGNORE INTO tokens (address, chain_id, symbol, name) VALUES (?, 'solana', 'OLD', 'Old')", (old_base,))
    cur.execute("INSERT OR IGNORE INTO tokens (address, chain_id, symbol, name) VALUES (?, 'solana', 'OLD', 'Old')", (old_quote,))
    db._conn.commit()
    db.close()

    db = Database(TEST_DB)
    s_del, p_del, t_del = db.prune_by_pair_age(max_age_hours=24, dry_run=False, vacuum=False)
    db.close()

    conn = sqlite3.connect(TEST_DB)
    cur = conn.cursor()
    cutoff_ms = int((__import__("time").time() - 24 * 3600) * 1000)
    cur.execute(
        "SELECT COUNT(*) FROM pairs WHERE pair_created_at_ms < ? AND pair_created_at_ms IS NOT NULL AND pair_created_at_ms != 0",
        (cutoff_ms,),
    )
    old_pairs = cur.fetchone()[0]
    cur.execute(
        """
        SELECT COUNT(*) FROM tokens
        WHERE NOT EXISTS (
            SELECT 1 FROM pairs p
            WHERE p.base_address = tokens.address OR p.quote_address = tokens.address
        )
        """
    )
    orphaned_tokens = cur.fetchone()[0]
    conn.close()

    if old_pairs != 0:
        print("  FAIL: %s old pairs remaining after prune" % old_pairs)
        return False
    if orphaned_tokens != 0:
        print("  FAIL: %s orphaned tokens remaining" % orphaned_tokens)
        return False
    print("  OK: prune_by_pair_age verified (deleted s=%s p=%s t=%s)" % (s_del, p_del, t_del))
    return True


def smoke_export() -> bool:
    """Export to JSON and CSV, check files and content."""
    print("Smoke: export JSON and CSV ...")
    from dexscreener_screener.cli import cmd_export

    class Args:
        db = TEST_DB
        format = "json"
        out = TEST_JSON
        table = "snapshots"

    if cmd_export(Args()) != 0:
        print("  FAIL: export json failed")
        return False
    if not Path(TEST_JSON).exists():
        print("  FAIL: json file not created")
        return False
    with open(TEST_JSON, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or (data and "pair_address" not in data[0]):
        print("  FAIL: json content unexpected")
        return False

    Args.format = "csv"
    Args.out = TEST_CSV
    if cmd_export(Args()) != 0:
        print("  FAIL: export csv failed")
        return False
    if not Path(TEST_CSV).exists():
        print("  FAIL: csv file not created")
        return False
    with open(TEST_CSV, encoding="utf-8") as f:
        lines = f.readlines()
    if not lines or "pair_address" not in lines[0]:
        print("  FAIL: csv content unexpected")
        return False
    print("  OK: JSON and CSV exported")
    return True


def main() -> int:
    ok = True
    ok = smoke_client_and_model() and ok
    ok = smoke_db_and_collect() and ok
    ok = smoke_export() and ok
    ok = smoke_prune_dry_run() and ok
    ok = smoke_prune_real() and ok
    for f in [TEST_DB, TEST_JSON, TEST_CSV]:
        if Path(f).exists():
            try:
                Path(f).unlink()
            except Exception:
                pass
    if ok:
        print("All smoke tests passed.")
    else:
        print("Some smoke tests failed.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
