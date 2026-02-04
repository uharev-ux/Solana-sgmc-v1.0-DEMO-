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
from dexscreener_screener.db import Database
from dexscreener_screener.models import PairSnapshot, from_api_pair

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
    from dexscreener_screener.collector import Collector

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
