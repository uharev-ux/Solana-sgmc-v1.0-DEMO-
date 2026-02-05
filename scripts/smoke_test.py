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
from dexscreener_screener.strategy import run_strategy_once

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


def smoke_bootstrap() -> bool:
    """Pair with 1 snapshot -> expect WATCHLIST_BOOTSTRAP, not REJECT."""
    print("Smoke: bootstrap (1 snapshot -> WATCHLIST_BOOTSTRAP) ...")
    bootstrap_db = "test_bootstrap.sqlite"
    if Path(bootstrap_db).exists():
        Path(bootstrap_db).unlink()
    db = Database(bootstrap_db)
    db.init_schema()
    cur = db._conn.cursor()
    now_ms = int(__import__("time").time() * 1000)
    pair_addr = "BOOTSTRAP_PAIR_1"
    price = 1.5
    liq = 15_000.0
    vol = 600.0
    cur.execute(
        """
        INSERT INTO pairs (pair_address, pair_created_at_ms, liquidity_usd, volume_h24, txns_h24_buys, txns_h24_sells, base_address, quote_address)
        VALUES (?, ?, ?, ?, ?, ?, 'base', 'quote')
        """,
        (pair_addr, now_ms - 3600_000, liq, vol, 3, 2),
    )
    cur.execute(
        "INSERT INTO snapshots (pair_address, snapshot_ts, price_usd, liquidity_usd, volume_h24, pair_created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
        (pair_addr, now_ms, price, liq, vol, now_ms - 3600_000),
    )
    db._conn.commit()
    db.ensure_strategy_schema()
    signals, wl_bootstrap, _wl3, _wl2, _wl1 = run_strategy_once(db)
    cur.execute("SELECT decision, reasons_json FROM strategy_decisions WHERE pair_address = ? ORDER BY decided_at DESC LIMIT 1", (pair_addr,))
    row = cur.fetchone()
    db.close()
    if Path(bootstrap_db).exists():
        Path(bootstrap_db).unlink(missing_ok=True)
    if not row:
        print("  FAIL: no strategy decision for bootstrap pair")
        return False
    if row["decision"] != "WATCHLIST_BOOTSTRAP":
        print("  FAIL: expected WATCHLIST_BOOTSTRAP, got %s" % row["decision"])
        return False
    reasons = json.loads(row["reasons_json"] or "{}")
    if reasons.get("reason") != "insufficient_price_history":
        print("  FAIL: expected reason insufficient_price_history, got %s" % reasons.get("reason"))
        return False
    print("  OK: 1 snapshot -> WATCHLIST_BOOTSTRAP, reason=insufficient_price_history")
    return True


def smoke_post_analysis() -> bool:
    """Create signal_event + PENDING evaluation, run post-analysis, verify DONE or NO_DATA."""
    print("Smoke: post-analysis (signal_events, signal_evaluations) ...")
    if not Path(TEST_DB).exists():
        smoke_db_and_collect()

    import sqlite3

    db = Database(TEST_DB)
    cur = db._conn.cursor()
    cur.execute("SELECT pair_address, price_usd, snapshot_ts FROM snapshots WHERE price_usd IS NOT NULL AND price_usd > 0 LIMIT 1")
    row = cur.fetchone()
    if not row:
        print("  SKIP: no snapshots with valid price")
        db.close()
        return True

    pair_address = row["pair_address"]
    entry_price = float(row["price_usd"])
    signal_ts = int(row["snapshot_ts"])
    db.ensure_strategy_schema()
    signal_id = db.insert_signal_event(
        pair_address=pair_address,
        signal_ts=signal_ts,
        entry_price=entry_price,
        ath_price=entry_price * 1.5,
        drop_from_ath=33.33,
        score=33.33,
        features_json="{}",
    )
    db.insert_signal_evaluation(signal_id=signal_id, horizon_sec=1, status="PENDING")
    db.close()

    from dexscreener_screener.strategy.post_analyzer import run_post_analysis

    done_cnt, no_data_cnt = run_post_analysis(TEST_DB, now_ts=signal_ts + 2000)
    if done_cnt + no_data_cnt < 1:
        print("  FAIL: post-analysis did not process evaluation")
        return False

    conn = sqlite3.connect(TEST_DB)
    cur = conn.cursor()
    cur.execute("SELECT status FROM signal_evaluations WHERE signal_id = ?", (signal_id,))
    ev = cur.fetchone()
    conn.close()
    if not ev or ev[0] not in ("DONE", "NO_DATA"):
        print("  FAIL: evaluation status not DONE/NO_DATA: %s" % (ev[0] if ev else None))
        return False

    print("  OK: post-analysis processed evaluation (done=%s no_data=%s)" % (done_cnt, no_data_cnt))
    return True


def smoke_post_analysis_one_point() -> bool:
    """Evaluation with exactly 1 snapshot in window -> DONE with max=min=end."""
    print("Smoke: post-analysis 1 point in window -> DONE (max=min=end) ...")
    one_pt_db = "test_post_one_pt.sqlite"
    if Path(one_pt_db).exists():
        Path(one_pt_db).unlink()
    db = Database(one_pt_db)
    db.init_schema()
    cur = db._conn.cursor()
    now_ms = int(__import__("time").time() * 1000)
    pair_addr = "ONEPT_PAIR"
    entry_price = 2.0
    signal_ts = now_ms - 7200_000  # 2h ago
    horizon_sec = 3600
    # Single snapshot at signal_ts (start of window); window [signal_ts, signal_ts+3600*1000]
    cur.execute(
        "INSERT INTO pairs (pair_address, pair_created_at_ms, base_address, quote_address) VALUES (?, ?, 'b', 'q')",
        (pair_addr, signal_ts - 1000),
    )
    cur.execute(
        "INSERT INTO snapshots (pair_address, snapshot_ts, price_usd, pair_created_at_ms) VALUES (?, ?, ?, ?)",
        (pair_addr, signal_ts, entry_price, signal_ts - 1000),
    )
    db._conn.commit()
    db.ensure_strategy_schema()
    signal_id = db.insert_signal_event(
        pair_address=pair_addr,
        signal_ts=signal_ts,
        entry_price=entry_price,
        ath_price=entry_price * 1.2,
        drop_from_ath=16.67,
        score=16.67,
        features_json="{}",
    )
    db.insert_signal_evaluation(signal_id=signal_id, horizon_sec=horizon_sec, status="PENDING")
    db.close()
    from dexscreener_screener.strategy.post_analyzer import run_post_analysis
    until_ts = signal_ts + horizon_sec * 1000
    done_cnt, no_data_cnt = run_post_analysis(one_pt_db, now_ts=until_ts)
    conn = __import__("sqlite3").connect(one_pt_db)
    cur = conn.cursor()
    cur.execute(
        "SELECT status, price_end, max_price, min_price FROM signal_evaluations WHERE signal_id = ?",
        (signal_id,),
    )
    ev = cur.fetchone()
    conn.close()
    if Path(one_pt_db).exists():
        Path(one_pt_db).unlink(missing_ok=True)
    if not ev or ev[0] != "DONE":
        print("  FAIL: expected status DONE, got %s" % (ev[0] if ev else None))
        return False
    p_end, p_max, p_min = ev[1], ev[2], ev[3]
    if p_end is None or p_max is None or p_min is None:
        print("  FAIL: price_end/max/min should be set")
        return False
    if abs(p_end - p_max) > 1e-9 or abs(p_max - p_min) > 1e-9:
        print("  FAIL: expected price_end=max=min (one point), got end=%s max=%s min=%s" % (p_end, p_max, p_min))
        return False
    print("  OK: 1 point in window -> DONE, price_end=max=min")
    return True


def smoke_trigger_tp1_first_bu() -> bool:
    """CASE 1: entry=100, snapshots 100(t0), 120, 140, 100, 200 -> TP1_FIRST, tp1 at 140, bu_hit_after_tp1=1, post_tp1_max_pct=100."""
    print("Smoke: trigger CASE 1 (TP1_FIRST + BU hit) ...")
    trigger_db = "test_trigger_tp1.sqlite"
    if Path(trigger_db).exists():
        Path(trigger_db).unlink()
    db = Database(trigger_db)
    db.init_schema()
    db.ensure_strategy_schema()
    cur = db._conn.cursor()
    t0 = 1000000000000  # ms
    pair_addr = "TRIGGER_PAIR_1"
    entry_price = 100.0
    cur.execute(
        "INSERT INTO pairs (pair_address, pair_created_at_ms, base_address, quote_address) VALUES (?, ?, 'b', 'q')",
        (pair_addr, t0 - 3600000),
    )
    # Snapshots: (t0, 100), (t0+1, 120), (t0+2, 140), (t0+3, 100), (t0+4, 200)
    for i, (ts, price) in enumerate([(t0, 100), (t0 + 1, 120), (t0 + 2, 140), (t0 + 3, 100), (t0 + 4, 200)]):
        cur.execute(
            "INSERT INTO snapshots (pair_address, snapshot_ts, price_usd) VALUES (?, ?, ?)",
            (pair_addr, ts, price),
        )
    db._conn.commit()
    signal_id = db.insert_signal_event(
        pair_address=pair_addr,
        signal_ts=t0,
        entry_price=entry_price,
        ath_price=150.0,
        drop_from_ath=33.33,
        score=50.0,
        features_json="{}",
    )
    db.insert_trigger_eval_pending(signal_id)
    db.close()

    from dexscreener_screener.strategy.trigger_analyzer import run_trigger_analysis
    run_trigger_analysis(trigger_db, now_ts=t0 + 10000, limit=100)

    conn = __import__("sqlite3").connect(trigger_db)
    cur = conn.cursor()
    cur.execute(
        "SELECT outcome, tp1_hit_ts, bu_hit_after_tp1, post_tp1_max_pct FROM signal_trigger_evaluations WHERE signal_id = ?",
        (signal_id,),
    )
    row = cur.fetchone()
    conn.close()
    if Path(trigger_db).exists():
        Path(trigger_db).unlink(missing_ok=True)
    if not row:
        print("  FAIL: no trigger eval row")
        return False
    outcome, tp1_hit_ts, bu_hit, post_tp1_max = row[0], row[1], row[2], row[3]
    if outcome != "TP1_FIRST":
        print("  FAIL: expected outcome TP1_FIRST, got %s" % outcome)
        return False
    # tp1 first hit at price 140 -> ts = t0+2
    if tp1_hit_ts != t0 + 2:
        print("  FAIL: expected tp1_hit_ts=%s (140 at t0+2), got %s" % (t0 + 2, tp1_hit_ts))
        return False
    if bu_hit != 1:
        print("  FAIL: expected bu_hit_after_tp1=1, got %s" % bu_hit)
        return False
    if post_tp1_max is None or abs(float(post_tp1_max) - 100.0) > 0.01:
        print("  FAIL: expected post_tp1_max_pct=100, got %s" % post_tp1_max)
        return False
    print("  OK: TP1_FIRST, tp1_hit_ts at 140, bu_hit_after_tp1=1, post_tp1_max_pct=100")
    return True


def smoke_trigger_sl_first() -> bool:
    """CASE 2: entry=100, snapshots 100, 70, 49 -> SL_FIRST."""
    print("Smoke: trigger CASE 2 (SL_FIRST) ...")
    trigger_db = "test_trigger_sl.sqlite"
    if Path(trigger_db).exists():
        Path(trigger_db).unlink()
    db = Database(trigger_db)
    db.init_schema()
    db.ensure_strategy_schema()
    cur = db._conn.cursor()
    t0 = 2000000000000
    pair_addr = "TRIGGER_PAIR_2"
    entry_price = 100.0
    cur.execute(
        "INSERT INTO pairs (pair_address, pair_created_at_ms, base_address, quote_address) VALUES (?, ?, 'b', 'q')",
        (pair_addr, t0 - 3600000),
    )
    for ts, price in [(t0, 100), (t0 + 1, 70), (t0 + 2, 49)]:
        cur.execute(
            "INSERT INTO snapshots (pair_address, snapshot_ts, price_usd) VALUES (?, ?, ?)",
            (pair_addr, ts, price),
        )
    db._conn.commit()
    signal_id = db.insert_signal_event(
        pair_address=pair_addr,
        signal_ts=t0,
        entry_price=entry_price,
        ath_price=120.0,
        drop_from_ath=25.0,
        score=25.0,
        features_json="{}",
    )
    db.insert_trigger_eval_pending(signal_id)
    db.close()

    from dexscreener_screener.strategy.trigger_analyzer import run_trigger_analysis
    run_trigger_analysis(trigger_db, now_ts=t0 + 10000, limit=100)

    conn = __import__("sqlite3").connect(trigger_db)
    cur = conn.cursor()
    cur.execute(
        "SELECT outcome FROM signal_trigger_evaluations WHERE signal_id = ?",
        (signal_id,),
    )
    row = cur.fetchone()
    conn.close()
    if Path(trigger_db).exists():
        Path(trigger_db).unlink(missing_ok=True)
    if not row:
        print("  FAIL: no trigger eval row")
        return False
    if row[0] != "SL_FIRST":
        print("  FAIL: expected outcome SL_FIRST, got %s" % row[0])
        return False
    print("  OK: SL_FIRST")
    return True


def smoke_strategy_selfcheck() -> bool:
    """Run strategy self-check on the smoke test DB (before cleanup)."""
    print("Smoke: strategy_selfcheck on test DB ...")
    if not Path(TEST_DB).exists():
        print("  SKIP: test DB not found (run smoke_db_and_collect first)")
        return True
    import subprocess
    root = Path(__file__).resolve().parent.parent
    script = root / "scripts" / "strategy_selfcheck.py"
    result = subprocess.run(
        [sys.executable, str(script), "--db", str(root / TEST_DB)],
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        print("  FAIL: strategy_selfcheck exit code %s" % result.returncode)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        return False
    print("  OK: strategy_selfcheck passed")
    return True


def main() -> int:
    ok = True
    ok = smoke_client_and_model() and ok
    ok = smoke_db_and_collect() and ok
    ok = smoke_export() and ok
    ok = smoke_bootstrap() and ok
    ok = smoke_post_analysis() and ok
    ok = smoke_post_analysis_one_point() and ok
    ok = smoke_trigger_tp1_first_bu() and ok
    ok = smoke_trigger_sl_first() and ok
    ok = smoke_strategy_selfcheck() and ok  # run while DB has snapshots (before prune)
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
