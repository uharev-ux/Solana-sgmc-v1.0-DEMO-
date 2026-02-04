"""
Full automated verification of collect-new: CLI, one cycle in-process, dedup, then CLI subprocess.
Run from project root: python scripts/verify_collect_new.py
No pytest required.
"""

import subprocess
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB = "test_collect_new_verify.sqlite"


def step1_cli_help() -> bool:
    """Verify collect-new subcommand exists and has expected args."""
    print("Step 1: CLI help (collect-new subcommand)...")
    r = subprocess.run(
        [sys.executable, "-m", "dexscreener_screener.cli", "collect-new", "--help"],
        cwd=Path(__file__).resolve().parent.parent,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if r.returncode != 0:
        print("  FAIL: collect-new --help returned", r.returncode)
        return False
    out = r.stdout + r.stderr
    if "collect-new" not in out or "interval-sec" not in out or "limit-per-cycle" not in out:
        print("  FAIL: expected args not in help")
        return False
    print("  OK: collect-new --help OK")
    return True


def step2_one_cycle_in_process() -> bool:
    """Run one collect-new cycle in-process: token-profiles -> pairs -> dedup -> persist."""
    print("Step 2: One cycle in-process (token-profiles -> pairs -> persist)...")
    from dexscreener_screener.client import DexScreenerClient
    from dexscreener_screener.collector import Collector
    from dexscreener_screener.db import Database

    if Path(TEST_DB).exists():
        Path(TEST_DB).unlink()
    db = Database(TEST_DB)
    client = DexScreenerClient(timeout_sec=15, max_retries=3, rate_limit_rps=2)
    collector = Collector(client, db)

    token_addresses = client.get_latest_token_profiles()
    if not token_addresses:
        print("  FAIL: get_latest_token_profiles returned no addresses")
        db.close()
        return False
    token_addresses = token_addresses[:5]
    raw_pairs = client.get_pairs_by_token_addresses_batched(token_addresses)
    known = db.get_known_pair_addresses()
    processed, errors, skipped = collector.collect_from_raw_pairs(raw_pairs, known)
    db.close()

    if processed < 0 or errors < 0 or skipped < 0:
        print("  FAIL: invalid counts processed=%s errors=%s skipped=%s" % (processed, errors, skipped))
        return False
    if not raw_pairs and processed != 0:
        print("  FAIL: no raw pairs but processed=%s" % processed)
        return False
    print("  OK: tokens=%s raw_pairs=%s processed=%s errors=%s skipped=%s" % (
        len(token_addresses), len(raw_pairs), processed, errors, skipped))
    return True


def step3_db_grows() -> bool:
    """Check DB has pairs and snapshots."""
    print("Step 3: DB content (pairs + snapshots)...")
    import sqlite3
    conn = sqlite3.connect(TEST_DB)
    cur = conn.cursor()
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


def step4_dedup_second_run() -> bool:
    """Run cycle again: same pairs must be skipped (dedup)."""
    print("Step 4: Dedup (second run, same pairs skipped)...")
    from dexscreener_screener.client import DexScreenerClient
    from dexscreener_screener.collector import Collector
    from dexscreener_screener.db import Database

    db = Database(TEST_DB)
    client = DexScreenerClient(timeout_sec=15, max_retries=3, rate_limit_rps=2)
    collector = Collector(client, db)
    token_addresses = client.get_latest_token_profiles()[:5]
    raw_pairs = client.get_pairs_by_token_addresses_batched(token_addresses)
    known = db.get_known_pair_addresses()
    processed, errors, skipped = collector.collect_from_raw_pairs(raw_pairs, known)
    db.close()

    if len(raw_pairs) > 0 and skipped == 0:
        print("  FAIL: expected skipped > 0 on second run, got skipped=%s" % skipped)
        return False
    print("  OK: second run processed=%s skipped=%s (dedup works)" % (processed, skipped))
    return True


def step5_cli_subprocess_cycles() -> bool:
    """Run collect-new via CLI for a few seconds, check stdout for cycle and summary."""
    print("Step 5: CLI collect-new (subprocess, ~10s, 2–3 cycles)...")
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "dexscreener_screener.cli", "collect-new",
            "--db", TEST_DB,
            "--interval-sec", "3",
            "--limit-per-cycle", "3",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        out_lines = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and proc.poll() is None:
            line = proc.stdout.readline()
            if line:
                out_lines.append(line)
            time.sleep(0.2)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        out = "".join(out_lines)
    except Exception as e:
        print("  FAIL: subprocess error:", e)
        if proc.poll() is None:
            proc.kill()
        return False

    if "collect-new cycle" not in out:
        print("  FAIL: no 'collect-new cycle' in output")
        return False
    if "candidates_tokens=" not in out or "processed=" not in out:
        print("  FAIL: no cycle summary in output")
        return False
    print("  OK: CLI ran, cycle and summary in log")
    return True


def main() -> int:
    ok = True
    ok = step1_cli_help() and ok
    ok = step2_one_cycle_in_process() and ok
    ok = step3_db_grows() and ok
    ok = step4_dedup_second_run() and ok
    ok = step5_cli_subprocess_cycles() and ok

    if Path(TEST_DB).exists():
        try:
            Path(TEST_DB).unlink()
        except Exception:
            pass

    if ok:
        print("All collect-new checks passed.")
    else:
        print("Some checks failed.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
