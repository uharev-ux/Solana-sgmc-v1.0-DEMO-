"""
Strategy self-check: True-ATH diagnostics.
Verifies snapshots counts, time units, current_price vs last_snapshot, ath_price, drop_from_ath.
Shows raw ATH vs valid ATH, activity around ATH, fallback flag.
Exit 0 = OK, 1 = critical issues (empty snapshots or time-unit mismatch).
Run from project root: python scripts/strategy_selfcheck.py [--db path]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dexscreener_screener import config
from dexscreener_screener.storage import Database
from dexscreener_screener.strategy.engine import _find_valid_ath


def _info(msg: str, *args: object) -> None:
    print("[strategy_selfcheck] " + (msg % args if args else msg))


def main() -> int:
    import argparse
    p = argparse.ArgumentParser(description="Strategy True-ATH self-check")
    p.add_argument("--db", default=config.DEFAULT_DB, help="SQLite DB path")
    args = p.parse_args()
    db_path = args.db

    if not Path(db_path).exists():
        _info("FAIL: database not found: %s", db_path)
        return 1

    db = Database(db_path)
    cur = db._conn.cursor()

    # --- Counts ---
    total_snapshots = cur.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    pairs_count = cur.execute("SELECT COUNT(*) FROM pairs").fetchone()[0]

    row = cur.execute("SELECT MIN(snapshot_ts) AS mn, MAX(snapshot_ts) AS mx FROM snapshots").fetchone()
    snapshots_ts_min = row["mn"] if row and row["mn"] is not None else None
    snapshots_ts_max = row["mx"] if row and row["mx"] is not None else None

    _info("total_snapshots=%s", total_snapshots)
    _info("snapshots_ts_min=%s snapshots_ts_max=%s", snapshots_ts_min, snapshots_ts_max)
    _info("pairs_count=%s", pairs_count)

    anomalies: list[str] = []
    critical = False

    if total_snapshots == 0:
        anomalies.append("snapshots empty")
        critical = True

    if total_snapshots < 3 and pairs_count > 0:
        anomalies.append("too few snapshots (total_snapshots=%s)" % total_snapshots)

    # --- Time unit: snapshot_ts vs pair_created_at_ms ---
    snapshot_ts_looks_ms = snapshots_ts_max is not None and snapshots_ts_max > 10**12
    cur.execute("SELECT pair_address, pair_created_at_ms FROM pairs WHERE pair_created_at_ms IS NOT NULL AND pair_created_at_ms > 0 LIMIT 1")
    sample_pair_row = cur.fetchone()
    created_looks_ms = False
    if sample_pair_row and sample_pair_row["pair_created_at_ms"]:
        created_looks_ms = sample_pair_row["pair_created_at_ms"] > 10**12
    if snapshot_ts_looks_ms != created_looks_ms and total_snapshots > 0:
        anomalies.append(
            "time-unit mismatch: snapshot_ts looks %s, pair_created_at_ms looks %s"
            % ("ms" if snapshot_ts_looks_ms else "sec", "ms" if created_looks_ms else "sec")
        )
        critical = True

    # --- Sample pairs (top 5 by snapshot count) ---
    cur.execute(
        """
        SELECT pair_address, COUNT(*) AS cnt, MIN(snapshot_ts) AS ts_min, MAX(snapshot_ts) AS ts_max
        FROM snapshots
        GROUP BY pair_address
        ORDER BY cnt DESC
        LIMIT 5
        """
    )
    sample_pairs = [dict(r) for r in cur.fetchall()]
    _info("sample_pairs (top 5 by snapshot count):")
    for r in sample_pairs:
        _info("  %s snapshots_count=%s ts_min=%s ts_max=%s", r["pair_address"][:44], r["cnt"], r["ts_min"], r["ts_max"])

    # --- For 3 sample pairs: raw ATH vs valid ATH, activity around ATH, fallback flag ---
    for i, row in enumerate(sample_pairs[:3]):
        pair_address = row["pair_address"]
        current_price = db.fetch_latest_price(pair_address)
        last_snap = cur.execute(
            "SELECT price_usd FROM snapshots WHERE pair_address = ? AND price_usd IS NOT NULL AND price_usd > 0 ORDER BY snapshot_ts DESC LIMIT 1",
            (pair_address,),
        ).fetchone()
        last_snapshot_price = float(last_snap["price_usd"]) if last_snap and last_snap["price_usd"] is not None else None
        pair_row = cur.execute("SELECT pair_created_at_ms FROM pairs WHERE pair_address = ?", (pair_address,)).fetchone()
        since_ts = int(pair_row["pair_created_at_ms"]) if pair_row and pair_row["pair_created_at_ms"] else None

        raw_ath_point = db.fetch_ath_point(pair_address, since_ts=since_ts)
        raw_ath_price = raw_ath_point["ath_price"] if raw_ath_point else None
        raw_ath_ts = raw_ath_point["ath_ts"] if raw_ath_point else None
        activity_around_ath = None
        if raw_ath_ts is not None:
            activity_around_ath = db.fetch_activity_window(
                pair_address, raw_ath_ts, config.ATH_VALIDATE_WINDOW_SEC
            )

        valid_ath_result = _find_valid_ath(db, pair_address, since_ts, current_price)
        valid_ath_price = valid_ath_result[0] if valid_ath_result else None
        ath_source = valid_ath_result[3] if valid_ath_result else None  # raw / fallback
        ath_validation_metrics = valid_ath_result[2] if valid_ath_result else None

        if valid_ath_price is not None and current_price is not None and current_price > 0 and valid_ath_price > 0:
            drop_from_ath = (valid_ath_price - current_price) / valid_ath_price * 100.0
        else:
            drop_from_ath = None

        _info("  pair[%s] current_price=%s last_snapshot_price=%s", i, current_price, last_snapshot_price)
        _info("    raw_ath_price=%s raw_ath_ts=%s", raw_ath_price, raw_ath_ts)
        _info("    activity_around_ath=%s", activity_around_ath)
        _info("    valid_ath_price=%s ath_source=%s drop_from_ath=%s", valid_ath_price, ath_source, drop_from_ath)
        if ath_source == "fallback":
            _info("    [FALLBACK] valid ATH is from fallback (raw ATH rejected)")

        ath_price = valid_ath_price  # for anomaly checks below
        if ath_price is None and row["cnt"] > 0:
            anomalies.append("pair %s: ath_price is NULL but has snapshots" % pair_address[:20])
        if current_price is not None and last_snapshot_price is not None and abs(current_price - last_snapshot_price) > 1e-6:
            rel = abs(current_price - last_snapshot_price) / last_snapshot_price if last_snapshot_price else 0
            if rel > 0.01:
                anomalies.append(
                    "pair %s: current_price (%.6g) differs from last_snapshot_price (%.6g)" % (pair_address[:20], current_price, last_snapshot_price)
                )
        if drop_from_ath is not None:
            if drop_from_ath < 0:
                anomalies.append("pair %s: drop_from_ath < 0 (%.2f)" % (pair_address[:20], drop_from_ath))
            if drop_from_ath > 99.9:
                anomalies.append("pair %s: drop_from_ath > 99.9 (%.2f)" % (pair_address[:20], drop_from_ath))

    db.close()

    if anomalies:
        _info("anomalies: %s", anomalies)
    if critical:
        _info("RESULT: FAIL (critical)")
        return 1
    if anomalies:
        _info("RESULT: OK with warnings")
    else:
        _info("RESULT: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
