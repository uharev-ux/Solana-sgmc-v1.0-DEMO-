"""
Post-analyzer: evaluate signal quality at configured horizons (30/60/120 min).
Updates PENDING → DONE or NO_DATA based on snapshot data in strict [signal_ts, signal_ts + horizon].
Uses same ms/sec normalization as storage for snapshot_ts.
"""

from __future__ import annotations

import time
from pathlib import Path

from dexscreener_screener.storage import Database
from dexscreener_screener.storage.sqlite import normalize_since_ts


def run_post_analysis(db_path: str, now_ts: int | None = None) -> tuple[int, int]:
    """
    Process all PENDING evaluations where now_ts >= signal_ts + horizon_sec.
    Range strictly [signal_ts, signal_ts + horizon]. No snapshots in range -> NO_DATA.
    Single snapshot in range -> DONE with price_end=max=min=that point.
    Returns (done_count, no_data_count).
    """
    if not Path(db_path).exists():
        return 0, 0

    if now_ts is None:
        now_ts = int(time.time() * 1000)

    db = Database(db_path)
    done_cnt = 0
    no_data_cnt = 0

    try:
        # Normalize to same unit as snapshot_ts (ms or sec) for range query
        snapshot_ts_is_ms = db._detect_snapshot_ts_unit()

        for ev in db.iter_pending_evaluations(now_ts):
            eval_id = int(ev["eval_id"])
            pair_address = str(ev["pair_address"])
            signal_ts = int(ev["signal_ts"])
            entry_price = float(ev["entry_price"])
            horizon_sec = int(ev["horizon_sec"])

            ts_is_ms = signal_ts > 10**12
            horizon_unit = horizon_sec * 1000 if ts_is_ms else horizon_sec
            until_ts_raw = signal_ts + horizon_unit
            # Strict window [signal_ts, signal_ts + horizon]; normalize to snapshot_ts unit
            since_ts = normalize_since_ts(signal_ts, snapshot_ts_is_ms)
            until_ts = normalize_since_ts(until_ts_raw, snapshot_ts_is_ms)

            snapshots = list(
                db.iterate_snapshots(
                    pair_address=pair_address,
                    since_ts=since_ts,
                    until_ts=until_ts,
                )
            )

            prices = []
            for row in snapshots:
                p = row.get("price_usd")
                if p is not None and float(p) > 0:
                    prices.append(float(p))

            if not prices:
                db.update_evaluation_no_data(eval_id)
                no_data_cnt += 1
                continue

            # One point or more: price_end = last, max = max, min = min (single point -> all equal)
            price_end = prices[-1]
            max_price = max(prices)
            min_price = min(prices)

            if entry_price <= 0:
                db.update_evaluation_no_data(eval_id)
                no_data_cnt += 1
                continue

            return_end_pct = (price_end - entry_price) / entry_price * 100.0
            max_return_pct = (max_price - entry_price) / entry_price * 100.0
            min_return_pct = (min_price - entry_price) / entry_price * 100.0

            db.update_evaluation_done(
                eval_id=eval_id,
                evaluated_at=now_ts,
                price_end=price_end,
                max_price=max_price,
                min_price=min_price,
                return_end_pct=return_end_pct,
                max_return_pct=max_return_pct,
                min_return_pct=min_return_pct,
            )
            done_cnt += 1
    finally:
        db.close()

    return done_cnt, no_data_cnt
