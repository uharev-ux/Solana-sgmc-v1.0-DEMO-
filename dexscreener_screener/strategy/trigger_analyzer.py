"""
Trigger-based post-analysis: TP1 (+40%), SL (-50%), BU after TP1.
Uses only snapshots (pair_address, price_usd, snapshot_ts). No %change.
"""

from __future__ import annotations

import time
from pathlib import Path

from dexscreener_screener import config
from dexscreener_screener.storage import Database
from dexscreener_screener.storage.sqlite import normalize_since_ts


def run_trigger_analysis(
    db_path: str,
    now_ts: int | None = None,
    limit: int = 100,
) -> dict:
    """
    Process PENDING signal_trigger_evaluations: load snapshots, compute outcome (TP1_FIRST/SL_FIRST/NEITHER),
    mfe/mae, and if TP1_FIRST then bu_hit_after_tp1 and post_tp1_max_pct.
    Returns summary dict for reporting.
    """
    if not Path(db_path).exists():
        return _empty_summary()

    if now_ts is None:
        now_ts = int(time.time() * 1000)

    db = Database(db_path)
    try:
        snapshot_ts_is_ms = db._detect_snapshot_ts_unit()
        processed = 0
        no_data = 0

        for ev in db.iter_pending_trigger_evals(limit=limit):
            signal_id = int(ev["signal_id"])
            pair_address = str(ev["pair_address"])
            signal_ts = int(ev["signal_ts"])
            entry_price = float(ev["entry_price"])

            if entry_price <= 0:
                db.update_trigger_eval_no_data(signal_id, reason="invalid_entry_price")
                no_data += 1
                continue

            # Range [signal_ts, signal_ts + TRIGGER_EVAL_MAX_AGE_SEC] in snapshot_ts unit
            since_ts = normalize_since_ts(signal_ts, snapshot_ts_is_ms)
            if snapshot_ts_is_ms:
                until_ts = signal_ts + config.TRIGGER_EVAL_MAX_AGE_SEC * 1000
            else:
                until_ts = (signal_ts // 1000 if signal_ts > 10**12 else signal_ts) + config.TRIGGER_EVAL_MAX_AGE_SEC

            snapshots = list(
                db.iterate_snapshots(
                    pair_address=pair_address,
                    since_ts=since_ts,
                    until_ts=until_ts,
                )
            )

            # Only price_usd and snapshot_ts
            points = []
            for row in snapshots:
                p = row.get("price_usd")
                ts = row.get("snapshot_ts")
                if p is not None and ts is not None and float(p) > 0:
                    points.append((int(ts), float(p)))

            points.sort(key=lambda x: x[0])

            if len(points) < config.TRIGGER_EVAL_MIN_SNAPSHOTS:
                db.update_trigger_eval_no_data(signal_id, reason="insufficient_snapshots")
                no_data += 1
                continue

            tp1_pct = config.TP1_PCT
            sl_pct = config.SL_PCT
            tp1_hit_ts = None
            sl_hit_ts = None
            tp1_price = None
            sl_price = None
            mfe_pct = None
            mae_pct = None
            max_price = None
            min_price = None

            pcts = []
            for ts, price in points:
                pct = (price - entry_price) / entry_price * 100.0
                pcts.append((ts, price, pct))
                if tp1_hit_ts is None and pct >= tp1_pct:
                    tp1_hit_ts = ts
                    tp1_price = price
                if sl_hit_ts is None and pct <= sl_pct:
                    sl_hit_ts = ts
                    sl_price = price

            all_pcts = [p[2] for p in pcts]
            mfe_pct = max(all_pcts) if all_pcts else None
            mae_pct = min(all_pcts) if all_pcts else None
            max_price = max(p[1] for p in pcts) if pcts else None
            min_price = min(p[1] for p in pcts) if pcts else None

            if tp1_hit_ts is not None and (sl_hit_ts is None or tp1_hit_ts < sl_hit_ts):
                outcome = "TP1_FIRST"
            elif sl_hit_ts is not None and (tp1_hit_ts is None or sl_hit_ts < tp1_hit_ts):
                outcome = "SL_FIRST"
            else:
                outcome = "NEITHER"

            bu_hit_after_tp1 = None
            post_tp1_max_pct = None
            post_tp1_max_price = None

            if outcome == "TP1_FIRST" and tp1_hit_ts is not None:
                after_tp1 = [(t, pr, pc) for t, pr, pc in pcts if t >= tp1_hit_ts]
                if after_tp1:
                    bu_hit_after_tp1 = 1 if any(pr <= entry_price for _, pr, _ in after_tp1) else 0
                    post_tp1_max_pct = max(pc for _, _, pc in after_tp1)
                    post_tp1_max_price = max(pr for _, pr, _ in after_tp1)
                else:
                    bu_hit_after_tp1 = 0
                    post_tp1_max_pct = (tp1_price - entry_price) / entry_price * 100.0 if tp1_price else None
                    post_tp1_max_price = tp1_price

            db.update_trigger_eval_done(
                signal_id=signal_id,
                evaluated_at=now_ts,
                outcome=outcome,
                tp1_hit_ts=tp1_hit_ts,
                sl_hit_ts=sl_hit_ts,
                tp1_price=tp1_price,
                sl_price=sl_price,
                mfe_pct=mfe_pct,
                mae_pct=mae_pct,
                max_price=max_price,
                min_price=min_price,
                bu_hit_after_tp1=bu_hit_after_tp1,
                post_tp1_max_pct=post_tp1_max_pct,
                post_tp1_max_price=post_tp1_max_price,
            )
            processed += 1

        return _build_summary(db)
    finally:
        db.close()


def _empty_summary() -> dict:
    return {
        "total_signals": 0,
        "trigger_done": 0,
        "trigger_no_data": 0,
        "trigger_pending": 0,
        "outcome_tp1_first": 0,
        "outcome_sl_first": 0,
        "outcome_neither": 0,
        "tp1_hit_rate": 0.0,
        "sl_first_rate": 0.0,
        "bu_after_tp1_rate": 0.0,
        "post_tp1_max_pct_avg": None,
        "post_tp1_max_pct_median": None,
        "top10_post_tp1": [],
    }


def _build_summary(db: Database) -> dict:
    cur = db._conn.cursor()
    cur.execute("SELECT COUNT(*) FROM signal_events")
    total_signals = cur.fetchone()[0] or 0

    cur.execute(
        "SELECT status, COUNT(*) FROM signal_trigger_evaluations GROUP BY status"
    )
    by_status = dict(cur.fetchall())
    trigger_done = int(by_status.get("DONE", 0))
    trigger_no_data = int(by_status.get("NO_DATA", 0))
    trigger_pending = int(by_status.get("PENDING", 0))

    cur.execute(
        "SELECT outcome, COUNT(*) FROM signal_trigger_evaluations WHERE status = 'DONE' GROUP BY outcome"
    )
    by_outcome = dict(cur.fetchall())
    outcome_tp1_first = int(by_outcome.get("TP1_FIRST", 0))
    outcome_sl_first = int(by_outcome.get("SL_FIRST", 0))
    outcome_neither = int(by_outcome.get("NEITHER", 0))

    tp1_hit_rate = (outcome_tp1_first / trigger_done) if trigger_done else 0.0
    sl_first_rate = (outcome_sl_first / trigger_done) if trigger_done else 0.0

    cur.execute(
        """
        SELECT bu_hit_after_tp1 FROM signal_trigger_evaluations
        WHERE status = 'DONE' AND outcome = 'TP1_FIRST' AND bu_hit_after_tp1 IS NOT NULL
        """
    )
    bu_rows = cur.fetchall()
    bu_hits = sum(1 for r in bu_rows if r[0] == 1)
    bu_after_tp1_rate = (bu_hits / outcome_tp1_first) if outcome_tp1_first else 0.0

    cur.execute(
        """
        SELECT post_tp1_max_pct FROM signal_trigger_evaluations
        WHERE status = 'DONE' AND outcome = 'TP1_FIRST' AND post_tp1_max_pct IS NOT NULL
        """
    )
    pct_rows = [float(r[0]) for r in cur.fetchall()]
    if pct_rows:
        post_tp1_max_pct_avg = sum(pct_rows) / len(pct_rows)
        pct_rows_sorted = sorted(pct_rows)
        mid = len(pct_rows_sorted) // 2
        post_tp1_max_pct_median = (
            (pct_rows_sorted[mid] + pct_rows_sorted[mid - 1]) / 2.0
            if mid > 0 and len(pct_rows_sorted) % 2 == 0
            else pct_rows_sorted[mid]
        )
    else:
        post_tp1_max_pct_avg = None
        post_tp1_max_pct_median = None

    cur.execute(
        """
        SELECT s.pair_address, s.entry_price, t.post_tp1_max_pct, p.url
        FROM signal_trigger_evaluations t
        JOIN signal_events s ON s.id = t.signal_id
        LEFT JOIN pairs p ON p.pair_address = s.pair_address
        WHERE t.status = 'DONE' AND t.outcome = 'TP1_FIRST' AND t.post_tp1_max_pct IS NOT NULL
        ORDER BY t.post_tp1_max_pct DESC
        LIMIT 10
        """
    )
    top10 = [
        {
            "pair_address": row[0],
            "entry_price": row[1],
            "post_tp1_max_pct": row[2],
            "url": row[3] or "",
        }
        for row in cur.fetchall()
    ]

    return {
        "total_signals": total_signals,
        "trigger_done": trigger_done,
        "trigger_no_data": trigger_no_data,
        "trigger_pending": trigger_pending,
        "outcome_tp1_first": outcome_tp1_first,
        "outcome_sl_first": outcome_sl_first,
        "outcome_neither": outcome_neither,
        "tp1_hit_rate": tp1_hit_rate,
        "sl_first_rate": sl_first_rate,
        "bu_after_tp1_rate": bu_after_tp1_rate,
        "post_tp1_max_pct_avg": post_tp1_max_pct_avg,
        "post_tp1_max_pct_median": post_tp1_max_pct_median,
        "top10_post_tp1": top10,
    }
