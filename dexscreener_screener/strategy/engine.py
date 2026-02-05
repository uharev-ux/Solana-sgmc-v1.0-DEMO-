"""
Strategy engine: true ATH from price history, drop_from_ath, hard filters, WATCHLIST / SIGNAL.
Uses ONLY real prices from DB (no %change).
"""

from __future__ import annotations

import json
import time
from typing import Any

from dexscreener_screener import config
from dexscreener_screener.storage import Database


def _float(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def compute_drop_from_ath(ath_price: float, current_price: float) -> float:
    """drop_from_ath = (ath_price - current_price) / ath_price * 100. No %change used."""
    if ath_price is None or ath_price <= 0:
        return 0.0
    if current_price is None or current_price < 0:
        current_price = 0.0
    return (ath_price - current_price) / ath_price * 100.0


def _classify_by_drop(
    drop: float, liq: float, txns: int, buys: int
) -> str:
    """
    Classify by drop level; apply market_quality_downgrade if market is weak.
    Returns: REJECT | WATCHLIST_L1 | WATCHLIST_L2 | WATCHLIST_L3 | SIGNAL
    """
    if drop < config.WL1_MIN_DROP:
        return "REJECT"
    if config.SIGNAL_MIN_DROP <= drop <= config.SIGNAL_MAX_DROP:
        if txns >= config.TXNS_SIGNAL and buys >= config.BUYS_MIN and liq >= config.LIQ_SIGNAL:
            return "SIGNAL"
        return "REJECT"  # in signal zone but conditions not met
    # Watchlist zone: determine initial level, then downgrade if weak market
    if config.WL3_MIN_DROP <= drop < config.SIGNAL_MIN_DROP:
        cand = "WATCHLIST_L3"
    elif config.WL2_MIN_DROP <= drop < config.WL3_MIN_DROP:
        cand = "WATCHLIST_L2"
    else:  # WL1_MIN_DROP <= drop < WL2_MIN_DROP
        cand = "WATCHLIST_L1"
    # market_quality_downgrade
    if cand == "WATCHLIST_L3" and (txns < config.WL3_MIN_TXNS or liq < config.WL3_MIN_LIQ):
        cand = "WATCHLIST_L2"
    if cand == "WATCHLIST_L2" and (txns < config.WL2_MIN_TXNS or liq < config.WL2_MIN_LIQ):
        cand = "WATCHLIST_L1"
    if cand == "WATCHLIST_L1" and (txns < config.WL1_MIN_TXNS or liq < config.WL1_MIN_LIQ):
        return "REJECT"
    return cand


def _validate_ath_activity(activity: dict[str, Any]) -> bool:
    """True if activity window meets config thresholds (snapshots; txns/volume if present)."""
    snapshots_count = activity.get("snapshots_count") or 0
    if snapshots_count < config.ATH_MIN_SNAPSHOTS_IN_WINDOW:
        return False
    txns_sum = activity.get("txns_sum")
    if txns_sum is not None and txns_sum < config.ATH_MIN_TXNS_IN_WINDOW:
        return False
    volume_sum = activity.get("volume_sum")
    if volume_sum is not None and volume_sum < config.ATH_MIN_VOLUME_IN_WINDOW:
        return False
    return True


def _find_valid_ath(
    db: Database,
    pair_address: str,
    since_ts: int | None,
    current_price: float,
) -> tuple[float | None, int | None, dict[str, Any] | None, str] | tuple[str, dict[str, Any]] | None:
    """
    Return (valid_ath_price, valid_ath_ts, ath_validation_metrics, ath_source) or None if no valid ATH.
    Return ("BOOTSTRAP", activity) when ATH exists but insufficient snapshots in window (do not REJECT).
    ath_source is "raw" or "fallback".
    """
    ath_point = db.fetch_ath_point(pair_address, since_ts=since_ts)
    if not ath_point:
        return None
    raw_price = ath_point["ath_price"]
    raw_ts = ath_point["ath_ts"]
    current_ts = ath_point.get("current_ts")
    if current_price is not None and current_ts is not None and raw_ts == current_ts and raw_price == current_price:
        return None  # no drawdown: ATH is current
    activity = db.fetch_activity_window(
        pair_address, raw_ts, config.ATH_VALIDATE_WINDOW_SEC
    )
    if _validate_ath_activity(activity):
        return (raw_price, raw_ts, activity, "raw")

    # Raw ATH failed; if failure due to insufficient snapshots, remember for bootstrap
    bootstrap_activity = activity if (activity.get("snapshots_count") or 0) < config.ATH_MIN_SNAPSHOTS_IN_WINDOW else None

    candidates = db.fetch_ath_candidates(
        pair_address, since_ts=since_ts, limit=config.ATH_FALLBACK_MAX_ATTEMPTS
    )
    for price, ts in candidates[1:]:  # skip raw (first)
        if price <= 0 or price <= current_price:
            continue
        act = db.fetch_activity_window(pair_address, ts, config.ATH_VALIDATE_WINDOW_SEC)
        if _validate_ath_activity(act):
            return (price, ts, act, "fallback")
    # No valid ATH; if we had insufficient price history (snapshots in window), return BOOTSTRAP
    if bootstrap_activity is not None:
        return ("BOOTSTRAP", bootstrap_activity)
    return None


class StrategyEngine:
    """
    Second screener: for each pair get current price and ATH from price history,
    compute drop_from_ath, apply hard filters, classify WATCHLIST / SIGNAL.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Run strategy once. Returns (signals, watchlist_bootstrap, watchlist_l3, watchlist_l2, watchlist_l1).
        Each entry has pair_address, current_price, ath_price, drop_from_ath, liq, vol, txns, url, score, etc.
        """
        signals: list[dict[str, Any]] = []
        watchlist_bootstrap: list[dict[str, Any]] = []
        watchlist_l3: list[dict[str, Any]] = []
        watchlist_l2: list[dict[str, Any]] = []
        watchlist_l1: list[dict[str, Any]] = []
        now_ms = int(time.time() * 1000)
        max_age_ms = int(config.STRATEGY_MAX_AGE_HOURS * 3600 * 1000)

        for pair_row in self.db.iterate_pairs():
            pair_address = pair_row.get("pair_address") or ""
            if not pair_address:
                continue

            pair_created_at_ms = _int(pair_row.get("pair_created_at_ms"))
            age_hours = None
            if pair_created_at_ms and pair_created_at_ms > 0:
                age_hours = (now_ms - pair_created_at_ms) / (3600 * 1000)
                if age_hours > config.STRATEGY_MAX_AGE_HOURS:
                    continue

            current_price = self.db.fetch_latest_price(pair_address)
            since_ts = pair_created_at_ms if pair_created_at_ms and pair_created_at_ms > 0 else None

            if current_price is None or current_price <= 0:
                continue  # REJECT: current_price missing or <= 0

            # Bootstrap: insufficient price history (fewer than BOOTSTRAP_MIN_SNAPSHOTS) -> WATCHLIST_BOOTSTRAP, not REJECT
            snapshot_count = self.db.get_snapshot_count(pair_address)
            if snapshot_count < config.BOOTSTRAP_MIN_SNAPSHOTS:
                liq = _float(pair_row.get("liquidity_usd"))
                vol = _float(pair_row.get("volume_h24"))
                txns_h24 = _int(pair_row.get("txns_h24_buys")) + _int(pair_row.get("txns_h24_sells"))
                if liq < config.BOOTSTRAP_MIN_LIQ or vol < config.STRATEGY_MIN_VOL or txns_h24 < config.BOOTSTRAP_MIN_TXNS:
                    continue
                url = str(pair_row.get("url") or "")
                entry = {
                    "pair_address": pair_address,
                    "url": url,
                    "current_price": current_price,
                    "ath_price": None,
                    "drop_from_ath": None,
                    "score": 0.0,
                    "liquidity_usd": liq,
                    "volume_h24": vol,
                    "txns_h24": txns_h24,
                    "buys_h24": _int(pair_row.get("txns_h24_buys")),
                }
                watchlist_bootstrap.append(entry)
                self.db.insert_strategy_decision(
                    pair_address=pair_address,
                    decision="WATCHLIST_BOOTSTRAP",
                    current_price=current_price,
                    ath_price=None,
                    drop_from_ath=None,
                    reasons_json=json.dumps({
                        "reason": "insufficient_price_history",
                        "ath_valid": False,
                        "ath_validation_metrics": {"snapshots_count": snapshot_count},
                    }),
                )
                continue

            valid_ath_result = _find_valid_ath(
                self.db, pair_address, since_ts, current_price
            )
            if valid_ath_result is None:
                self.db.insert_strategy_decision(
                    pair_address=pair_address,
                    decision="REJECT",
                    current_price=current_price,
                    ath_price=None,
                    drop_from_ath=None,
                    reasons_json=json.dumps({
                        "reason": "valid_ath_not_found",
                        "ath_valid": False,
                        "ath_validation_metrics": None,
                        "ath_source": None,
                    }),
                )
                continue

            # Bootstrap: insufficient price history; apply hard filters only, no ATH logic
            if isinstance(valid_ath_result, (tuple, list)) and len(valid_ath_result) == 2 and valid_ath_result[0] == "BOOTSTRAP":
                _activity = valid_ath_result[1]
                liq = _float(pair_row.get("liquidity_usd"))
                vol = _float(pair_row.get("volume_h24"))
                txns_h24 = _int(pair_row.get("txns_h24_buys")) + _int(pair_row.get("txns_h24_sells"))
                if liq < config.BOOTSTRAP_MIN_LIQ or vol < config.STRATEGY_MIN_VOL or txns_h24 < config.BOOTSTRAP_MIN_TXNS:
                    continue
                url = str(pair_row.get("url") or "")
                entry = {
                    "pair_address": pair_address,
                    "url": url,
                    "current_price": current_price,
                    "ath_price": None,
                    "drop_from_ath": None,
                    "score": 0.0,
                    "liquidity_usd": liq,
                    "volume_h24": vol,
                    "txns_h24": txns_h24,
                    "buys_h24": _int(pair_row.get("txns_h24_buys")),
                }
                watchlist_bootstrap.append(entry)
                self.db.insert_strategy_decision(
                    pair_address=pair_address,
                    decision="WATCHLIST_BOOTSTRAP",
                    current_price=current_price,
                    ath_price=None,
                    drop_from_ath=None,
                    reasons_json=json.dumps({
                        "reason": "insufficient_price_history",
                        "ath_valid": False,
                        "ath_validation_metrics": _activity,
                    }),
                )
                continue

            ath_price, _ath_ts, ath_validation_metrics, ath_source = valid_ath_result
            if ath_price is None or ath_price <= 0:
                continue
            if ath_price == current_price:
                continue  # REJECT: no drawdown

            drop_from_ath = compute_drop_from_ath(ath_price, current_price)

            liq = _float(pair_row.get("liquidity_usd"))
            vol = _float(pair_row.get("volume_h24"))
            buys_h24 = _int(pair_row.get("txns_h24_buys"))
            sells_h24 = _int(pair_row.get("txns_h24_sells"))
            txns_h24 = buys_h24 + sells_h24

            if liq < config.STRATEGY_MIN_LIQ:
                continue
            if vol < config.STRATEGY_MIN_VOL:
                continue
            if txns_h24 < config.STRATEGY_MIN_TXNS:
                continue

            url = str(pair_row.get("url") or "")
            score = drop_from_ath  # for sorting: higher drop = higher score
            entry = {
                "pair_address": pair_address,
                "url": url,
                "current_price": current_price,
                "ath_price": ath_price,
                "drop_from_ath": drop_from_ath,
                "score": score,
                "liquidity_usd": liq,
                "volume_h24": vol,
                "txns_h24": txns_h24,
                "buys_h24": buys_h24,
            }

            base_reasons: dict[str, Any] = {
                "drop_from_ath": drop_from_ath,
                "ath_valid": True,
                "ath_validation_metrics": ath_validation_metrics,
                "ath_source": ath_source,
            }

            # Determine decision by drop level, then apply market_quality_downgrade
            decision = _classify_by_drop(drop_from_ath, liq, txns_h24, buys_h24)
            if decision == "REJECT":
                self.db.insert_strategy_decision(
                    pair_address=pair_address,
                    decision="REJECT",
                    current_price=current_price,
                    ath_price=ath_price,
                    drop_from_ath=drop_from_ath,
                    reasons_json=json.dumps({
                        **base_reasons,
                        "reason": "drop_below_wl1" if drop_from_ath < config.WL1_MIN_DROP else "market_quality_downgrade",
                        "liq": liq,
                        "txns": txns_h24,
                    }),
                )
                continue

            if decision == "SIGNAL":
                last_signal = self.db.get_last_signal_at(pair_address)
                if last_signal is not None:
                    if (now_ms - last_signal) / 1000 < config.SIGNAL_COOLDOWN_SEC:
                        continue
                signals.append(entry)
                self.db.insert_strategy_decision(
                    pair_address=pair_address,
                    decision="SIGNAL",
                    current_price=current_price,
                    ath_price=ath_price,
                    drop_from_ath=drop_from_ath,
                    reasons_json=json.dumps({
                        **base_reasons,
                        "txns": txns_h24,
                        "buys": buys_h24,
                        "liq": liq,
                    }),
                )
                self.db.set_signal_cooldown(pair_address)
                signal_id = self.db.insert_signal_event(
                    pair_address=pair_address,
                    signal_ts=now_ms,
                    entry_price=current_price,
                    ath_price=ath_price,
                    drop_from_ath=drop_from_ath,
                    score=score,
                    features_json=json.dumps({
                        "liquidity_usd": liq,
                        "volume_h24": vol,
                        "txns_h24": txns_h24,
                        "buys_h24": buys_h24,
                    }),
                )
                self.db.insert_trigger_eval_pending(signal_id)
                for horizon_sec in config.POST_HORIZONS_SEC:
                    self.db.insert_signal_evaluation(signal_id=signal_id, horizon_sec=horizon_sec, status="PENDING")
                continue

            # Watchlist level
            if decision == "WATCHLIST_L3":
                watchlist_l3.append(entry)
            elif decision == "WATCHLIST_L2":
                watchlist_l2.append(entry)
            elif decision == "WATCHLIST_L1":
                watchlist_l1.append(entry)
            self.db.insert_strategy_decision(
                pair_address=pair_address,
                decision=decision,
                current_price=current_price,
                ath_price=ath_price,
                drop_from_ath=drop_from_ath,
                reasons_json=json.dumps({
                    **base_reasons,
                    "liq": liq,
                    "vol": vol,
                    "txns": txns_h24,
                }),
            )

        return signals, watchlist_bootstrap, watchlist_l3, watchlist_l2, watchlist_l1


def run_strategy_once(
    db: Database,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Convenience: run StrategyEngine once. Returns (signals, watchlist_bootstrap, watchlist_l3, watchlist_l2, watchlist_l1)."""
    engine = StrategyEngine(db)
    return engine.run()
