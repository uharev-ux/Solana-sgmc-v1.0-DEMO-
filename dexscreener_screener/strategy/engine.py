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


class StrategyEngine:
    """
    Second screener: for each pair get current price and ATH from price history,
    compute drop_from_ath, apply hard filters, classify WATCHLIST / SIGNAL.
    """

    def __init__(self, db: Database) -> None:
        self.db = db

    def run(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Run strategy once. Returns (watchlist_entries, signal_entries).
        Each entry has pair_address, current_price, ath_price, drop_from_ath, liq, vol, txns, url, etc.
        """
        watchlist: list[dict[str, Any]] = []
        signals: list[dict[str, Any]] = []
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
            ath_price = self.db.fetch_ath_price(pair_address, since_ts=since_ts)

            if current_price is None or current_price <= 0:
                continue
            if ath_price is None or ath_price <= 0:
                continue
            if ath_price == current_price:
                continue

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
            entry = {
                "pair_address": pair_address,
                "url": url,
                "current_price": current_price,
                "ath_price": ath_price,
                "drop_from_ath": drop_from_ath,
                "liquidity_usd": liq,
                "volume_h24": vol,
                "txns_h24": txns_h24,
                "buys_h24": buys_h24,
            }

            if drop_from_ath >= config.WATCHLIST_MIN_DROP and drop_from_ath < config.SIGNAL_MIN_DROP:
                watchlist.append(entry)
                self.db.insert_strategy_decision(
                    pair_address=pair_address,
                    decision="WATCHLIST",
                    current_price=current_price,
                    ath_price=ath_price,
                    drop_from_ath=drop_from_ath,
                    reasons_json=json.dumps({"drop_from_ath": drop_from_ath, "liq": liq, "vol": vol, "txns": txns_h24}),
                )
            elif (
                config.SIGNAL_MIN_DROP <= drop_from_ath <= config.SIGNAL_MAX_DROP
                and txns_h24 >= config.TXNS_SIGNAL
                and buys_h24 >= config.BUYS_MIN
                and liq >= config.LIQ_SIGNAL
            ):
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
                        "drop_from_ath": drop_from_ath,
                        "txns": txns_h24,
                        "buys": buys_h24,
                        "liq": liq,
                    }),
                )
                self.db.set_signal_cooldown(pair_address)

        return watchlist, signals


def run_strategy_once(db: Database) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Convenience: run StrategyEngine once. Returns (watchlist_entries, signal_entries)."""
    engine = StrategyEngine(db)
    return engine.run()
