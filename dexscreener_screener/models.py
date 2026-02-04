"""Unified data model for DexScreener pair snapshots."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TokenInfo:
    """Token identity (base or quote)."""
    address: str
    symbol: str
    name: str


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def _get_nested(d: dict | None, *keys: str) -> Any:
    if not d or not isinstance(d, dict):
        return None
    for k in keys:
        d = d.get(k) if isinstance(d, dict) else None
        if d is None:
            return None
    return d


def _period_value(d: dict | None, period: str) -> Any:
    if not d or not isinstance(d, dict):
        return None
    return d.get(period)


@dataclass
class PairSnapshot:
    """Unified snapshot of a DEX pair from any DexScreener endpoint."""
    snapshot_ts: int
    chain_id: str
    dex_id: str
    pair_address: str
    url: str
    base_token: TokenInfo
    quote_token: TokenInfo
    price_usd: float | None = None
    price_native: float | None = None
    liquidity_usd: float | None = None
    liquidity_base: float | None = None
    liquidity_quote: float | None = None
    volume_m5: float | None = None
    volume_h1: float | None = None
    volume_h6: float | None = None
    volume_h24: float | None = None
    price_change_m5: float | None = None
    price_change_h1: float | None = None
    price_change_h6: float | None = None
    price_change_h24: float | None = None
    txns_m5_buys: int | None = None
    txns_m5_sells: int | None = None
    txns_h1_buys: int | None = None
    txns_h1_sells: int | None = None
    txns_h6_buys: int | None = None
    txns_h6_sells: int | None = None
    txns_h24_buys: int | None = None
    txns_h24_sells: int | None = None
    fdv: float | None = None
    market_cap: float | None = None
    pair_created_at_ms: int | None = None
    age_seconds: float | None = None


def _token_from_dict(d: dict | None) -> TokenInfo:
    if not d or not isinstance(d, dict):
        return TokenInfo(address="", symbol="", name="")
    return TokenInfo(
        address=str(d.get("address") or "").strip(),
        symbol=str(d.get("symbol") or "").strip(),
        name=str(d.get("name") or "").strip(),
    )


def from_api_pair(pair_dict: dict, snapshot_ts: int) -> PairSnapshot:
    """
    Build a PairSnapshot from a raw pair object returned by any DexScreener endpoint
    (/latest/dex/pairs, /tokens/v1, /token-pairs/v1).
    """
    base = _token_from_dict(pair_dict.get("baseToken"))
    quote = _token_from_dict(pair_dict.get("quoteToken"))
    chain_id = str(pair_dict.get("chainId") or "solana").strip()
    dex_id = str(pair_dict.get("dexId") or "").strip()
    pair_address = str(pair_dict.get("pairAddress") or "").strip()
    url = str(pair_dict.get("url") or "").strip()

    price_usd = _parse_float(pair_dict.get("priceUsd"))
    price_native = _parse_float(pair_dict.get("priceNative"))

    liq = pair_dict.get("liquidity")
    liquidity_usd = _parse_float(_get_nested(liq, "usd")) if liq else None
    liquidity_base = _parse_float(_get_nested(liq, "base")) if liq else None
    liquidity_quote = _parse_float(_get_nested(liq, "quote")) if liq else None

    vol = pair_dict.get("volume")
    volume_m5 = _parse_float(_period_value(vol, "m5"))
    volume_h1 = _parse_float(_period_value(vol, "h1"))
    volume_h6 = _parse_float(_period_value(vol, "h6"))
    volume_h24 = _parse_float(_period_value(vol, "h24"))

    pc = pair_dict.get("priceChange")
    price_change_m5 = _parse_float(_period_value(pc, "m5"))
    price_change_h1 = _parse_float(_period_value(pc, "h1"))
    price_change_h6 = _parse_float(_period_value(pc, "h6"))
    price_change_h24 = _parse_float(_period_value(pc, "h24"))

    txns = pair_dict.get("txns")
    def tx_buys(period: str) -> int | None:
        p = _period_value(txns, period)
        return _parse_int(p.get("buys")) if p and isinstance(p, dict) else None
    def tx_sells(period: str) -> int | None:
        p = _period_value(txns, period)
        return _parse_int(p.get("sells")) if p and isinstance(p, dict) else None

    pair_created_at_ms = _parse_int(pair_dict.get("pairCreatedAt"))
    if isinstance(pair_dict.get("pairCreatedAt"), int):
        pair_created_at_ms = pair_dict["pairCreatedAt"]
    age_seconds = None
    if pair_created_at_ms is not None and snapshot_ts is not None:
        age_seconds = (snapshot_ts - pair_created_at_ms) / 1000.0

    return PairSnapshot(
        snapshot_ts=snapshot_ts,
        chain_id=chain_id,
        dex_id=dex_id,
        pair_address=pair_address,
        url=url,
        base_token=base,
        quote_token=quote,
        price_usd=price_usd,
        price_native=price_native,
        liquidity_usd=liquidity_usd,
        liquidity_base=liquidity_base,
        liquidity_quote=liquidity_quote,
        volume_m5=volume_m5,
        volume_h1=volume_h1,
        volume_h6=volume_h6,
        volume_h24=volume_h24,
        price_change_m5=price_change_m5,
        price_change_h1=price_change_h1,
        price_change_h6=price_change_h6,
        price_change_h24=price_change_h24,
        txns_m5_buys=tx_buys("m5"),
        txns_m5_sells=tx_sells("m5"),
        txns_h1_buys=tx_buys("h1"),
        txns_h1_sells=tx_sells("h1"),
        txns_h6_buys=tx_buys("h6"),
        txns_h6_sells=tx_sells("h6"),
        txns_h24_buys=tx_buys("h24"),
        txns_h24_sells=tx_sells("h24"),
        fdv=_parse_float(pair_dict.get("fdv")),
        market_cap=_parse_float(pair_dict.get("marketCap")),
        pair_created_at_ms=pair_created_at_ms,
        age_seconds=age_seconds,
    )
