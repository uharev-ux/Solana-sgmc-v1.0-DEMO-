"""Collector: fetch pairs via client, normalize, persist via storage. Orchestrates client + storage."""

from __future__ import annotations

import csv
import time
from pathlib import Path

from dexscreener_screener.client import DexScreenerClient
from dexscreener_screener.logging_setup import get_logger
from dexscreener_screener.models import from_api_pair
from dexscreener_screener.storage import Database

logger = get_logger(__name__)


def parse_addresses_input(value: str) -> list[str]:
    """
    Parse input as file path (first column as addresses) or comma-separated string.
    Returns list of non-empty stripped addresses.
    """
    value = (value or "").strip()
    if not value:
        return []
    p = Path(value)
    if p.is_file():
        addresses = []
        for encoding in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
            try:
                with open(p, newline="", encoding=encoding) as f:
                    reader = csv.reader(f)
                    for row in reader:
                        if row and row[0].strip():
                            addresses.append(row[0].strip())
                return addresses
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                logger.warning("Failed to read file %s: %s", value, e)
                break
        return addresses
    return [a.strip() for a in value.split(",") if a.strip()]


class Collector:
    """Orchestrates fetch (client) -> normalize (models) -> persist (storage)."""

    def __init__(self, client: DexScreenerClient, db: Database) -> None:
        self.client = client
        self.db = db

    def collect_for_tokens(self, token_addresses: list[str]) -> tuple[int, int]:
        """
        Mode A: fetch pairs by token addresses, normalize, persist.
        Returns (pairs_processed, errors).
        """
        if not token_addresses:
            logger.info("collect_for_tokens: no token addresses provided")
            return 0, 0
        logger.info("collect_for_tokens: starting for %s token address(es)", len(token_addresses))
        raw_pairs = self.client.get_pairs_by_token_addresses_batched(token_addresses)
        return self._persist_pairs(raw_pairs)

    def collect_for_pairs(self, pair_addresses: list[str]) -> tuple[int, int]:
        """
        Mode B: fetch pairs by pair addresses, normalize, persist.
        Returns (pairs_processed, errors).
        """
        if not pair_addresses:
            logger.info("collect_for_pairs: no pair addresses provided")
            return 0, 0
        logger.info("collect_for_pairs: starting for %s pair address(es)", len(pair_addresses))
        raw_pairs = self.client.get_pairs_by_pair_addresses(pair_addresses)
        return self._persist_pairs(raw_pairs)

    def collect_from_raw_pairs(
        self, raw_pairs: list[dict], known_pair_addresses: set[str]
    ) -> tuple[int, int, int]:
        """
        Filter raw pairs to those not in known_pair_addresses, persist.
        Returns (processed, errors, skipped).
        """
        filtered = []
        for raw in raw_pairs:
            if not isinstance(raw, dict):
                continue
            addr = (raw.get("pairAddress") or raw.get("pair_address") or "").strip()
            if addr and addr not in known_pair_addresses:
                filtered.append(raw)
        skipped = len(raw_pairs) - len(filtered)
        processed, errors = self._persist_pairs(filtered)
        return processed, errors, skipped

    def _persist_pairs(self, raw_pairs: list[dict]) -> tuple[int, int]:
        """Normalize raw API pairs to PairSnapshot and write to DB. Returns (processed, errors)."""
        snapshot_ts = int(time.time() * 1000)
        processed = 0
        errors = 0
        for raw in raw_pairs:
            try:
                snapshot = from_api_pair(raw, snapshot_ts)
                if not snapshot.pair_address:
                    logger.warning("Skipping pair with empty pair_address")
                    errors += 1
                    continue
                self.db.upsert_token(snapshot.base_token)
                self.db.upsert_token(snapshot.quote_token)
                self.db.upsert_pair(snapshot)
                self.db.insert_snapshot(snapshot)
                self.db.update_dump_watchlist_for_snapshot(snapshot.pair_address)
                processed += 1
            except Exception as e:
                logger.warning("Failed to persist pair: %s", e)
                errors += 1
        logger.info("Persisted %s pair(s), %s error(s)", processed, errors)
        return processed, errors
