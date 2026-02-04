"""SQLite storage: tokens, pairs (latest), snapshots (history)."""

import sqlite3
from pathlib import Path
from typing import Any, Generator

from dexscreener_screener.models import PairSnapshot, TokenInfo

SCHEMA_TOKENS = """
CREATE TABLE IF NOT EXISTS tokens (
    address TEXT PRIMARY KEY,
    chain_id TEXT,
    symbol TEXT,
    name TEXT
);
"""

SCHEMA_PAIRS = """
CREATE TABLE IF NOT EXISTS pairs (
    pair_address TEXT PRIMARY KEY,
    chain_id TEXT,
    dex_id TEXT,
    url TEXT,
    base_address TEXT,
    base_symbol TEXT,
    base_name TEXT,
    quote_address TEXT,
    quote_symbol TEXT,
    quote_name TEXT,
    price_usd REAL,
    price_native REAL,
    liquidity_usd REAL,
    liquidity_base REAL,
    liquidity_quote REAL,
    volume_m5 REAL,
    volume_h1 REAL,
    volume_h6 REAL,
    volume_h24 REAL,
    price_change_m5 REAL,
    price_change_h1 REAL,
    price_change_h6 REAL,
    price_change_h24 REAL,
    txns_m5_buys INTEGER,
    txns_m5_sells INTEGER,
    txns_h1_buys INTEGER,
    txns_h1_sells INTEGER,
    txns_h6_buys INTEGER,
    txns_h6_sells INTEGER,
    txns_h24_buys INTEGER,
    txns_h24_sells INTEGER,
    fdv REAL,
    market_cap REAL,
    pair_created_at_ms INTEGER,
    snapshot_ts INTEGER
);
"""

SCHEMA_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_address TEXT NOT NULL,
    chain_id TEXT,
    dex_id TEXT,
    url TEXT,
    base_address TEXT,
    base_symbol TEXT,
    base_name TEXT,
    quote_address TEXT,
    quote_symbol TEXT,
    quote_name TEXT,
    price_usd REAL,
    price_native REAL,
    liquidity_usd REAL,
    liquidity_base REAL,
    liquidity_quote REAL,
    volume_m5 REAL,
    volume_h1 REAL,
    volume_h6 REAL,
    volume_h24 REAL,
    price_change_m5 REAL,
    price_change_h1 REAL,
    price_change_h6 REAL,
    price_change_h24 REAL,
    txns_m5_buys INTEGER,
    txns_m5_sells INTEGER,
    txns_h1_buys INTEGER,
    txns_h1_sells INTEGER,
    txns_h6_buys INTEGER,
    txns_h6_sells INTEGER,
    txns_h24_buys INTEGER,
    txns_h24_sells INTEGER,
    fdv REAL,
    market_cap REAL,
    pair_created_at_ms INTEGER,
    snapshot_ts INTEGER
);
"""

IDX_SNAPSHOTS_PAIR_TS = """
CREATE INDEX IF NOT EXISTS idx_snapshots_pair_ts ON snapshots (pair_address, snapshot_ts);
"""

PAIRS_COLUMNS = [
    "pair_address", "chain_id", "dex_id", "url",
    "base_address", "base_symbol", "base_name",
    "quote_address", "quote_symbol", "quote_name",
    "price_usd", "price_native",
    "liquidity_usd", "liquidity_base", "liquidity_quote",
    "volume_m5", "volume_h1", "volume_h6", "volume_h24",
    "price_change_m5", "price_change_h1", "price_change_h6", "price_change_h24",
    "txns_m5_buys", "txns_m5_sells", "txns_h1_buys", "txns_h1_sells",
    "txns_h6_buys", "txns_h6_sells", "txns_h24_buys", "txns_h24_sells",
    "fdv", "market_cap", "pair_created_at_ms", "snapshot_ts",
]

SNAPSHOTS_COLUMNS = ["pair_address"] + PAIRS_COLUMNS[1:]


def _snapshot_to_row(s: PairSnapshot) -> tuple:
    return (
        s.pair_address,
        s.chain_id,
        s.dex_id,
        s.url,
        s.base_token.address,
        s.base_token.symbol,
        s.base_token.name,
        s.quote_token.address,
        s.quote_token.symbol,
        s.quote_token.name,
        s.price_usd,
        s.price_native,
        s.liquidity_usd,
        s.liquidity_base,
        s.liquidity_quote,
        s.volume_m5,
        s.volume_h1,
        s.volume_h6,
        s.volume_h24,
        s.price_change_m5,
        s.price_change_h1,
        s.price_change_h6,
        s.price_change_h24,
        s.txns_m5_buys,
        s.txns_m5_sells,
        s.txns_h1_buys,
        s.txns_h1_sells,
        s.txns_h6_buys,
        s.txns_h6_sells,
        s.txns_h24_buys,
        s.txns_h24_sells,
        s.fdv,
        s.market_cap,
        s.pair_created_at_ms,
        s.snapshot_ts,
    )


class Database:
    """SQLite wrapper for tokens, pairs, and snapshots."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._connect()
        self.init_schema()

    def _connect(self) -> None:
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript(SCHEMA_TOKENS + SCHEMA_PAIRS + SCHEMA_SNAPSHOTS + IDX_SNAPSHOTS_PAIR_TS)
        self._conn.commit()

    def upsert_token(self, token: TokenInfo) -> None:
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO tokens (address, chain_id, symbol, name) VALUES (?, ?, ?, ?)",
            (token.address, "solana", token.symbol, token.name),
        )
        self._conn.commit()

    def upsert_pair(self, snapshot: PairSnapshot) -> None:
        cur = self._conn.cursor()
        placeholders = ",".join("?" * len(PAIRS_COLUMNS))
        cur.execute(
            f"INSERT OR REPLACE INTO pairs ({','.join(PAIRS_COLUMNS)}) VALUES ({placeholders})",
            _snapshot_to_row(snapshot),
        )
        self._conn.commit()

    def insert_snapshot(self, snapshot: PairSnapshot) -> None:
        cur = self._conn.cursor()
        placeholders = ",".join("?" * len(SNAPSHOTS_COLUMNS))
        cur.execute(
            f"INSERT INTO snapshots ({','.join(SNAPSHOTS_COLUMNS)}) VALUES ({placeholders})",
            _snapshot_to_row(snapshot),
        )
        self._conn.commit()

    def iterate_snapshots(
        self,
        pair_address: str | None = None,
        since_ts: int | None = None,
        until_ts: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Yield snapshot rows as dicts for export."""
        cur = self._conn.cursor()
        sql = "SELECT * FROM snapshots WHERE 1=1"
        params: list[Any] = []
        if pair_address:
            sql += " AND pair_address = ?"
            params.append(pair_address)
        if since_ts is not None:
            sql += " AND snapshot_ts >= ?"
            params.append(since_ts)
        if until_ts is not None:
            sql += " AND snapshot_ts <= ?"
            params.append(until_ts)
        sql += " ORDER BY snapshot_ts ASC"
        cur.execute(sql, params)
        for row in cur:
            yield dict(row)

    def iterate_pairs(self) -> Generator[dict[str, Any], None, None]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM pairs")
        for row in cur:
            yield dict(row)

    def iterate_tokens(self) -> Generator[dict[str, Any], None, None]:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM tokens")
        for row in cur:
            yield dict(row)

    def get_known_pair_addresses(self) -> set[str]:
        """Return set of pair_address from pairs table for deduplication."""
        cur = self._conn.cursor()
        cur.execute("SELECT pair_address FROM pairs")
        return {row["pair_address"] for row in cur}

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
