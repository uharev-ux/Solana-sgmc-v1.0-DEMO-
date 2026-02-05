"""SQLite storage: tokens, pairs (latest), snapshots (history). Only DB logic; no API knowledge."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Generator, Sequence

from dexscreener_screener import config
from dexscreener_screener.models import PairSnapshot, TokenInfo

# Schema and column definitions (DB-specific)
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

IDX_SNAPSHOTS_PAIR = """
CREATE INDEX IF NOT EXISTS idx_snapshots_pair_address ON snapshots (pair_address);
"""

IDX_PAIRS_CREATED = """
CREATE INDEX IF NOT EXISTS idx_pairs_pair_created_at_ms ON pairs (pair_created_at_ms);
"""

SCHEMA_DUMP_WATCHLIST = """
CREATE TABLE IF NOT EXISTS dump_watchlist (
    pair_address TEXT PRIMARY KEY,
    added_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL,
    peak_price REAL NOT NULL,
    peak_ts INTEGER NOT NULL,
    low_price REAL NOT NULL,
    low_ts INTEGER NOT NULL,
    last_price REAL NOT NULL,
    last_ts INTEGER NOT NULL,
    drop_pct REAL NOT NULL,
    volume_m5 REAL,
    buys_m5 INTEGER,
    sells_m5 INTEGER,
    signal_ts INTEGER,
    signal_price REAL
);
"""
IDX_DUMP_WATCHLIST_STATE = "CREATE INDEX IF NOT EXISTS idx_dump_watchlist_state ON dump_watchlist(state);"
IDX_DUMP_WATCHLIST_UPDATED = "CREATE INDEX IF NOT EXISTS idx_dump_watchlist_updated ON dump_watchlist(updated_at_ms);"

# --- Strategy layer (second screener): ATH-based drawdown ---
SCHEMA_STRATEGY_DECISIONS = """
CREATE TABLE IF NOT EXISTS strategy_decisions (
    pair_address TEXT NOT NULL,
    decided_at INTEGER NOT NULL,
    decision TEXT NOT NULL,
    current_price REAL,
    ath_price REAL,
    drop_from_ath REAL,
    reasons_json TEXT
);
"""
IDX_STRATEGY_DECISIONS_PAIR = "CREATE INDEX IF NOT EXISTS idx_strategy_decisions_pair ON strategy_decisions(pair_address);"
IDX_STRATEGY_DECISIONS_DECIDED = "CREATE INDEX IF NOT EXISTS idx_strategy_decisions_decided_at ON strategy_decisions(decided_at);"

SCHEMA_SIGNAL_COOLDOWNS = """
CREATE TABLE IF NOT EXISTS signal_cooldowns (
    pair_address TEXT PRIMARY KEY,
    last_signal_at INTEGER NOT NULL
);
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


def _pragma_table_info(conn: sqlite3.Connection, table: str) -> list[tuple[str, str]]:
    """Return (name, type) for each column in table."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [(r[1], (r[2] or "").upper()) for r in cur.fetchall()]


def _pick(cols: Sequence[tuple[str, str]], candidates: Sequence[str]) -> str | None:
    """Return first matching column name (preserving schema case) or None."""
    m = {name.lower(): name for name, _ in cols}
    for c in candidates:
        if c.lower() in m:
            return m[c.lower()]
    return None


def _must_pick(
    conn: sqlite3.Connection,
    table: str,
    candidates: Sequence[str],
    what: str,
) -> str:
    """Return matching column or raise ValueError with helpful message."""
    cols = _pragma_table_info(conn, table)
    picked = _pick(cols, candidates)
    if not picked:
        raise ValueError(
            f"{what}: table '{table}' has no recognized column. "
            f"Checked: {list(candidates)}. Found: {[c[0] for c in cols]}"
        )
    return picked


def _detect_ms_or_sec(conn: sqlite3.Connection, table: str, ts_col: str) -> int:
    """Return 1000 if column values are ms, else 1 (seconds)."""
    row = conn.execute(f"SELECT MAX({ts_col}) FROM {table}").fetchone()
    mx = row[0] if row else None
    if not mx:
        return 1
    return 1000 if mx > 10**12 else 1


def _snapshot_to_row(s: PairSnapshot) -> tuple:
    """Convert PairSnapshot to row tuple for insert."""
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
    """SQLite wrapper for tokens, pairs, snapshots, dump_watchlist. No API knowledge."""

    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._connect()
        self.init_schema()

    def _connect(self) -> None:
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row

    def init_schema(self) -> None:
        """Create tables and indexes if missing."""
        cur = self._conn.cursor()
        cur.executescript(
            SCHEMA_TOKENS + SCHEMA_PAIRS + SCHEMA_SNAPSHOTS
            + IDX_SNAPSHOTS_PAIR_TS + IDX_SNAPSHOTS_PAIR + IDX_PAIRS_CREATED
        )
        self.ensure_dump_watchlist_schema()
        self.ensure_strategy_schema()
        self._conn.commit()

    def upsert_token(self, token: TokenInfo) -> None:
        """Insert or replace token by address."""
        cur = self._conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO tokens (address, chain_id, symbol, name) VALUES (?, ?, ?, ?)",
            (token.address, config.CHAIN_SOLANA, token.symbol, token.name),
        )
        self._conn.commit()

    def upsert_pair(self, snapshot: PairSnapshot) -> None:
        """Insert or replace pair by pair_address."""
        cur = self._conn.cursor()
        placeholders = ",".join("?" * len(PAIRS_COLUMNS))
        cur.execute(
            f"INSERT OR REPLACE INTO pairs ({','.join(PAIRS_COLUMNS)}) VALUES ({placeholders})",
            _snapshot_to_row(snapshot),
        )
        self._conn.commit()

    def insert_snapshot(self, snapshot: PairSnapshot) -> None:
        """Append one snapshot row (history)."""
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
        """Yield all pairs as dicts."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM pairs")
        for row in cur:
            yield dict(row)

    def iterate_tokens(self) -> Generator[dict[str, Any], None, None]:
        """Yield all tokens as dicts."""
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM tokens")
        for row in cur:
            yield dict(row)

    def get_known_pair_addresses(self) -> set[str]:
        """Return set of pair_address from pairs table for deduplication."""
        cur = self._conn.cursor()
        cur.execute("SELECT pair_address FROM pairs")
        return {row["pair_address"] for row in cur}

    def _ensure_prune_indexes(
        self,
        snap_ts_col: str,
        snap_pair_ref_col: str,
        pairs_pair_col: str,
        pairs_base_col: str,
        pairs_quote_col: str,
        tokens_addr_col: str,
    ) -> None:
        """Create indexes for prune performance (best-effort, ignore failures)."""
        cur = self._conn.cursor()
        stmts = [
            f"CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots({snap_ts_col})",
            f"CREATE INDEX IF NOT EXISTS idx_snapshots_pairref ON snapshots({snap_pair_ref_col})",
            f"CREATE INDEX IF NOT EXISTS idx_pairs_pair ON pairs({pairs_pair_col})",
            f"CREATE INDEX IF NOT EXISTS idx_pairs_base ON pairs({pairs_base_col})",
            f"CREATE INDEX IF NOT EXISTS idx_pairs_quote ON pairs({pairs_quote_col})",
            f"CREATE INDEX IF NOT EXISTS idx_tokens_addr ON tokens({tokens_addr_col})",
        ]
        for sql in stmts:
            try:
                cur.execute(sql)
            except Exception:
                pass
        self._conn.commit()

    def prune(
        self,
        max_age_hours: float = config.DEFAULT_PRUNE_MAX_AGE_HOURS,
        ts_column: str | None = None,
        dry_run: bool = False,
        vacuum: bool = False,
    ) -> tuple[int, int, int]:
        """
        Remove old snapshots, orphaned pairs, orphaned tokens.
        Returns (snapshots_deleted, pairs_deleted, tokens_deleted).
        Uses NOT EXISTS; auto-detects timestamp column and ms/sec.
        """
        snap_ts_col = ts_column or _must_pick(
            self._conn, "snapshots", config.TS_CANDIDATES, "Timestamp"
        )
        snap_pair_ref_col = _must_pick(
            self._conn, "snapshots", config.SNAP_PAIR_REF_CANDIDATES, "Snapshot pair ref"
        )
        pairs_pair_col = _must_pick(
            self._conn, "pairs", config.PAIRS_PAIR_CANDIDATES, "Pairs address"
        )
        pairs_base_col = _must_pick(
            self._conn, "pairs", config.PAIRS_BASE_CANDIDATES, "Pairs base token"
        )
        pairs_quote_col = _must_pick(
            self._conn, "pairs", config.PAIRS_QUOTE_CANDIDATES, "Pairs quote token"
        )
        tokens_addr_col = _must_pick(
            self._conn, "tokens", config.TOKENS_ADDR_CANDIDATES, "Tokens address"
        )

        unit = _detect_ms_or_sec(self._conn, "snapshots", snap_ts_col)
        cutoff = int((time.time() - max_age_hours * 3600) * unit)

        self._ensure_prune_indexes(
            snap_ts_col, snap_pair_ref_col, pairs_pair_col,
            pairs_base_col, pairs_quote_col, tokens_addr_col,
        )

        cur = self._conn.cursor()
        if not dry_run:
            self._conn.execute("BEGIN IMMEDIATE")

        try:
            if dry_run:
                s_cnt = cur.execute(
                    f"SELECT COUNT(*) FROM snapshots WHERE {snap_ts_col} < ?",
                    (cutoff,),
                ).fetchone()[0]
            else:
                cur.execute(
                    f"DELETE FROM snapshots WHERE {snap_ts_col} < ?",
                    (cutoff,),
                )
                s_cnt = cur.rowcount

            if dry_run:
                p_cnt = cur.execute(
                    f"""
                    SELECT COUNT(*) FROM pairs
                    WHERE NOT EXISTS (
                        SELECT 1 FROM snapshots s
                        WHERE s.{snap_pair_ref_col} = pairs.{pairs_pair_col}
                    )
                    """
                ).fetchone()[0]
            else:
                cur.execute(
                    f"""
                    DELETE FROM pairs
                    WHERE NOT EXISTS (
                        SELECT 1 FROM snapshots s
                        WHERE s.{snap_pair_ref_col} = pairs.{pairs_pair_col}
                    )
                    """
                )
                p_cnt = cur.rowcount

            if dry_run:
                t_cnt = cur.execute(
                    f"""
                    SELECT COUNT(*) FROM tokens
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pairs p
                        WHERE p.{pairs_base_col} = tokens.{tokens_addr_col}
                           OR p.{pairs_quote_col} = tokens.{tokens_addr_col}
                    )
                    """
                ).fetchone()[0]
            else:
                cur.execute(
                    f"""
                    DELETE FROM tokens
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pairs p
                        WHERE p.{pairs_base_col} = tokens.{tokens_addr_col}
                           OR p.{pairs_quote_col} = tokens.{tokens_addr_col}
                    )
                    """
                )
                t_cnt = cur.rowcount

            if dry_run:
                pass
            else:
                self._conn.commit()
                if vacuum:
                    self._conn.execute("VACUUM")

            return int(s_cnt), int(p_cnt), int(t_cnt)

        except Exception:
            if not dry_run:
                self._conn.rollback()
            raise

    def prune_by_pair_age(
        self,
        max_age_hours: float = config.DEFAULT_PRUNE_MAX_AGE_HOURS,
        dry_run: bool = False,
        vacuum: bool = False,
    ) -> tuple[int, int, int]:
        """
        Remove pairs older than max_age_hours (by pair_created_at_ms) and orphan tokens.
        Returns (snapshots_deleted, pairs_deleted, tokens_deleted).
        """
        cutoff_ms = int((time.time() - max_age_hours * 3600) * 1000)
        cur = self._conn.cursor()

        if not dry_run:
            self._conn.execute("BEGIN IMMEDIATE")

        try:
            if dry_run:
                s_cnt = cur.execute(
                    """
                    SELECT COUNT(*) FROM snapshots
                    WHERE EXISTS (
                        SELECT 1 FROM pairs p
                        WHERE p.pair_address = snapshots.pair_address
                          AND p.pair_created_at_ms < ?
                          AND p.pair_created_at_ms IS NOT NULL
                          AND p.pair_created_at_ms != 0
                    )
                    """,
                    (cutoff_ms,),
                ).fetchone()[0]
            else:
                cur.execute(
                    """
                    DELETE FROM snapshots
                    WHERE EXISTS (
                        SELECT 1 FROM pairs p
                        WHERE p.pair_address = snapshots.pair_address
                          AND p.pair_created_at_ms < ?
                          AND p.pair_created_at_ms IS NOT NULL
                          AND p.pair_created_at_ms != 0
                    )
                    """,
                    (cutoff_ms,),
                )
                s_cnt = cur.rowcount

            if dry_run:
                p_cnt = cur.execute(
                    """
                    SELECT COUNT(*) FROM pairs
                    WHERE pair_created_at_ms < ?
                      AND pair_created_at_ms IS NOT NULL
                      AND pair_created_at_ms != 0
                    """,
                    (cutoff_ms,),
                ).fetchone()[0]
            else:
                cur.execute(
                    """
                    DELETE FROM pairs
                    WHERE pair_created_at_ms < ?
                      AND pair_created_at_ms IS NOT NULL
                      AND pair_created_at_ms != 0
                    """,
                    (cutoff_ms,),
                )
                p_cnt = cur.rowcount

            if dry_run:
                t_cnt = cur.execute(
                    """
                    SELECT COUNT(*) FROM tokens
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pairs p
                        WHERE p.base_address = tokens.address
                           OR p.quote_address = tokens.address
                    )
                    """
                ).fetchone()[0]
            else:
                cur.execute(
                    """
                    DELETE FROM tokens
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pairs p
                        WHERE p.base_address = tokens.address
                           OR p.quote_address = tokens.address
                    )
                    """
                )
                t_cnt = cur.rowcount

            if dry_run:
                pass
            else:
                self._conn.commit()
                if vacuum:
                    self._conn.execute("VACUUM")

            return int(s_cnt), int(p_cnt), int(t_cnt)

        except Exception:
            if not dry_run:
                self._conn.rollback()
            raise

    def self_check_invariants(self) -> tuple[int, int, int]:
        """
        Run 3 invariant checks (should all be 0 if prune_by_pair_age keeps only pairs <24h).
        Returns (old_pairs, old_pairs_snapshots, orphan_tokens).
        """
        cutoff_ms = int((time.time() - config.SELF_CHECK_AGE_HOURS * 3600) * 1000)
        cur = self._conn.cursor()
        a = cur.execute(
            """
            SELECT COUNT(*) FROM pairs
            WHERE pair_created_at_ms IS NOT NULL
              AND pair_created_at_ms > 0
              AND pair_created_at_ms < ?
            """,
            (cutoff_ms,),
        ).fetchone()[0]
        b = cur.execute(
            """
            SELECT COUNT(*) FROM snapshots s
            WHERE EXISTS (
              SELECT 1 FROM pairs p
              WHERE p.pair_address = s.pair_address
                AND p.pair_created_at_ms IS NOT NULL
                AND p.pair_created_at_ms > 0
                AND p.pair_created_at_ms < ?
            )
            """,
            (cutoff_ms,),
        ).fetchone()[0]
        c = cur.execute(
            """
            SELECT COUNT(*) FROM tokens t
            WHERE NOT EXISTS (
              SELECT 1 FROM pairs p
              WHERE p.base_address = t.address OR p.quote_address = t.address
            )
            """
        ).fetchone()[0]
        return int(a), int(b), int(c)

    def ensure_dump_watchlist_schema(self) -> None:
        """Create dump_watchlist table and indexes if missing."""
        cur = self._conn.cursor()
        cur.executescript(
            SCHEMA_DUMP_WATCHLIST + IDX_DUMP_WATCHLIST_STATE + IDX_DUMP_WATCHLIST_UPDATED
        )
        self._conn.commit()

    def update_dump_watchlist_for_snapshot(self, pair_address: str) -> None:
        """
        Update dump watchlist for a pair: detect dump (>=50% from peak), track low, signal reversal.
        Uses config thresholds: DROP_THRESHOLD, LIQ_MIN, VOL_M5_MIN, SELLS_MIN.
        """
        cur = self._conn.cursor()

        last_row = cur.execute(
            """
            SELECT price_usd, volume_m5, txns_m5_buys, txns_m5_sells, snapshot_ts
            FROM snapshots WHERE pair_address=? ORDER BY snapshot_ts DESC LIMIT 1
            """,
            (pair_address,),
        ).fetchone()
        if not last_row:
            return

        last_price = last_row["price_usd"]
        last_ts = last_row["snapshot_ts"]
        volume_m5 = last_row["volume_m5"]
        buys_m5 = last_row["txns_m5_buys"]
        sells_m5 = last_row["txns_m5_sells"]

        if last_price is None or last_price <= 0:
            return

        peak_row = cur.execute(
            """
            SELECT price_usd, snapshot_ts
            FROM snapshots
            WHERE pair_address=?
              AND price_usd IS NOT NULL AND price_usd > 0
            ORDER BY price_usd DESC, snapshot_ts DESC
            LIMIT 1
            """,
            (pair_address,),
        ).fetchone()
        if not peak_row:
            return

        peak_price = float(peak_row["price_usd"])
        peak_ts = int(peak_row["snapshot_ts"])

        drop_pct = (peak_price - last_price) / peak_price * 100.0

        pair_row = cur.execute(
            "SELECT liquidity_usd FROM pairs WHERE pair_address=?", (pair_address,)
        ).fetchone()
        liq = float(pair_row["liquidity_usd"]) if pair_row and pair_row["liquidity_usd"] is not None else 0.0
        vol = volume_m5 if volume_m5 is not None else 0.0
        sells = int(sells_m5) if sells_m5 is not None else 0

        now_ms = int(time.time() * 1000)

        two_rows = cur.execute(
            """
            SELECT price_usd, txns_m5_buys, txns_m5_sells, volume_m5, snapshot_ts
            FROM snapshots WHERE pair_address=? ORDER BY snapshot_ts DESC LIMIT 2
            """,
            (pair_address,),
        ).fetchall()

        existing = cur.execute(
            "SELECT pair_address, low_price, low_ts, state, signal_ts FROM dump_watchlist WHERE pair_address=?",
            (pair_address,),
        ).fetchone()

        if existing:
            cur_row = dict(existing)
            low_price = float(cur_row["low_price"])
            low_ts = int(cur_row["low_ts"])
            state = cur_row["state"]
            signal_ts = cur_row["signal_ts"]

            cur.execute(
                """
                UPDATE dump_watchlist SET
                    updated_at_ms=?, last_price=?, last_ts=?, drop_pct=?,
                    volume_m5=?, buys_m5=?, sells_m5=?,
                    peak_price=?, peak_ts=?
                WHERE pair_address=? AND peak_price < ?
                """,
                (now_ms, last_price, last_ts, drop_pct, volume_m5, buys_m5, sells_m5, peak_price, peak_ts, pair_address, peak_price),
            )

            if last_price < low_price:
                low_price = last_price
                low_ts = last_ts
                cur.execute(
                    "UPDATE dump_watchlist SET low_price=?, low_ts=? WHERE pair_address=?",
                    (low_price, low_ts, pair_address),
                )

            cur.execute(
                """
                UPDATE dump_watchlist SET
                    updated_at_ms=?, last_price=?, last_ts=?, drop_pct=?,
                    volume_m5=?, buys_m5=?, sells_m5=?
                WHERE pair_address=?
                """,
                (now_ms, last_price, last_ts, drop_pct, volume_m5, buys_m5, sells_m5, pair_address),
            )
        else:
            if drop_pct < config.DROP_THRESHOLD or liq < config.LIQ_MIN or vol < config.VOL_M5_MIN or sells < config.SELLS_MIN:
                return

            cur.execute(
                """
                INSERT INTO dump_watchlist (
                    pair_address, added_at_ms, updated_at_ms, state,
                    peak_price, peak_ts, low_price, low_ts, last_price, last_ts,
                    drop_pct, volume_m5, buys_m5, sells_m5, signal_ts, signal_price
                ) VALUES (?,?,?,'DUMPING',?,?,?,?,?,?,?,?,?,?,NULL,NULL)
                """,
                (
                    pair_address, now_ms, now_ms,
                    peak_price, peak_ts, last_price, last_ts, last_price, last_ts,
                    drop_pct, volume_m5, buys_m5, sells_m5,
                ),
            )
            low_price = last_price
            low_ts = last_ts
            state = "DUMPING"
            signal_ts = None

        self._conn.commit()

        row = cur.execute(
            "SELECT state, low_price, signal_ts FROM dump_watchlist WHERE pair_address=?",
            (pair_address,),
        ).fetchone()
        if not row:
            return
        state = row["state"]
        low_price = float(row["low_price"])
        signal_ts = row["signal_ts"]

        buys = int(buys_m5) if buys_m5 is not None else 0
        sells = int(sells_m5) if sells_m5 is not None else 0

        if state == "SIGNAL" and signal_ts is not None:
            return

        if state == "DUMPING" and len(two_rows) >= 2:
            threshold = low_price * 1.003
            last_snap = two_rows[0]
            prev_snap = two_rows[1]
            p1 = last_snap["price_usd"] if last_snap["price_usd"] is not None else 0.0
            p2 = prev_snap["price_usd"] if prev_snap["price_usd"] is not None else 0.0
            b1 = int(last_snap["txns_m5_buys"]) if last_snap["txns_m5_buys"] is not None else 0
            s1 = int(last_snap["txns_m5_sells"]) if last_snap["txns_m5_sells"] is not None else 0
            if p1 >= threshold and p2 >= threshold and b1 >= s1 * 0.8:
                cur.execute(
                    "UPDATE dump_watchlist SET state='BOTTOMING' WHERE pair_address=?",
                    (pair_address,),
                )
                state = "BOTTOMING"
                self._conn.commit()

        vol_safe = vol if vol is not None else 0.0
        prev_vol = float(two_rows[1]["volume_m5"]) if len(two_rows) >= 2 and two_rows[1]["volume_m5"] is not None else 0.0
        vol_min = max(prev_vol, 300.0)
        bounce_ok = last_price >= low_price * 1.01
        buys_gt_sells = buys > sells
        vol_ok = vol_safe >= vol_min

        if bounce_ok and buys_gt_sells and vol_ok:
            cur.execute(
                """
                UPDATE dump_watchlist SET state='SIGNAL', signal_ts=?, signal_price=?
                WHERE pair_address=? AND signal_ts IS NULL
                """,
                (last_ts, last_price, pair_address),
            )
            self._conn.commit()

    def prune_dump_watchlist(self, ttl_hours: float = config.DUMP_WATCHLIST_TTL_HOURS) -> int:
        """
        Remove expired entries (by updated_at_ms TTL) and orphaned entries (pair not in pairs).
        Returns number of rows deleted.
        """
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - int(ttl_hours * 3600 * 1000)
        cur = self._conn.cursor()

        cur.execute(
            """
            DELETE FROM dump_watchlist
            WHERE updated_at_ms < ?
            """,
            (cutoff_ms,),
        )
        ttl_cnt = cur.rowcount

        cur.execute(
            """
            DELETE FROM dump_watchlist
            WHERE NOT EXISTS (
              SELECT 1 FROM pairs p
              WHERE p.pair_address = dump_watchlist.pair_address
            )
            """
        )
        orphan_cnt = cur.rowcount

        self._conn.commit()
        return ttl_cnt + orphan_cnt

    def iterate_dump_watchlist(
        self,
        state: str | None = None,
        limit: int | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Yield dump_watchlist rows as dicts."""
        cur = self._conn.cursor()
        sql = "SELECT * FROM dump_watchlist WHERE 1=1"
        params: list[Any] = []
        if state:
            sql += " AND state = ?"
            params.append(state)
        sql += " ORDER BY updated_at_ms DESC"
        if limit is not None and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        cur.execute(sql, params)
        for row in cur:
            yield dict(row)

    # --- Price history (from snapshots; no %change) ---

    def fetch_price_history(
        self,
        pair_address: str,
        since_ts: int | None = None,
    ) -> list[tuple[int, float]]:
        """Return list of (ts, price) from snapshots for pair_address. ts = snapshot_ts (unix ms)."""
        cur = self._conn.cursor()
        sql = """
            SELECT snapshot_ts, price_usd
            FROM snapshots
            WHERE pair_address = ? AND price_usd IS NOT NULL AND price_usd > 0
        """
        params: list[Any] = [pair_address]
        if since_ts is not None:
            sql += " AND snapshot_ts >= ?"
            params.append(since_ts)
        sql += " ORDER BY snapshot_ts ASC"
        cur.execute(sql, params)
        return [(int(r["snapshot_ts"]), float(r["price_usd"])) for r in cur]

    def fetch_latest_price(self, pair_address: str) -> float | None:
        """Return latest price_usd for pair (from pairs table or last snapshot)."""
        cur = self._conn.cursor()
        row = cur.execute(
            "SELECT price_usd FROM pairs WHERE pair_address = ?",
            (pair_address,),
        ).fetchone()
        if row and row["price_usd"] is not None:
            return float(row["price_usd"])
        row = cur.execute(
            """
            SELECT price_usd FROM snapshots
            WHERE pair_address = ? AND price_usd IS NOT NULL AND price_usd > 0
            ORDER BY snapshot_ts DESC LIMIT 1
            """,
            (pair_address,),
        ).fetchone()
        if row:
            return float(row["price_usd"])
        return None

    def fetch_ath_price(
        self,
        pair_address: str,
        since_ts: int | None = None,
    ) -> float | None:
        """Return max(price_usd) from snapshots for pair_address (life of pair if since_ts given)."""
        cur = self._conn.cursor()
        sql = """
            SELECT MAX(price_usd) AS ath
            FROM snapshots
            WHERE pair_address = ? AND price_usd IS NOT NULL AND price_usd > 0
        """
        params: list[Any] = [pair_address]
        if since_ts is not None:
            sql += " AND snapshot_ts >= ?"
            params.append(since_ts)
        cur.execute(sql, params)
        row = cur.fetchone()
        if row and row["ath"] is not None:
            return float(row["ath"])
        return None

    # --- Strategy layer tables ---

    def ensure_strategy_schema(self) -> None:
        """Create strategy_decisions and signal_cooldowns tables and indexes if missing."""
        cur = self._conn.cursor()
        cur.executescript(
            SCHEMA_STRATEGY_DECISIONS
            + IDX_STRATEGY_DECISIONS_PAIR
            + IDX_STRATEGY_DECISIONS_DECIDED
            + SCHEMA_SIGNAL_COOLDOWNS
        )
        self._conn.commit()

    def insert_strategy_decision(
        self,
        pair_address: str,
        decision: str,
        current_price: float | None,
        ath_price: float | None,
        drop_from_ath: float | None,
        reasons_json: str | None = None,
    ) -> None:
        """Append one strategy decision row."""
        cur = self._conn.cursor()
        decided_at = int(time.time() * 1000)
        cur.execute(
            """
            INSERT INTO strategy_decisions
            (pair_address, decided_at, decision, current_price, ath_price, drop_from_ath, reasons_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (pair_address, decided_at, decision, current_price, ath_price, drop_from_ath, reasons_json),
        )
        self._conn.commit()

    def get_last_signal_at(self, pair_address: str) -> int | None:
        """Return last_signal_at (unix ms) for pair from signal_cooldowns, or None."""
        cur = self._conn.cursor()
        row = cur.execute(
            "SELECT last_signal_at FROM signal_cooldowns WHERE pair_address = ?",
            (pair_address,),
        ).fetchone()
        if row:
            return int(row["last_signal_at"])
        return None

    def set_signal_cooldown(self, pair_address: str) -> None:
        """Set or update last_signal_at for pair (now, unix ms)."""
        cur = self._conn.cursor()
        now_ms = int(time.time() * 1000)
        cur.execute(
            "INSERT OR REPLACE INTO signal_cooldowns (pair_address, last_signal_at) VALUES (?, ?)",
            (pair_address, now_ms),
        )
        self._conn.commit()

    def close(self) -> None:
        """Close DB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
