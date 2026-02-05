"""All constants and configuration for DexScreener Screener."""

# --- DexScreener API ---
BASE_URL = "https://api.dexscreener.com"
CHAIN_SOLANA = "solana"
PAIRS_CHUNK_SIZE = 20
TOKENS_CHUNK_SIZE = 30

# --- HTTP client defaults ---
DEFAULT_TIMEOUT_SEC = 10.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_BACKOFF_BASE = 0.5
DEFAULT_RATE_LIMIT_RPS = 3.0

# --- Check command (smoke / self-check) ---
CHECK_TIMEOUT_SEC = 15.0
CHECK_MAX_RETRIES = 2
CHECK_RATE_LIMIT_RPS = 2.0
CHECK_PAIR_ADDRESS = "3nMFwZXwY1s1M5s8vYAHqd4wGs4iSxXE4LRoUMMYqEgF"

# --- Database ---
DEFAULT_DB = "dexscreener.sqlite"

# --- Logging ---
LOG_DIR = "logs"
LOG_FILE = "app.log"

# --- Prune / age ---
DEFAULT_PRUNE_MAX_AGE_HOURS = 24.0
SELF_CHECK_AGE_HOURS = 24

# --- Dump watchlist ---
DUMP_WATCHLIST_TTL_HOURS = 3.0
DROP_THRESHOLD = 50.0
LIQ_MIN = 10000.0
VOL_M5_MIN = 500.0
SELLS_MIN = 5

# --- Collect-new ---
COLLECT_NEW_INTERVAL_SEC = 60.0
COLLECT_NEW_RATE_LIMIT_NOTE = "token-profiles 60/min"

# --- Storage: column name candidates for prune auto-detect ---
TS_CANDIDATES = [
    "snapshot_ts", "ts", "timestamp", "created_at", "captured_at", "observed_at",
    "created_at_ms", "timestamp_ms",
]
SNAP_PAIR_REF_CANDIDATES = ["pair_address", "pair", "pairAddress", "pair_id", "pairId"]
PAIRS_PAIR_CANDIDATES = ["pair_address", "pair", "address", "pairAddress"]
PAIRS_BASE_CANDIDATES = [
    "base_address", "base_token_address", "base_mint",
    "baseTokenAddress", "baseTokenMint", "baseToken",
]
PAIRS_QUOTE_CANDIDATES = [
    "quote_address", "quote_token_address", "quote_mint",
    "quoteTokenAddress", "quoteTokenMint", "quoteToken",
]
TOKENS_ADDR_CANDIDATES = ["address", "token_address", "mint", "token_mint"]

# --- Strategy screener (second layer): ATH-based drawdown ---
STRATEGY_MAX_AGE_HOURS = 24.0
STRATEGY_MIN_LIQ = 10_000.0
STRATEGY_MIN_VOL = 500.0
STRATEGY_MIN_TXNS = 5

# --- ATH validation (avoid single-trade spikes) ---
ATH_VALIDATE_WINDOW_SEC = 300.0  # window around ATH timestamp (half before, half after)
ATH_MIN_SNAPSHOTS_IN_WINDOW = 2  # min snapshots in window to accept ATH
ATH_MIN_TXNS_IN_WINDOW = 1       # min txns in window if txns available
ATH_MIN_VOLUME_IN_WINDOW = 0.0   # min volume in window if volume available (0 = no floor)
ATH_FALLBACK_MAX_ATTEMPTS = 10   # max candidate peaks to try when raw ATH is invalid

# --- Bootstrap mode (insufficient price history: do not REJECT, use WATCHLIST_BOOTSTRAP) ---
BOOTSTRAP_MIN_SNAPSHOTS = ATH_MIN_SNAPSHOTS_IN_WINDOW  # same threshold: skip ATH logic when below
BOOTSTRAP_MIN_LIQ = 10_000.0   # same as WL1
BOOTSTRAP_MIN_TXNS = 5         # same as WL1

# --- 3-tier watchlist drop thresholds ---
WL1_MIN_DROP = 25.0
WL2_MIN_DROP = 35.0
WL3_MIN_DROP = 45.0
SIGNAL_MIN_DROP = 50.0
SIGNAL_MAX_DROP = 60.0

# --- Market quality per watchlist level (downgrade if below) ---
WL1_MIN_TXNS = 5
WL2_MIN_TXNS = 7
WL3_MIN_TXNS = 10
WL1_MIN_LIQ = 10_000.0
WL2_MIN_LIQ = 15_000.0
WL3_MIN_LIQ = 20_000.0

TXNS_SIGNAL = 10
BUYS_MIN = 5
LIQ_SIGNAL = 5_000.0
SIGNAL_COOLDOWN_SEC = 3600

# --- Post-analysis: evaluate signals at 30/60/120 min ---
POST_HORIZONS_SEC = [1800, 3600, 7200]

# --- Trigger-based post-analysis (TP1/SL/BU) ---
TP1_PCT = 40.0
SL_PCT = -50.0
TRIGGER_EVAL_MAX_AGE_SEC = 86400   # analyze only within token life (<=24h)
TRIGGER_EVAL_MIN_SNAPSHOTS = 2
