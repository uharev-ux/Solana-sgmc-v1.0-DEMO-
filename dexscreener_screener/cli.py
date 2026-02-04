"""CLI: collect (tokens/pairs), export (json/csv), check (self-check)."""

import argparse
import json
import logging
import signal
import sys
import time
from pathlib import Path

from dexscreener_screener.client import DexScreenerClient
from dexscreener_screener.collector import Collector, parse_addresses_input
from dexscreener_screener.db import Database
from dexscreener_screener.models import PairSnapshot, from_api_pair

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

DEFAULT_DB = "dexscreener.sqlite"

# Known Solana pair for self-check (from DexScreener tokens API)
CHECK_PAIR_ADDRESS = "3nMFwZXwY1s1M5s8vYAHqd4wGs4iSxXE4LRoUMMYqEgF"


def cmd_check(args: argparse.Namespace) -> int:
    """
    Self-check: API -> normalization -> SQLite -> read -> serialization.
    Uses DexScreenerClient, PairSnapshot, Database. Non-zero exit on error.
    """
    logger.info("Check: starting full-cycle smoke (API -> normalize -> SQLite -> read -> serialize)")

    # 1) API
    logger.info("Check: calling DexScreener API for one pair")
    client = DexScreenerClient(
        timeout_sec=getattr(args, "timeout", 15.0),
        max_retries=getattr(args, "max_retries", 2),
        rate_limit_rps=getattr(args, "rate_limit_rps", 2.0),
    )
    raw_pairs = client.get_pairs_by_pair_addresses([CHECK_PAIR_ADDRESS])
    if not raw_pairs:
        logger.error("Check: API returned no pairs")
        return 1
    pair_dict = raw_pairs[0]
    if not pair_dict.get("pairAddress") or not pair_dict.get("baseToken"):
        logger.error("Check: API response missing pairAddress or baseToken")
        return 1
    logger.info("Check: API OK, pair_address=%s...", pair_dict.get("pairAddress", "")[:20])

    # 2) Normalization
    logger.info("Check: normalizing to PairSnapshot")
    snapshot_ts = int(time.time() * 1000)
    try:
        snapshot = from_api_pair(pair_dict, snapshot_ts)
    except Exception as e:
        logger.error("Check: normalization failed: %s", e)
        return 1
    if not isinstance(snapshot, PairSnapshot) or not snapshot.pair_address:
        logger.error("Check: invalid PairSnapshot after normalization")
        return 1
    logger.info("Check: normalization OK, pair_address=%s", snapshot.pair_address[:20] + "...")

    # 3) SQLite write (in-memory)
    logger.info("Check: writing to SQLite")
    db = Database(":memory:")
    try:
        db.upsert_token(snapshot.base_token)
        db.upsert_token(snapshot.quote_token)
        db.upsert_pair(snapshot)
        db.insert_snapshot(snapshot)
    except Exception as e:
        logger.error("Check: SQLite write failed: %s", e)
        db.close()
        return 1
    logger.info("Check: SQLite write OK")

    # 4) Read back
    logger.info("Check: reading from SQLite")
    try:
        rows = list(db.iterate_snapshots())
    except Exception as e:
        logger.error("Check: SQLite read failed: %s", e)
        db.close()
        return 1
    if not rows:
        logger.error("Check: no rows read from snapshots")
        db.close()
        return 1
    row = dict(rows[0])
    db.close()
    logger.info("Check: read OK, %s row(s)", len(rows))

    # 5) Basic serialization
    logger.info("Check: basic JSON serialization")
    try:
        payload = json.dumps(row, default=str, ensure_ascii=False)
    except Exception as e:
        logger.error("Check: serialization failed: %s", e)
        return 1
    if not payload or "pair_address" not in payload:
        logger.error("Check: serialized payload invalid")
        return 1
    logger.info("Check: serialization OK, %s bytes", len(payload))

    logger.info("Check: all steps passed")
    return 0


def _row_to_export_dict(row: dict) -> dict:
    """Convert sqlite Row-like dict for JSON (e.g. omit None or keep)."""
    return {k: (v if v is not None else None) for k, v in row.items()}


def cmd_collect(args: argparse.Namespace) -> int:
    db_path = args.db or DEFAULT_DB
    db = Database(db_path)
    client = DexScreenerClient(
        timeout_sec=args.timeout,
        max_retries=args.max_retries,
        rate_limit_rps=args.rate_limit_rps,
    )
    collector = Collector(client, db)
    if args.tokens is not None:
        addresses = parse_addresses_input(args.tokens)
        if not addresses:
            logger.error("No token addresses parsed from: %s", args.tokens)
            return 1
        processed, errors = collector.collect_for_tokens(addresses)
    elif args.pairs is not None:
        addresses = parse_addresses_input(args.pairs)
        if not addresses:
            logger.error("No pair addresses parsed from: %s", args.pairs)
            return 1
        processed, errors = collector.collect_for_pairs(addresses)
    else:
        logger.error("Specify either --tokens or --pairs")
        return 1
    db.close()
    logger.info("Done: %s pair(s) written, %s error(s)", processed, errors)
    return 0 if errors == 0 else 0


def cmd_collect_new(args: argparse.Namespace) -> int:
    """
    Continuous collection of new pairs: token-profiles -> token addresses -> pairs -> dedup -> persist.
    Exits only on SIGINT (Ctrl+C). Use --interval-sec 60 to respect token-profiles rate limit (60/min).
    """
    db_path = args.db or DEFAULT_DB
    interval_sec = getattr(args, "interval_sec", 60)
    if interval_sec < 1:
        logger.error("--interval-sec must be >= 1")
        return 1
    limit_per_cycle = getattr(args, "limit_per_cycle", None)

    db = Database(db_path)
    client = DexScreenerClient(
        timeout_sec=args.timeout,
        max_retries=args.max_retries,
        rate_limit_rps=args.rate_limit_rps,
    )
    collector = Collector(client, db)

    shutdown = False

    def _on_sigint(signum, frame):
        nonlocal shutdown
        if shutdown:
            logger.warning("Second Ctrl+C, exiting immediately")
            sys.exit(1)
        shutdown = True
        logger.info("SIGINT received, finishing current cycle then exiting")

    signal.signal(signal.SIGINT, _on_sigint)

    total_cycles = 0
    total_candidates_tokens = 0
    total_candidates_pairs = 0
    total_new = 0
    total_skipped = 0
    total_processed = 0
    total_snapshots = 0
    total_errors = 0

    cycle_num = 0
    while not shutdown:
        cycle_num += 1
        cycle_candidates_tokens = 0
        cycle_candidates_pairs = 0
        cycle_new = 0
        cycle_skipped = 0
        cycle_processed = 0
        cycle_snapshots = 0
        cycle_errors = 0

        try:
            token_addresses = client.get_latest_token_profiles()
            cycle_candidates_tokens = len(token_addresses)
            if limit_per_cycle is not None and limit_per_cycle > 0:
                token_addresses = token_addresses[:limit_per_cycle]

            if not token_addresses:
                logger.info("collect-new cycle %s: no token candidates from API", cycle_num)
            else:
                raw_pairs = client.get_pairs_by_token_addresses_batched(token_addresses)
                cycle_candidates_pairs = len(raw_pairs)
                known = db.get_known_pair_addresses()
                processed, errors, skipped = collector.collect_from_raw_pairs(raw_pairs, known)
                cycle_processed = processed
                cycle_snapshots = processed
                cycle_skipped = skipped
                cycle_new = len(raw_pairs) - skipped
                cycle_errors = errors

            total_cycles = cycle_num
            total_candidates_tokens += cycle_candidates_tokens
            total_candidates_pairs += cycle_candidates_pairs
            total_new += cycle_new
            total_skipped += cycle_skipped
            total_processed += cycle_processed
            total_snapshots += cycle_snapshots
            total_errors += cycle_errors

            logger.info(
                "collect-new cycle %s | candidates_tokens=%s candidates_pairs=%s new=%s skipped=%s processed=%s snapshots=%s errors=%s",
                cycle_num,
                cycle_candidates_tokens,
                cycle_candidates_pairs,
                cycle_new,
                cycle_skipped,
                cycle_processed,
                cycle_snapshots,
                cycle_errors,
            )
            logger.info(
                "collect-new totals | cycles=%s candidates_tokens=%s candidates_pairs=%s new=%s skipped=%s processed=%s snapshots=%s errors=%s",
                total_cycles,
                total_candidates_tokens,
                total_candidates_pairs,
                total_new,
                total_skipped,
                total_processed,
                total_snapshots,
                total_errors,
            )
        except Exception as e:
            cycle_errors = 1
            total_errors += 1
            logger.exception("collect-new cycle %s failed: %s", cycle_num, e)

        if shutdown:
            break
        time.sleep(interval_sec)

    db.close()
    logger.info(
        "collect-new stopped | total_cycles=%s total_processed=%s total_snapshots=%s total_errors=%s",
        total_cycles,
        total_processed,
        total_snapshots,
        total_errors,
    )
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    db_path = args.db or DEFAULT_DB
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        return 1
    db = Database(db_path)
    table = (args.table or "snapshots").lower()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if table == "snapshots":
        rows = list(db.iterate_snapshots())
    elif table == "pairs":
        rows = list(db.iterate_pairs())
    elif table == "tokens":
        rows = list(db.iterate_tokens())
    else:
        logger.error("Unknown table: %s (use snapshots, pairs, or tokens)", table)
        db.close()
        return 1

    export_format = (args.format or "json").lower()
    if export_format == "json":
        data = [_row_to_export_dict(r) for r in rows]
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    elif export_format == "csv":
        import csv
        if not rows:
            logger.warning("No rows to export")
            with open(out_path, "w", encoding="utf-8", newline="") as f:
                f.write("")
        else:
            headers = list(rows[0].keys())
            with open(out_path, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=headers)
                w.writeheader()
                for r in rows:
                    w.writerow({k: r.get(k) for k in headers})
    else:
        logger.error("Unknown format: %s (use json or csv)", args.format)
        db.close()
        return 1

    db.close()
    logger.info("Exported %s row(s) to %s (%s)", len(rows), out_path, export_format)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dexscreener_screener",
        description="DexScreener Screener v1: collect Solana pair data and export to JSON/CSV.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect", help="Collect pairs from DexScreener API")
    collect_parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path (default: %s)" % DEFAULT_DB)
    collect_parser.add_argument("--tokens", metavar="FILE_OR_CSV", help="Token addresses file or comma-separated list")
    collect_parser.add_argument("--pairs", metavar="FILE_OR_CSV", help="Pair addresses file or comma-separated list")
    collect_parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout seconds")
    collect_parser.add_argument("--max-retries", type=int, default=4, help="Max HTTP retries")
    collect_parser.add_argument("--rate-limit-rps", type=float, default=3.0, help="Max requests per second")
    collect_parser.set_defaults(func=cmd_collect)

    collect_new_parser = subparsers.add_parser(
        "collect-new",
        help="Continuous collection of new pairs from token-profiles (Solana); exit with Ctrl+C",
    )
    collect_new_parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path (default: %s)" % DEFAULT_DB)
    collect_new_parser.add_argument(
        "--interval-sec",
        type=float,
        default=60.0,
        help="Seconds between cycles (default 60; token-profiles rate limit 60/min)",
    )
    collect_new_parser.add_argument(
        "--limit-per-cycle",
        type=int,
        default=None,
        metavar="N",
        help="Max token candidates per cycle (optional)",
    )
    collect_new_parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout seconds")
    collect_new_parser.add_argument("--max-retries", type=int, default=4, help="Max HTTP retries")
    collect_new_parser.add_argument("--rate-limit-rps", type=float, default=3.0, help="Max requests per second")
    collect_new_parser.set_defaults(func=cmd_collect_new)

    export_parser = subparsers.add_parser("export", help="Export data from SQLite to JSON or CSV")
    export_parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path (default: %s)" % DEFAULT_DB)
    export_parser.add_argument("--format", choices=["json", "csv"], required=True, help="Output format")
    export_parser.add_argument("--out", required=True, help="Output file path")
    export_parser.add_argument("--table", choices=["snapshots", "pairs", "tokens"], default="snapshots", help="Table to export")
    export_parser.set_defaults(func=cmd_export)

    check_parser = subparsers.add_parser("check", help="Self-check full cycle: API -> normalize -> SQLite -> read -> serialize")
    check_parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
    check_parser.add_argument("--max-retries", type=int, default=2, help="Max HTTP retries")
    check_parser.add_argument("--rate-limit-rps", type=float, default=2.0, help="Max requests per second")
    check_parser.set_defaults(func=cmd_check)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
