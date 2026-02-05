"""CLI: collect (tokens/pairs), export (json/csv), check (self-check)."""

import argparse
import csv
import json
import signal
import sys
import time
from pathlib import Path

from dexscreener_screener import config
from dexscreener_screener.client import DexScreenerClient
from dexscreener_screener.logging_setup import get_logger, setup_logging
from dexscreener_screener.models import PairSnapshot, from_api_pair
from dexscreener_screener.pipeline import Collector, parse_addresses_input
from dexscreener_screener.storage import Database
from dexscreener_screener.strategy import run_strategy_once

setup_logging()
logger = get_logger(__name__)


def cmd_self_check(args: argparse.Namespace) -> int:
    """
    Self-check DB invariants: only pairs <24h, no snapshots for old pairs, no orphan tokens.
    Exit 0 if OK, 2 if FAIL. With --fix runs prune_by_pair_age(24) and re-checks.
    """
    db_path = args.db or config.DEFAULT_DB
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        return 2
    db = Database(db_path)
    try:
        old_pairs, old_snapshots, orphan_tokens = db.self_check_invariants()
        ok = old_pairs == 0 and old_snapshots == 0 and orphan_tokens == 0

        if ok:
            print("SELF-CHECK OK")
        else:
            print("SELF-CHECK FAIL")
        print("counts: old_pairs=%s, old_pairs_snapshots=%s, orphan_tokens=%s" % (old_pairs, old_snapshots, orphan_tokens))

        if not ok and getattr(args, "fix", False):
            s_cnt, p_cnt, t_cnt = db.prune_by_pair_age(max_age_hours=config.SELF_CHECK_AGE_HOURS, dry_run=False, vacuum=False)
            print("FIX APPLIED: prune_by_pair_age(max_age_hours=24) => snapshots=%s pairs=%s tokens=%s" % (s_cnt, p_cnt, t_cnt))
            old_pairs, old_snapshots, orphan_tokens = db.self_check_invariants()
            print("counts: old_pairs=%s, old_pairs_snapshots=%s, orphan_tokens=%s" % (old_pairs, old_snapshots, orphan_tokens))
            ok = old_pairs == 0 and old_snapshots == 0 and orphan_tokens == 0

        db.close()
        return 0 if ok else 2
    except Exception as e:
        logger.exception("self-check failed: %s", e)
        db.close()
        return 2


def cmd_check(args: argparse.Namespace) -> int:
    """
    Self-check: API -> normalization -> SQLite -> read -> serialization.
    Uses DexScreenerClient, PairSnapshot, Database. Non-zero exit on error.
    """
    logger.info("Check: starting full-cycle smoke (API -> normalize -> SQLite -> read -> serialize)")

    timeout_sec = getattr(args, "timeout", config.CHECK_TIMEOUT_SEC)
    max_retries = getattr(args, "max_retries", config.CHECK_MAX_RETRIES)
    rate_limit_rps = getattr(args, "rate_limit_rps", config.CHECK_RATE_LIMIT_RPS)

    logger.info("Check: calling DexScreener API for one pair")
    client = DexScreenerClient(
        timeout_sec=timeout_sec,
        max_retries=max_retries,
        rate_limit_rps=rate_limit_rps,
    )
    raw_pairs = client.get_pairs_by_pair_addresses([config.CHECK_PAIR_ADDRESS])
    if not raw_pairs:
        logger.error("Check: API returned no pairs")
        return 1
    pair_dict = raw_pairs[0]
    if not pair_dict.get("pairAddress") or not pair_dict.get("baseToken"):
        logger.error("Check: API response missing pairAddress or baseToken")
        return 1
    logger.info("Check: API OK, pair_address=%s...", pair_dict.get("pairAddress", "")[:20])

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


def cmd_prune(args: argparse.Namespace) -> int:
    """Prune pairs older than max-age (by pair_created_at_ms) and orphan tokens."""
    db_path = args.db or config.DEFAULT_DB
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        return 1
    db = Database(db_path)
    try:
        s_cnt, p_cnt, t_cnt = db.prune_by_pair_age(
            max_age_hours=args.max_age_hours,
            dry_run=args.dry_run,
            vacuum=args.vacuum,
        )
    except Exception as e:
        logger.error("Prune failed: %s", e)
        db.close()
        return 1
    db.close()
    if args.dry_run:
        logger.info("prune (dry-run): would delete snapshots=%s pairs=%s tokens=%s", s_cnt, p_cnt, t_cnt)
    else:
        logger.info("prune: deleted snapshots=%s pairs=%s tokens=%s", s_cnt, p_cnt, t_cnt)
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    """Collect pairs by --tokens or --pairs, then optional auto-prune."""
    db_path = args.db or config.DEFAULT_DB
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

    if not getattr(args, "no_prune", False):
        try:
            max_h = getattr(args, "prune_max_age_hours", None) or config.DEFAULT_PRUNE_MAX_AGE_HOURS
            s_cnt, p_cnt, t_cnt = db.prune_by_pair_age(max_age_hours=max_h, dry_run=False, vacuum=False)
            logger.info("auto-prune: snapshots=%s pairs=%s tokens=%s", s_cnt, p_cnt, t_cnt)
            dw_cnt = db.prune_dump_watchlist(ttl_hours=config.DUMP_WATCHLIST_TTL_HOURS)
            if dw_cnt:
                logger.info("dump-watchlist prune: removed %s", dw_cnt)
        except Exception as e:
            logger.warning("auto-prune skipped: %s", e)

    db.close()
    logger.info("Done: %s pair(s) written, %s error(s)", processed, errors)
    return 0 if errors == 0 else 0


def cmd_collect_new(args: argparse.Namespace) -> int:
    """
    Continuous collection of new pairs: token-profiles -> token addresses -> pairs -> dedup -> persist.
    Exits only on SIGINT (Ctrl+C). Use --interval-sec 60 to respect token-profiles rate limit (60/min).
    """
    db_path = args.db or config.DEFAULT_DB
    interval_sec = getattr(args, "interval_sec", config.COLLECT_NEW_INTERVAL_SEC)
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
            if not getattr(args, "no_prune", False):
                try:
                    max_h = getattr(args, "prune_max_age_hours", config.DEFAULT_PRUNE_MAX_AGE_HOURS)
                    s_cnt, p_cnt, t_cnt = db.prune_by_pair_age(max_age_hours=max_h, dry_run=False, vacuum=False)
                    logger.info("auto-prune: snapshots=%s pairs=%s tokens=%s", s_cnt, p_cnt, t_cnt)
                    dw_cnt = db.prune_dump_watchlist(ttl_hours=config.DUMP_WATCHLIST_TTL_HOURS)
                    if dw_cnt:
                        logger.info("dump-watchlist prune: removed %s", dw_cnt)
                except Exception as e:
                    logger.warning("auto-prune skipped: %s", e)
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


def cmd_strategy(args: argparse.Namespace) -> int:
    """
    Run strategy screener (ATH-based drawdown). --once or --loop N.
    Output: WATCHLIST (pair, drop_from_ath, liq, vol, txns); SIGNAL (pair, drop_from_ath, ath_price, current_price, DexScreener link).
    """
    db_path = args.db or config.DEFAULT_DB
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        return 1
    db = Database(db_path)
    try:
        loop_interval = getattr(args, "loop", None)
        if loop_interval is not None:
            interval = max(1, int(loop_interval))
            shutdown = [False]

            def _on_sigint(signum, frame):
                if shutdown[0]:
                    sys.exit(1)
                shutdown[0] = True
                logger.info("SIGINT received, finishing cycle then exiting")

            signal.signal(signal.SIGINT, _on_sigint)
            while not shutdown[0]:
                watchlist, signals = run_strategy_once(db)
                _print_strategy_output(watchlist, signals)
                if shutdown[0]:
                    break
                time.sleep(interval)
        else:
            watchlist, signals = run_strategy_once(db)
            _print_strategy_output(watchlist, signals)
        return 0
    finally:
        db.close()


def _print_strategy_output(
    watchlist: list[dict],
    signals: list[dict],
) -> None:
    """Print WATCHLIST and SIGNAL lines."""
    print("--- WATCHLIST ---")
    if not watchlist:
        print("(none)")
    else:
        fmt = "%-44s %7s %12s %12s %6s"
        print(fmt % ("pair", "drop%", "liq", "vol", "txns"))
        for e in watchlist:
            print(
                fmt
                % (
                    (e.get("pair_address") or "")[:44],
                    "%.1f" % (e.get("drop_from_ath") or 0),
                    "%.0f" % (e.get("liquidity_usd") or 0),
                    "%.0f" % (e.get("volume_h24") or 0),
                    e.get("txns_h24") or 0,
                )
            )
    print("--- SIGNAL ---")
    if not signals:
        print("(none)")
    else:
        for e in signals:
            pair = (e.get("pair_address") or "")[:44]
            drop = "%.1f" % (e.get("drop_from_ath") or 0)
            ath = "%.6g" % (e.get("ath_price") or 0)
            cur = "%.6g" % (e.get("current_price") or 0)
            url = e.get("url") or ""
            print("pair=%s drop_from_ath=%s%% ath_price=%s current_price=%s %s" % (pair, drop, ath, cur, url))
    print("---")


def cmd_dump_watchlist(args: argparse.Namespace) -> int:
    """List dump watchlist entries (pair_address, state, drop_pct, peak_price, low_price, last_price, updated_at_ms, signal_price)."""
    db_path = args.db or config.DEFAULT_DB
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        return 1
    db = Database(db_path)
    state = getattr(args, "state", None)
    limit = getattr(args, "limit", 50)
    rows = list(db.iterate_dump_watchlist(state=state, limit=limit))
    db.close()
    if not rows:
        print("No dump watchlist entries")
        return 0
    cols = ["pair_address", "state", "drop_pct", "peak_price", "low_price", "last_price", "updated_at_ms", "signal_price"]
    fmt = "%-44s %-9s %7s %12s %12s %12s %14s %12s"
    print(fmt % tuple(cols))
    for r in rows:
        print(
            fmt
            % (
                (r.get("pair_address") or "")[:44],
                r.get("state") or "",
                "%.1f" % (r.get("drop_pct") or 0),
                "%.6g" % (r.get("peak_price") or 0),
                "%.6g" % (r.get("low_price") or 0),
                "%.6g" % (r.get("last_price") or 0),
                r.get("updated_at_ms") or "",
                "%.6g" % (r.get("signal_price") or 0) if r.get("signal_price") is not None else "",
            )
        )
    return 0


def cmd_dump_watchlist_export(args: argparse.Namespace) -> int:
    """Export dump_watchlist to JSON or CSV."""
    db_path = args.db or config.DEFAULT_DB
    if not Path(db_path).exists():
        logger.error("Database not found: %s", db_path)
        return 1
    db = Database(db_path)
    state = getattr(args, "state", None)
    rows = list(db.iterate_dump_watchlist(state=state))
    db.close()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    export_format = (args.format or "json").lower()
    if export_format == "json":
        data = [_row_to_export_dict(r) for r in rows]
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    elif export_format == "csv":
        if not rows:
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
        return 1
    logger.info("Exported %s dump_watchlist row(s) to %s (%s)", len(rows), out_path, export_format)
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Export snapshots/pairs/tokens from DB to JSON or CSV."""
    db_path = args.db or config.DEFAULT_DB
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
    """
    CLI entrypoint: parse args, dispatch to command handlers.
    Commands: collect, collect-new, prune, export, dump-watchlist, dump-watchlist-export, self-check, check.
    """
    parser = argparse.ArgumentParser(
        prog="dexscreener_screener",
        description="DexScreener Screener v1: collect Solana pair data and export to JSON/CSV.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect", help="Collect pairs from DexScreener API")
    collect_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path (default: %s)" % config.DEFAULT_DB)
    collect_parser.add_argument("--tokens", metavar="FILE_OR_CSV", help="Token addresses file or comma-separated list")
    collect_parser.add_argument("--pairs", metavar="FILE_OR_CSV", help="Pair addresses file or comma-separated list")
    collect_parser.add_argument("--timeout", type=float, default=config.DEFAULT_TIMEOUT_SEC, help="HTTP timeout seconds")
    collect_parser.add_argument("--max-retries", type=int, default=config.DEFAULT_MAX_RETRIES, help="Max HTTP retries")
    collect_parser.add_argument("--rate-limit-rps", type=float, default=config.DEFAULT_RATE_LIMIT_RPS, help="Max requests per second")
    collect_parser.add_argument("--no-prune", action="store_true", help="Disable auto-prune after successful collect")
    collect_parser.add_argument("--prune-max-age-hours", type=float, default=config.DEFAULT_PRUNE_MAX_AGE_HOURS, help="Max age in hours for auto-prune (default 24)")
    collect_parser.set_defaults(func=cmd_collect)

    collect_new_parser = subparsers.add_parser(
        "collect-new",
        help="Continuous collection of new pairs from token-profiles (Solana); exit with Ctrl+C",
    )
    collect_new_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path (default: %s)" % config.DEFAULT_DB)
    collect_new_parser.add_argument(
        "--interval-sec",
        type=float,
        default=config.COLLECT_NEW_INTERVAL_SEC,
        help="Seconds between cycles (default 60; token-profiles rate limit 60/min)",
    )
    collect_new_parser.add_argument(
        "--limit-per-cycle",
        type=int,
        default=None,
        metavar="N",
        help="Max token candidates per cycle (optional)",
    )
    collect_new_parser.add_argument("--timeout", type=float, default=config.DEFAULT_TIMEOUT_SEC, help="HTTP timeout seconds")
    collect_new_parser.add_argument("--max-retries", type=int, default=config.DEFAULT_MAX_RETRIES, help="Max HTTP retries")
    collect_new_parser.add_argument("--rate-limit-rps", type=float, default=config.DEFAULT_RATE_LIMIT_RPS, help="Max requests per second")
    collect_new_parser.add_argument("--no-prune", action="store_true", help="Disable auto-prune after each cycle")
    collect_new_parser.add_argument("--prune-max-age-hours", type=float, default=config.DEFAULT_PRUNE_MAX_AGE_HOURS, help="Max age in hours for auto-prune (default 24)")
    collect_new_parser.set_defaults(func=cmd_collect_new)

    prune_parser = subparsers.add_parser("prune", help="Remove pairs older than N hours (by pair_created_at_ms) and orphan tokens")
    prune_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path (default: %s)" % config.DEFAULT_DB)
    prune_parser.add_argument("--max-age-hours", type=float, default=config.DEFAULT_PRUNE_MAX_AGE_HOURS, help="Delete pairs older than N hours (default 24)")
    prune_parser.add_argument("--dry-run", action="store_true", help="Only report what would be deleted")
    prune_parser.add_argument("--vacuum", action="store_true", help="Run VACUUM after pruning")
    prune_parser.set_defaults(func=cmd_prune)

    export_parser = subparsers.add_parser("export", help="Export data from SQLite to JSON or CSV")
    export_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path (default: %s)" % config.DEFAULT_DB)
    export_parser.add_argument("--format", choices=["json", "csv"], required=True, help="Output format")
    export_parser.add_argument("--out", required=True, help="Output file path")
    export_parser.add_argument("--table", choices=["snapshots", "pairs", "tokens"], default="snapshots", help="Table to export")
    export_parser.set_defaults(func=cmd_export)

    dump_watchlist_parser = subparsers.add_parser("dump-watchlist", help="View dump watchlist entries")
    dump_watchlist_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path")
    dump_watchlist_parser.add_argument("--state", choices=["DUMPING", "BOTTOMING", "SIGNAL"], help="Filter by state")
    dump_watchlist_parser.add_argument("--limit", type=int, default=50, help="Max rows (default 50)")
    dump_watchlist_parser.set_defaults(func=cmd_dump_watchlist)

    dump_export_parser = subparsers.add_parser("dump-watchlist-export", help="Export dump_watchlist to JSON or CSV")
    dump_export_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path")
    dump_export_parser.add_argument("--format", choices=["json", "csv"], required=True, help="Output format")
    dump_export_parser.add_argument("--out", required=True, help="Output file path")
    dump_export_parser.add_argument("--state", choices=["DUMPING", "BOTTOMING", "SIGNAL"], help="Filter by state")
    dump_export_parser.set_defaults(func=cmd_dump_watchlist_export)

    self_check_parser = subparsers.add_parser(
        "self-check",
        help="Check DB invariants: only pairs <24h, no old-pair snapshots, no orphan tokens; exit 0=OK, 2=FAIL",
    )
    self_check_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path (default: %s)" % config.DEFAULT_DB)
    self_check_parser.add_argument("--fix", action="store_true", help="If FAIL, run prune_by_pair_age(24) and re-check")
    self_check_parser.set_defaults(func=cmd_self_check)

    check_parser = subparsers.add_parser("check", help="Self-check full cycle: API -> normalize -> SQLite -> read -> serialize")
    check_parser.add_argument("--timeout", type=float, default=config.CHECK_TIMEOUT_SEC, help="HTTP timeout seconds")
    check_parser.add_argument("--max-retries", type=int, default=config.CHECK_MAX_RETRIES, help="Max HTTP retries")
    check_parser.add_argument("--rate-limit-rps", type=float, default=config.CHECK_RATE_LIMIT_RPS, help="Max requests per second")
    check_parser.set_defaults(func=cmd_check)

    strategy_parser = subparsers.add_parser(
        "strategy",
        help="Strategy screener (ATH-based drawdown): WATCHLIST / SIGNAL from price history",
    )
    strategy_parser.add_argument("--db", default=config.DEFAULT_DB, help="SQLite database path (default: %s)" % config.DEFAULT_DB)
    strategy_parser.add_argument("--once", action="store_true", help="Run once and exit")
    strategy_parser.add_argument("--loop", type=float, metavar="SEC", help="Run every N seconds until Ctrl+C")
    strategy_parser.set_defaults(func=cmd_strategy)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
