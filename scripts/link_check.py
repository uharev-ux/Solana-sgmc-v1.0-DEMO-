"""
Link check: signal_event -> PENDING trigger row -> run_trigger_analysis -> DONE/NO_DATA.
Uses same DB API as engine (insert_signal_event, insert_trigger_eval_pending).
Run from project root: python scripts/link_check.py
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

DB_PATH = _root / "debug_link.sqlite"

def main() -> int:
    if DB_PATH.exists():
        DB_PATH.unlink()
    try:
        from dexscreener_screener.storage import Database
        from dexscreener_screener.models import TokenInfo, PairSnapshot
        from dexscreener_screener.strategy.trigger_analyzer import run_trigger_analysis

        db = Database(str(DB_PATH))
        base = TokenInfo(address="link_base", symbol="B", name="Base")
        quote = TokenInfo(address="link_quote", symbol="Q", name="Quote")
        db.upsert_token(base)
        db.upsert_token(quote)

        t0 = 1000000000000  # ms
        pair_addr = "link_pair_1"
        entry_price = 100.0
        for ts, price in [(t0, 100.0), (t0 + 1, 120.0), (t0 + 2, 140.0)]:
            snap = PairSnapshot(
                snapshot_ts=ts,
                chain_id="solana",
                dex_id="",
                pair_address=pair_addr,
                url="",
                base_token=base,
                quote_token=quote,
                price_usd=price,
                liquidity_usd=15000.0,
                volume_h24=600.0,
                txns_h24_buys=3,
                txns_h24_sells=2,
                pair_created_at_ms=t0 - 3600000,
            )
            db.upsert_pair(snap)
            db.insert_snapshot(snap)
        db._conn.commit()

        signal_id = db.insert_signal_event(
            pair_address=pair_addr,
            signal_ts=t0,
            entry_price=entry_price,
            ath_price=150.0,
            drop_from_ath=33.33,
            score=50.0,
            features_json="{}",
        )
        db.insert_trigger_eval_pending(signal_id)

        cur = db._conn.cursor()
        cur.execute(
            "SELECT status FROM signal_trigger_evaluations WHERE signal_id = ?",
            (signal_id,),
        )
        row = cur.fetchone()
        if not row or row[0] != "PENDING":
            print("LINK_CHECK: FAIL after insert_signal_event: expected PENDING row, got", row)
            db.close()
            DB_PATH.unlink(missing_ok=True)
            return 1
        print("  OK: signal_event + insert_trigger_eval_pending -> PENDING row")

        db.close()
        run_trigger_analysis(str(DB_PATH), now_ts=t0 + 10000, limit=100)

        db2 = Database(str(DB_PATH))
        cur = db2._conn.cursor()
        cur.execute(
            "SELECT status, outcome FROM signal_trigger_evaluations WHERE signal_id = ?",
            (signal_id,),
        )
        row = cur.fetchone()
        db2.close()
        if not row:
            print("LINK_CHECK: FAIL no trigger eval row after run_trigger_analysis")
            DB_PATH.unlink(missing_ok=True)
            return 1
        status, outcome = row[0], row[1]
        if status not in ("DONE", "NO_DATA"):
            print("LINK_CHECK: FAIL expected status DONE or NO_DATA, got", status)
            DB_PATH.unlink(missing_ok=True)
            return 1
        print("  OK: run_trigger_analysis -> status=%s outcome=%s" % (status, outcome))
        DB_PATH.unlink(missing_ok=True)
        print("LINK_CHECK: OK")
        return 0
    except Exception as e:
        if DB_PATH.exists():
            DB_PATH.unlink(missing_ok=True)
        print("LINK_CHECK: FAIL", e)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
