"""
Architectural check: import key modules and run compileall.
Run from project root: python scripts/arch_check.py
"""
import compileall
import sys
from pathlib import Path

# Project root (parent of scripts/)
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

def main() -> int:
    try:
        import dexscreener_screener  # noqa: F401
        import dexscreener_screener.cli  # noqa: F401
        from dexscreener_screener.storage.sqlite import Database  # noqa: F401
        from dexscreener_screener.strategy.engine import run_strategy_once  # noqa: F401
        from dexscreener_screener.strategy.trigger_analyzer import run_trigger_analysis  # noqa: F401
        compileall.compile_dir(str(_root), quiet=1)
        print("ARCH_CHECK: OK")
        return 0
    except Exception as exc:
        print("ARCH_CHECK: FAIL", exc)
        return 1

if __name__ == "__main__":
    sys.exit(main())
