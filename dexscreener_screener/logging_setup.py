"""Unified logging setup for DexScreener Screener."""

import logging
from pathlib import Path

from dexscreener_screener import config


def setup_logging(
    level: int = logging.INFO,
    format_string: str = "%(asctime)s %(levelname)s %(name)s: %(message)s",
) -> None:
    """
    Configure root logging: one file (logs/app.log), format timestamp level module: message.
    Preserves current log style for analysis compatibility.
    """
    root = logging.getLogger()
    root.setLevel(level)
    if not root.handlers:
        log_dir = Path(config.LOG_DIR)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / config.LOG_FILE
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter(format_string))
        root.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(logging.Formatter(format_string))
        root.addHandler(sh)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return module logger (call after setup_logging for consistent config)."""
    return logging.getLogger(name)
