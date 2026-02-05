"""Unified logging setup for DexScreener Screener."""

import logging


def setup_logging(
    level: int = logging.INFO,
    format_string: str = "%(asctime)s %(levelname)s %(message)s",
) -> None:
    """
    Configure root logging and third-party loggers.
    Preserves current log style for analysis compatibility.
    """
    logging.basicConfig(level=level, format=format_string)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return module logger (call after setup_logging for consistent config)."""
    return logging.getLogger(name)
