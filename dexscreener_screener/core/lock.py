"""
File lock for single-DB loop mode: prevent two processes from running on the same SQLite DB.
Lock file: <db_path>.lock with content "pid\\ttimestamp\\n".
Only for loop mode; smoke/debug do not use this.
"""

from __future__ import annotations

import os
import time
from pathlib import Path


def _lock_path(db_path: str) -> Path:
    return Path(db_path).with_suffix(Path(db_path).suffix + ".lock")


def _pid_alive(pid: int) -> bool:
    """True if process pid exists (Unix: kill 0; Windows: OpenProcess)."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    except Exception:
        return False


def try_acquire_db_lock(db_path: str, pid: int | None = None) -> bool:
    """
    Try to acquire lock for this DB. Create <db>.lock with pid and timestamp.
    If pid is None, use current process (os.getpid()); else use given pid (e.g. PowerShell $PID).
    If lock exists and PID is alive -> return False (refuse start).
    If lock exists and PID is dead -> overwrite (stale lock).
    Returns True if lock acquired, False if another process holds it.
    """
    lp = _lock_path(db_path)
    my_pid = pid if pid is not None else os.getpid()
    ts = int(time.time())

    if lp.exists():
        try:
            raw = lp.read_text(encoding="utf-8").strip()
            parts = raw.split("\t")
            if len(parts) >= 1 and parts[0].isdigit():
                old_pid = int(parts[0])
                if _pid_alive(old_pid):
                    return False
        except (ValueError, OSError):
            pass
        # Stale or unreadable: overwrite

    try:
        lp.parent.mkdir(parents=True, exist_ok=True)
        lp.write_text(f"{my_pid}\t{ts}\n", encoding="utf-8")
        return True
    except OSError:
        return False


def release_db_lock(db_path: str, pid: int | None = None) -> None:
    """
    Release lock: remove <db>.lock only if it contains the given pid (or current process if pid is None).
    """
    lp = _lock_path(db_path)
    if not lp.exists():
        return
    my_pid = pid if pid is not None else os.getpid()
    try:
        raw = lp.read_text(encoding="utf-8").strip()
        parts = raw.split("\t")
        if len(parts) >= 1 and parts[0].isdigit() and int(parts[0]) == my_pid:
            lp.unlink()
    except (ValueError, OSError):
        pass
