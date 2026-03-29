"""
Unified Logging Configuration
===============================
Sets up a single consistent logger for the entire application.

Usage:
    from core.logging_config import get_logger
    log = get_logger("DataAgent")
    log.info("Fetching NIFTY chain...")
"""

import logging
import os
import sys
from datetime import datetime
from core.config import cfg


_INITIALISED = False


def _init_root_logger():
    """Configure the root logger once for the entire process."""
    global _INITIALISED
    if _INITIALISED:
        return
    _INITIALISED = True

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # ── Console handler ──
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(console)

    # ── File handler ──
    log_file = os.path.join(
        cfg.log_dir,
        f"nse_tools_{datetime.now().strftime('%Y%m%d')}.log"
    )
    try:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        root.addHandler(file_handler)
    except Exception:
        # Don't crash if log file can't be created
        pass


def get_logger(name: str) -> logging.Logger:
    """
    Get a named logger with consistent formatting.

    Args:
        name: Logger name (e.g., "DataAgent", "Strategy", "Backtest")

    Returns:
        Configured logging.Logger instance
    """
    _init_root_logger()
    return logging.getLogger(name)
