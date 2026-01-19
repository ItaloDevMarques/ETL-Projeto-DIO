from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging(log_path: Path | None = None, level: int = logging.INFO) -> logging.Logger:

    logger = logging.getLogger("etl_vendas")
    logger.setLevel(level)
    logger.propagate = False

    # Evita handlers duplicados em execuções repetidas
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
