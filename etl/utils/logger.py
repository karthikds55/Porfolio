"""Structured logger for the ETL pipeline using loguru."""
import sys
from pathlib import Path
from loguru import logger


def setup_logger(log_dir: str = "logs", log_level: str = "INFO") -> None:
    """Configure loguru logger with console and file sinks.

    Args:
        log_dir: Directory to write log files to.
        log_level: Minimum log level to capture.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger.remove()

    logger.add(
        sys.stdout,
        level=log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    logger.add(
        Path(log_dir) / "etl_{time:YYYY-MM-DD}.log",
        level=log_level,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
            "{name}:{function}:{line} | {message}"
        ),
        rotation="1 day",
        retention="7 days",
        compression="zip",
    )


def get_logger(name: str):
    """Return a named child logger bound to a module name."""
    return logger.bind(name=name)
