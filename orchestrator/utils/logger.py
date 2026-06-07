"""Logging configuration utility."""

import logging
import sys


def setup_logger(name: str = None, level: int = logging.INFO) -> logging.Logger:
    """
    Create a configured logger with consistent formatting.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name or __name__)
    
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    
    logger.setLevel(level)
    return logger