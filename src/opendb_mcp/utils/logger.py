"""
Logger utility that writes to stderr (not stdout) for stdio transport compatibility.
"""

import logging
import os
import sys
from typing import Any, Optional

# Log level mapping
LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class StderrHandler(logging.Handler):
    """Custom handler that always writes to stderr."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            sys.stderr.write(msg + "\n")
            sys.stderr.flush()
        except Exception:
            self.handleError(record)


class Logger:
    """Logger class that writes to stderr for stdio transport compatibility."""

    def __init__(self, name: str = "opendb-mcp", level: str = "info"):
        self._logger = logging.getLogger(name)

        # Set level from environment or default
        env_level = os.environ.get("LOG_LEVEL", level).lower()
        log_level = LOG_LEVELS.get(env_level, logging.INFO)
        self._logger.setLevel(log_level)

        # Remove any existing handlers
        self._logger.handlers.clear()

        # Add stderr handler
        handler = StderrHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)-5s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
        )
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

        # Prevent propagation to root logger
        self._logger.propagate = False

    def set_level(self, level: str) -> None:
        """Set the log level."""
        log_level = LOG_LEVELS.get(level.lower(), logging.INFO)
        self._logger.setLevel(log_level)

    def _format_meta(self, meta: Optional[Any]) -> str:
        """Format metadata for logging."""
        if meta is None:
            return ""

        if isinstance(meta, Exception):
            import traceback

            return "\n" + "".join(traceback.format_exception(type(meta), meta, meta.__traceback__))

        if isinstance(meta, dict):
            import json

            try:
                return " " + json.dumps(meta)
            except (TypeError, ValueError):
                return " " + str(meta)

        return " " + str(meta)

    def debug(self, message: str, meta: Optional[Any] = None) -> None:
        """Log a debug message."""
        self._logger.debug(message + self._format_meta(meta))

    def info(self, message: str, meta: Optional[Any] = None) -> None:
        """Log an info message."""
        self._logger.info(message + self._format_meta(meta))

    def warning(self, message: str, meta: Optional[Any] = None) -> None:
        """Log a warning message."""
        self._logger.warning(message + self._format_meta(meta))

    def warn(self, message: str, meta: Optional[Any] = None) -> None:
        """Alias for warning."""
        self.warning(message, meta)

    def error(self, message: str, meta: Optional[Any] = None) -> None:
        """Log an error message."""
        self._logger.error(message + self._format_meta(meta))


# Global logger instance
logger = Logger()
