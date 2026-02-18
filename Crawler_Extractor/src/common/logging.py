"""Central logging configuration."""

from __future__ import annotations

import logging
from typing import Callable, List, Optional

import structlog

_STRUCTLOG_PROCESSORS = [
    structlog.contextvars.merge_contextvars,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.JSONRenderer(),
]
_LOGGER_FACTORY = structlog.stdlib.LoggerFactory()
_STREAMLIT_WRITERS: List[Callable[[str], None]] = []
_STREAMLIT_HANDLER: Optional["_StreamlitHandler"] = None


def configure_logging(level: int = logging.INFO) -> None:
    """Configure structlog so every module shares consistent settings."""

    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=_STRUCTLOG_PROCESSORS,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=_LOGGER_FACTORY,
    )


def set_log_level(level: int) -> None:
    """Dynamically adjust logging level for both stdlib and structlog."""

    logging.getLogger().setLevel(level)
    structlog.configure(
        processors=_STRUCTLOG_PROCESSORS,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=_LOGGER_FACTORY,
    )


def register_streamlit_sink(writer: Callable[[str], None]) -> Callable[[], None]:
    """Mirror log records to Streamlit by registering a writer callback."""

    if writer not in _STREAMLIT_WRITERS:
        _STREAMLIT_WRITERS.append(writer)
    _ensure_streamlit_handler()

    def _remove() -> None:
        try:
            _STREAMLIT_WRITERS.remove(writer)
        except ValueError:
            pass

    return _remove


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Return a structured logger bound to the provided name."""

    return structlog.get_logger(name)


class _StreamlitHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if not _STREAMLIT_WRITERS:
            return
        message = self.format(record)
        for writer in list(_STREAMLIT_WRITERS):
            try:
                writer(message)
            except Exception:
                continue


def _ensure_streamlit_handler() -> None:
    global _STREAMLIT_HANDLER
    if _STREAMLIT_HANDLER is not None:
        return
    handler = _StreamlitHandler()
    handler.setLevel(logging.NOTSET)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(handler)
    _STREAMLIT_HANDLER = handler
