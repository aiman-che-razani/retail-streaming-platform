"""Structured JSON logging (ADR-009).

Usage::

    configure_logging(service="simulator", settings=LoggingSettings())
    log = get_logger(__name__).bind(topic="pos.transactions")
    log.info("event_produced", event_id=..., partition=3, offset=1042, status="acked")

Every line is one JSON object with `timestamp`, `level`, `service` and `event` plus bound
context. Standard-library loggers (librdkafka wrappers, snowflake connector, py4j) are routed
through the same renderer so that *all* output is machine-parseable.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from retail_platform.config.settings import LoggingSettings

# Libraries that are chatty at INFO level; kept at WARNING unless we run DEBUG.
_NOISY_LOGGERS = ("py4j", "snowflake.connector", "urllib3", "httpx", "httpcore", "botocore")


def configure_logging(service: str, settings: LoggingSettings | None = None) -> None:
    """Configure structlog and stdlib logging for a process. Call once at startup."""
    settings = settings or LoggingSettings()
    level = logging.getLevelName(settings.level)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_service(service),
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if settings.format == "json"
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    # Route `warnings.warn` (e.g. third-party deprecation notices) through the JSON renderer
    # instead of raw stderr lines. Entry points import heavy client libraries after this.
    logging.captureWarnings(True)
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(max(level, logging.WARNING))


def _add_service(service: str) -> structlog.types.Processor:
    def processor(
        _logger: Any, _method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        event_dict.setdefault("service", service)
        return event_dict

    return processor


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.stdlib.get_logger(name)
    return logger
