"""Shared helpers for CLI entry points."""

from __future__ import annotations

import signal
import sys
import threading
from collections.abc import Callable
from types import FrameType

from retail_platform.config.settings import KafkaSettings, LoggingSettings
from retail_platform.contracts.catalog import TopicCatalog
from retail_platform.errors import RetailPlatformError
from retail_platform.observability.logging import configure_logging, get_logger

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 2


def init_process(service: str, *, logs_to_stderr: bool = False) -> None:
    stream = sys.stderr if logs_to_stderr else sys.stdout
    configure_logging(service=service, settings=LoggingSettings(), stream=stream)


def load_catalog(settings: KafkaSettings) -> TopicCatalog:
    return TopicCatalog.load(settings.topics_file, settings.schemas_dir)


class ShutdownSignal:
    """Thread-safe flag set on SIGINT/SIGTERM so loops can stop gracefully."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def install(self) -> ShutdownSignal:
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)
        return self

    def _handle(self, signum: int, _frame: FrameType | None) -> None:
        get_logger(__name__).info("shutdown_requested", signal=signal.Signals(signum).name)
        self._event.set()

    def set(self) -> None:
        self._event.set()

    @property
    def requested(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float) -> bool:
        """Sleep up to `timeout` seconds; returns True early if shutdown was requested."""
        return self._event.wait(timeout)


def run_main(service: str, body: Callable[[], int], *, logs_to_stderr: bool = False) -> int:
    """Process boundary: the only place a broad exception handler is allowed."""
    init_process(service, logs_to_stderr=logs_to_stderr)
    log = get_logger(service)
    try:
        return body()
    except RetailPlatformError as exc:
        # Expected, already-explained failure: log the message without a stack trace.
        log.error("fatal_error", error=str(exc), error_type=type(exc).__name__)  # noqa: TRY400
        return EXIT_FAILURE
    except KeyboardInterrupt:
        log.info("interrupted")
        return EXIT_OK
    except Exception:  # process boundary: log with stack trace and exit non-zero
        log.exception("unhandled_error")
        return EXIT_FAILURE
