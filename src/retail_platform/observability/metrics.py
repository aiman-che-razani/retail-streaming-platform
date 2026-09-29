"""Prometheus helpers.

Metric *definitions* live next to the component that emits them (e.g. `ProducerMetrics`),
each built against an injected `CollectorRegistry` so tests can use a private registry and
processes never share hidden global state. Names follow `retail_<component>_<what>_<unit>`
and are part of the contract in docs/architecture/overview.md §8.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, start_http_server

from retail_platform.observability.logging import get_logger

log = get_logger(__name__)


def new_registry() -> CollectorRegistry:
    return CollectorRegistry(auto_describe=True)


def serve_metrics(registry: CollectorRegistry, port: int) -> None:
    """Expose `/metrics` on `port` from a daemon thread. Port 0 disables the endpoint."""
    if port == 0:
        log.info("metrics_endpoint_disabled")
        return
    start_http_server(port, registry=registry)
    log.info("metrics_endpoint_started", port=port)
