"""
Azure Monitor integration for the e-commerce orders pipeline.

Replaces AWS CloudWatch custom metrics, CloudWatch Logs structured logging,
and X-Ray tracing with Azure Application Insights via the
``azure-monitor-opentelemetry-exporter`` / ``opencensus-ext-azure`` SDK.

Usage
-----
The module is intentionally no-op when the environment variable
``APPLICATIONINSIGHTS_CONNECTION_STRING`` is not set, so the pipeline
continues to work in local development without any Azure credentials.

    from monitoring.azure_monitor import get_logger, track_pipeline_run, track_quality_check, track_metric

    logger = get_logger("ingest")
    logger.info("Starting ingest pipeline")

    track_pipeline_run("ingest", status="success", duration_seconds=12.4, record_count=500)
    track_quality_check("null_check_order_value", result="PASS", details="0 nulls found")
    track_metric("null_rate_percent", value=0.0, properties={"column": "order_value"})
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Generator

# ── Optional Azure SDK import ─────────────────────────────────────────────────
# The SDK is only required when APPLICATIONINSIGHTS_CONNECTION_STRING is set.
# If the package is absent we degrade gracefully to local-only logging.

_AZURE_AVAILABLE = False
_azure_logger_handler = None

try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
    from opencensus.ext.azure import metrics_exporter
    from opencensus.stats import aggregation as aggregation_module
    from opencensus.stats import measure as measure_module
    from opencensus.stats import stats as stats_module
    from opencensus.stats import view as view_module
    from opencensus.tags import tag_map as tag_map_module

    _AZURE_AVAILABLE = True
except ImportError:
    pass  # SDK absent – local development mode

_CONNECTION_STRING: str = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING", "")

# ── Module-level logger ────────────────────────────────────────────────────────

_root_logger = logging.getLogger("ecommerce_pipeline")
if not _root_logger.handlers:
    _stream_handler = logging.StreamHandler()
    _stream_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s – %(message)s")
    )
    _root_logger.addHandler(_stream_handler)
    _root_logger.setLevel(logging.DEBUG)

# Attach Azure App Insights handler when credentials are available
if _AZURE_AVAILABLE and _CONNECTION_STRING:
    try:
        _azure_logger_handler = AzureLogHandler(connection_string=_CONNECTION_STRING)
        _azure_logger_handler.setLevel(logging.DEBUG)
        _root_logger.addHandler(_azure_logger_handler)
        _root_logger.debug(
            "Azure Application Insights log handler attached",
            extra={"custom_dimensions": {"event_type": "sdk_init"}},
        )
    except Exception as exc:  # noqa: BLE001
        _root_logger.warning("Failed to attach AzureLogHandler: %s", exc)
elif not _CONNECTION_STRING:
    _root_logger.debug(
        "APPLICATIONINSIGHTS_CONNECTION_STRING not set – Azure telemetry disabled (local mode)"
    )
elif not _AZURE_AVAILABLE:
    _root_logger.warning(
        "opencensus-ext-azure package not installed. "
        "Install with: pip install opencensus-ext-azure"
    )


def get_logger(pipeline_name: str) -> logging.Logger:
    """Return a child logger scoped to *pipeline_name*.

    All records are forwarded to the Azure App Insights handler when configured.
    """
    return _root_logger.getChild(pipeline_name)


# ── Structured event helpers ───────────────────────────────────────────────────

def _emit(logger: logging.Logger, level: int, message: str, extra_dims: dict[str, Any]) -> None:
    """Emit a log record with Azure custom_dimensions attached."""
    logger.log(level, message, extra={"custom_dimensions": extra_dims})


def track_pipeline_run(
    pipeline: str,
    *,
    status: str,
    duration_seconds: float,
    record_count: int = 0,
    extra: dict[str, Any] | None = None,
) -> None:
    """Emit a structured pipeline-run event.

    Replaces: AWS CloudWatch Logs structured JSON event + CloudWatch metric on
    ``GlueJobRunTime`` / ``LambdaDuration``.

    Parameters
    ----------
    pipeline:
        One of ``"ingest"`` or ``"transform"``.
    status:
        ``"success"`` or ``"error"``.
    duration_seconds:
        Wall-clock duration of the pipeline step.
    record_count:
        Number of records processed.
    extra:
        Additional key-value pairs merged into custom_dimensions.
    """
    log = get_logger(pipeline)
    dims: dict[str, Any] = {
        "event_type": "pipeline_run",
        "pipeline": pipeline,
        "status": status,
        "duration_seconds": round(duration_seconds, 3),
        "record_count": record_count,
    }
    if extra:
        dims.update(extra)

    level = logging.ERROR if status == "error" else logging.INFO
    _emit(log, level, f"[{pipeline}] run finished: status={status} duration={duration_seconds:.1f}s records={record_count}", dims)

    # Also push a numeric metric for duration
    track_metric(
        "pipeline_duration_seconds",
        value=duration_seconds,
        properties={"pipeline": pipeline, "status": status},
    )
    track_metric(
        "records_processed",
        value=float(record_count),
        properties={"pipeline": pipeline},
    )


def track_quality_check(
    check_name: str,
    *,
    result: str,
    details: str = "",
    pipeline: str = "quality_checks",
) -> None:
    """Emit a structured quality-check result event.

    Replaces: AWS CloudWatch Logs metric filter on quality check output +
    CloudWatch custom metric ``QualityCheckFailCount``.

    Parameters
    ----------
    check_name:
        E.g. ``"null_check_order_value"``, ``"duplicate_check"``.
    result:
        ``"PASS"`` or ``"FAIL"``.
    details:
        Human-readable summary (counts, thresholds, etc.).
    """
    log = get_logger(pipeline)
    dims: dict[str, Any] = {
        "event_type": "quality_check",
        "pipeline": pipeline,
        "check_name": check_name,
        "result": result,
        "details": details,
    }
    level = logging.WARNING if result == "FAIL" else logging.INFO
    _emit(log, level, f"[quality_check] {check_name}: {result} – {details}", dims)


def track_pipeline_error(
    pipeline: str,
    *,
    error_message: str,
    stage: str = "",
    extra: dict[str, Any] | None = None,
) -> None:
    """Emit a structured pipeline-error event.

    This is the event that triggers the ``alert-pipeline-failure`` alert rule.

    Replaces: AWS CloudWatch Logs metric filter on ERROR keyword +
    CloudWatch Alarm on that metric.
    """
    log = get_logger(pipeline)
    dims: dict[str, Any] = {
        "event_type": "pipeline_error",
        "pipeline": pipeline,
        "stage": stage,
        "error_message": error_message,
    }
    if extra:
        dims.update(extra)
    _emit(log, logging.ERROR, f"[{pipeline}] ERROR in stage='{stage}': {error_message}", dims)


def track_metric(
    metric_name: str,
    *,
    value: float,
    properties: dict[str, str] | None = None,
) -> None:
    """Emit a custom numeric metric to Application Insights AppMetrics table.

    Replaces: AWS CloudWatch ``put_metric_data`` calls.

    When the Azure SDK is unavailable the metric is only written to the local
    log as a DEBUG record.

    Parameters
    ----------
    metric_name:
        E.g. ``"null_rate_percent"``, ``"records_processed"``.
    value:
        The numeric value to record.
    properties:
        Optional string key-value labels (become searchable dimensions in
        Log Analytics KQL queries).
    """
    props = properties or {}
    _root_logger.debug(
        "METRIC %s = %s %s",
        metric_name,
        value,
        props,
        extra={
            "custom_dimensions": {
                "event_type": "metric",
                "metric_name": metric_name,
                "metric_value": value,
                **{f"dim_{k}": v for k, v in props.items()},
            }
        },
    )

    # When opencensus is available, also push via the metrics exporter
    if _AZURE_AVAILABLE and _CONNECTION_STRING:
        try:
            import opencensus.ext.azure.metrics_exporter as _me  # noqa: PLC0415

            _exporter = _me.new_metrics_exporter(connection_string=_CONNECTION_STRING)

            mmap = stats_module.stats.stats_recorder.new_measurement_map()
            tmap = tag_map_module.TagMap()
            for k, v in props.items():
                tmap.insert(k, v)

            m = measure_module.MeasureFloat(metric_name, metric_name, "1")
            mmap.measure_float_put(m, value)
            mmap.record(tmap)
        except Exception:  # noqa: BLE001
            pass  # Metric loss is acceptable; do not fail the pipeline


# ── Context-manager helpers ────────────────────────────────────────────────────

@contextmanager
def pipeline_span(
    pipeline: str,
    stage: str = "",
    record_count_fn: Any = None,
) -> Generator[dict[str, Any], None, None]:
    """Context manager that times a code block and emits a pipeline-run event.

    On success: emits a ``pipeline_run`` event with status=``success``.
    On exception: emits a ``pipeline_error`` event and re-raises.

    Usage::

        with pipeline_span("ingest") as ctx:
            df = ingest_orders()
            ctx["record_count"] = len(df)

    Parameters
    ----------
    pipeline:
        Pipeline name (``"ingest"`` or ``"transform"``).
    stage:
        Optional sub-stage label for finer-grained error attribution.
    record_count_fn:
        Optional zero-argument callable that returns the record count; called
        after the block completes successfully.
    """
    ctx: dict[str, Any] = {"record_count": 0}
    start = time.perf_counter()
    try:
        yield ctx
        duration = time.perf_counter() - start
        count = ctx["record_count"]
        if callable(record_count_fn):
            count = record_count_fn()
        track_pipeline_run(pipeline, status="success", duration_seconds=duration, record_count=count)
    except Exception as exc:  # noqa: BLE001
        duration = time.perf_counter() - start
        track_pipeline_error(pipeline, error_message=str(exc), stage=stage)
        track_pipeline_run(pipeline, status="error", duration_seconds=duration, record_count=0)
        raise
