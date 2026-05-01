"""
Pipeline telemetry helpers – context managers and decorators that wrap pipeline
stages with Azure Monitor instrumentation.

Usage in a pipeline stage:

    from monitoring.pipeline_telemetry import PipelineRun, stage_telemetry

    with PipelineRun(run_id="abc123") as run:
        with stage_telemetry(run, "ingest") as ctx:
            df = ingest_orders()
            save_staging(df)
            ctx.set_row_count(len(df))

    # Or using the decorator form:
    @stage_telemetry_decorator(run, "transform")
    def do_transform():
        ...

AWS migration note:
  This module replaces CloudWatch Embedded Metric Format (EMF) logging that was
  used on the original AWS deployment. The context managers emit the same
  structured data but target Azure Monitor instead.
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from typing import Generator

from monitoring.azure_monitor_client import AzureMonitorClient

logger = logging.getLogger(__name__)


class StageContext:
    """Mutable context object passed into a stage block."""

    def __init__(self, stage: str):
        self.stage = stage
        self.row_count: int = 0
        self.error: Exception | None = None
        self._start: float = time.perf_counter()

    def set_row_count(self, n: int) -> None:
        self.row_count = n

    @property
    def duration_seconds(self) -> float:
        return time.perf_counter() - self._start


class PipelineRun:
    """
    Represents a single end-to-end pipeline execution.

    AWS equivalent: A CloudWatch Logs stream or X-Ray trace per pipeline invocation.
    Azure equivalent: A RunId that groups events in PipelineEvents_CL and App Insights.

    Example:
        with PipelineRun() as run:
            run.monitor  # → AzureMonitorClient
            run.run_id   # → "550e8400-e29b-..."
    """

    def __init__(
        self,
        run_id: str | None = None,
        monitor: AzureMonitorClient | None = None,
    ):
        self.run_id  = run_id or str(uuid.uuid4())
        self.monitor = monitor or AzureMonitorClient()
        self._stages: list[str] = []

    def __enter__(self) -> "PipelineRun":
        logger.info("Pipeline run started: %s", self.run_id)
        self.monitor.send_pipeline_event(
            stage="pipeline",
            status="START",
            message=f"Pipeline run {self.run_id} started",
            run_id=self.run_id,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_type is None:
            self.monitor.send_pipeline_event(
                stage="pipeline",
                status="SUCCESS",
                message=f"Pipeline run {self.run_id} completed. Stages: {', '.join(self._stages)}",
                run_id=self.run_id,
            )
            self.monitor.emit_metric("pipeline.run.status", 1.0)
            logger.info("Pipeline run complete: %s", self.run_id)
        else:
            self.monitor.send_pipeline_event(
                stage="pipeline",
                status="ERROR",
                message=f"Pipeline run {self.run_id} failed",
                error_details=f"{exc_type.__name__}: {exc_val}",
                run_id=self.run_id,
            )
            self.monitor.emit_metric("pipeline.run.status", 0.0)
            logger.error("Pipeline run failed: %s – %s: %s", self.run_id, exc_type.__name__, exc_val)
        return False  # do not suppress exceptions


@contextmanager
def stage_telemetry(
    run: PipelineRun,
    stage_name: str,
) -> Generator[StageContext, None, None]:
    """
    Context manager that times a pipeline stage and emits Azure Monitor telemetry.

    AWS equivalent: CloudWatch Embedded Metric Format (EMF) log statement +
    a CloudWatch metric filter on that log group.

    Example:
        with stage_telemetry(run, "ingest") as ctx:
            df = ingest_orders()
            ctx.set_row_count(len(df))
    """
    ctx = StageContext(stage=stage_name)
    run._stages.append(stage_name)

    run.monitor.send_pipeline_event(
        stage=stage_name,
        status="START",
        message=f"Stage '{stage_name}' started",
        run_id=run.run_id,
    )

    try:
        yield ctx

        duration = ctx.duration_seconds
        run.monitor.send_pipeline_event(
            stage=stage_name,
            status="SUCCESS",
            message=f"Stage '{stage_name}' completed – {ctx.row_count:,} rows in {duration:.2f}s",
            row_count=ctx.row_count,
            duration_seconds=duration,
            run_id=run.run_id,
        )
        run.monitor.emit_metric(f"pipeline.{stage_name}.duration_sec", duration, unit="Seconds")
        if ctx.row_count > 0:
            run.monitor.emit_metric(f"pipeline.{stage_name}.row_count", float(ctx.row_count))
        logger.info("[%s] %s – %d rows, %.2fs", stage_name.upper(), "SUCCESS", ctx.row_count, duration)

    except Exception as exc:
        duration = ctx.duration_seconds
        run.monitor.send_pipeline_event(
            stage=stage_name,
            status="ERROR",
            message=f"Stage '{stage_name}' failed after {duration:.2f}s",
            duration_seconds=duration,
            error_details=f"{type(exc).__name__}: {exc}",
            run_id=run.run_id,
        )
        run.monitor.emit_metric(f"pipeline.{stage_name}.duration_sec", duration, unit="Seconds")
        logger.error("[%s] ERROR – %s: %s", stage_name.upper(), type(exc).__name__, exc)
        raise


def quality_check_telemetry(
    run: PipelineRun,
    check_name: str,
    passed: bool,
    details: str = "",
) -> None:
    """
    Emit a single quality check result to QualityCheckResults_CL.

    AWS equivalent: CloudWatch metric filter on /ecommerce/pipeline/quality
    matching '[FAIL]' or '[PASS]' patterns.
    """
    result = "PASS" if passed else "FAIL"
    run.monitor.send_quality_result(
        check_name=check_name,
        result=result,
        details=details,
        run_id=run.run_id,
    )
    if not passed:
        run.monitor.emit_metric("pipeline.quality.fail_count", 1.0)
