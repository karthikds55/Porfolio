"""
Azure Monitor client for the ecommerce orders pipeline.

Replaces the implicit AWS CloudWatch integration (boto3 / CloudWatch agent) with
explicit Azure Monitor calls using the official Azure SDK:
  - azure-monitor-ingestion  → Log Analytics Data Collection (custom tables)
  - azure-monitor-query      → KQL queries against Log Analytics
  - applicationinsights      → Application Insights custom events + metrics
  - azure-identity           → credential chain (env vars, managed identity, CLI)

Environment variables (set after running 01_setup_azure_monitoring.ps1):
  APPLICATIONINSIGHTS_CONNECTION_STRING  – App Insights connection string
  AZURE_LOG_ANALYTICS_ENDPOINT           – Data Collection Endpoint URL
  AZURE_LOG_ANALYTICS_RULE_ID            – Data Collection Rule (DCR) resource ID
  AZURE_LOG_ANALYTICS_STREAM_NAME        – Stream name, e.g. Custom-PipelineEvents_CL
  AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET  – Service principal
                                           (not needed when using managed identity)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ── Optional SDK imports – pipeline still runs if Azure SDK is not installed ──
try:
    from azure.identity import DefaultAzureCredential
    from azure.monitor.ingestion import LogsIngestionClient
    from azure.monitor.query import LogsQueryClient, LogsQueryStatus
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False
    logger.warning(
        "azure-monitor-ingestion / azure-monitor-query not installed. "
        "Install with: pip install azure-monitor-ingestion azure-monitor-query azure-identity"
    )

try:
    from opencensus.ext.azure import metrics_exporter
    from opencensus.ext.azure.log_exporter import AzureLogHandler
    from opencensus.stats import aggregation, measure, stats, view
    _OPENCENSUS_AVAILABLE = True
except ImportError:
    _OPENCENSUS_AVAILABLE = False


class AzureMonitorClient:
    """
    Thin wrapper around Azure Monitor SDK clients for pipeline telemetry.

    AWS migration equivalents
    ─────────────────────────
    boto3 CloudWatch.put_metric_data()  →  self.emit_metric()
    boto3 logs.put_log_events()         →  self.send_pipeline_event()
    CloudWatch Alarm (SNS action)       →  Azure Monitor alert rules (ARM template)
    AWS X-Ray segments                  →  App Insights dependency tracking
    """

    def __init__(
        self,
        connection_string: str | None = None,
        dce_endpoint: str | None = None,
        dcr_rule_id: str | None = None,
        stream_name: str | None = None,
    ):
        self._conn_str   = connection_string   or os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
        self._dce        = dce_endpoint        or os.getenv("AZURE_LOG_ANALYTICS_ENDPOINT", "")
        self._dcr_id     = dcr_rule_id         or os.getenv("AZURE_LOG_ANALYTICS_RULE_ID", "")
        self._stream     = stream_name         or os.getenv("AZURE_LOG_ANALYTICS_STREAM_NAME",
                                                             "Custom-PipelineEvents_CL")
        self._credential: Any = None
        self._log_client: Any = None
        self._query_client: Any = None

        if _SDK_AVAILABLE and (self._dce or self._dcr_id):
            try:
                self._credential   = DefaultAzureCredential()
                self._log_client   = LogsIngestionClient(endpoint=self._dce, credential=self._credential)
                self._query_client = LogsQueryClient(credential=self._credential)
                logger.info("AzureMonitorClient initialised with Log Analytics ingestion client.")
            except Exception as exc:
                logger.warning("Failed to initialise Azure Monitor SDK clients: %s", exc)

    # ─────────────────────────────────────────────────────────────────────────
    # Log ingestion  (AWS equiv: CloudWatch put_log_events)
    # ─────────────────────────────────────────────────────────────────────────

    def send_pipeline_event(
        self,
        stage: str,
        status: str,
        message: str,
        row_count: int = 0,
        duration_seconds: float = 0.0,
        error_details: str = "",
        run_id: str = "",
    ) -> None:
        """
        Write a structured pipeline event to the PipelineEvents_CL custom table.

        AWS equivalent: boto3 logs.put_log_events() to /ecommerce/pipeline/<stage>
        """
        event = {
            "TimeGenerated":   datetime.now(tz=timezone.utc).isoformat(),
            "Stage":           stage,
            "Status":          status,
            "Message":         message,
            "RowCount":        row_count,
            "DurationSeconds": duration_seconds,
            "ErrorDetails":    error_details,
            "RunId":           run_id,
        }

        if self._log_client is not None and self._dcr_id:
            try:
                self._log_client.upload(
                    rule_id=self._dcr_id,
                    stream_name=self._stream,
                    logs=[event],
                )
                logger.debug("Pipeline event sent to Log Analytics: %s %s", stage, status)
            except Exception as exc:
                logger.warning("Failed to send event to Log Analytics: %s", exc)
        else:
            logger.info("[AzureMonitor OFFLINE] %s", json.dumps(event))

    def send_quality_result(
        self,
        check_name: str,
        result: str,
        details: str = "",
        run_id: str = "",
    ) -> None:
        """
        Write a quality check result to QualityCheckResults_CL.

        AWS equivalent: CloudWatch metric filter on /ecommerce/pipeline/quality
        matching '[FAIL]' pattern.
        """
        record = {
            "TimeGenerated": datetime.now(tz=timezone.utc).isoformat(),
            "CheckName":     check_name,
            "Result":        result,
            "Details":       details,
            "RunId":         run_id,
        }

        quality_stream = self._stream.replace("PipelineEvents", "QualityCheckResults")

        if self._log_client is not None and self._dcr_id:
            try:
                self._log_client.upload(
                    rule_id=self._dcr_id,
                    stream_name=quality_stream,
                    logs=[record],
                )
                logger.debug("Quality result sent: %s %s", check_name, result)
            except Exception as exc:
                logger.warning("Failed to send quality result to Log Analytics: %s", exc)
        else:
            logger.info("[AzureMonitor OFFLINE] quality %s", json.dumps(record))

    # ─────────────────────────────────────────────────────────────────────────
    # Custom metrics  (AWS equiv: CloudWatch put_metric_data)
    # ─────────────────────────────────────────────────────────────────────────

    def emit_metric(self, name: str, value: float, unit: str = "Count") -> None:
        """
        Emit a custom metric to Application Insights.

        AWS equivalent: boto3 cloudwatch.put_metric_data(
            Namespace='EcommerceOrdersPipeline', MetricData=[{MetricName: name, Value: value}]
        )
        """
        if _OPENCENSUS_AVAILABLE and self._conn_str:
            try:
                mmap = stats.stats_module.new_measurement_map()
                m    = measure.MeasureFloat(name, name, unit)
                mmap.measure_float_put(m, value)
                mmap.record()
            except Exception as exc:
                logger.warning("emit_metric via opencensus failed: %s", exc)
        else:
            logger.info("[AzureMonitor OFFLINE] metric %s = %s %s", name, value, unit)

    # ─────────────────────────────────────────────────────────────────────────
    # KQL log queries  (AWS equiv: CloudWatch Logs Insights queries)
    # ─────────────────────────────────────────────────────────────────────────

    def run_kql_query(
        self,
        workspace_id: str,
        kql: str,
        timespan_hours: int = 24,
    ) -> list[dict[str, Any]]:
        """
        Execute a KQL query against Log Analytics.

        AWS equivalent: boto3 logs.start_query() + get_query_results()
        (CloudWatch Logs Insights).
        """
        from datetime import timedelta

        if self._query_client is None:
            logger.warning("Log Analytics query client not initialised.")
            return []

        try:
            response = self._query_client.query_workspace(
                workspace_id=workspace_id,
                query=kql,
                timespan=timedelta(hours=timespan_hours),
            )
            if response.status == LogsQueryStatus.SUCCESS:
                rows = response.tables[0].rows if response.tables else []
                cols = [c.name for c in response.tables[0].columns] if response.tables else []
                return [dict(zip(cols, row)) for row in rows]
            else:
                logger.warning("KQL query partial/failed: %s", response.partial_error)
                return []
        except Exception as exc:
            logger.error("KQL query error: %s", exc)
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Convenience: check if monitoring is available
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def is_available(self) -> bool:
        """True when SDK is installed and endpoint/rule ID are configured."""
        return _SDK_AVAILABLE and bool(self._dce) and bool(self._dcr_id)

    def __repr__(self) -> str:
        return (
            f"AzureMonitorClient("
            f"available={self.is_available}, "
            f"dce={self._dce!r}, "
            f"stream={self._stream!r})"
        )
