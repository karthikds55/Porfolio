# AWS → Azure Monitor Migration Guide
## Ecommerce Orders Pipeline

This document is the end-to-end reference for migrating the ecommerce orders pipeline
monitoring stack from **AWS CloudWatch** to **Azure Monitor**.

---

## Table of Contents

1. [Migration Map (AWS → Azure)](#1-migration-map)
2. [Repository Structure](#2-repository-structure)
3. [Prerequisites](#3-prerequisites)
4. [Step-by-Step Deployment](#4-step-by-step-deployment)
5. [Environment Variables for Python Telemetry](#5-environment-variables)
6. [KQL Query Reference](#6-kql-query-reference)
7. [Alert Rules Reference](#7-alert-rules-reference)
8. [Decommissioning AWS Resources](#8-decommissioning-aws-resources)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Migration Map

| AWS Resource | Azure Equivalent | File |
|---|---|---|
| CloudWatch Log Group `/ecommerce/pipeline/ingest` | Log Analytics Workspace custom table `PipelineEvents_CL` | `arm_templates/log_analytics_workspace.json` |
| CloudWatch Log Group `/ecommerce/pipeline/transform` | `PipelineEvents_CL` (Stage = 'transform') | same |
| CloudWatch Log Group `/ecommerce/pipeline/quality` | `QualityCheckResults_CL` | same |
| CloudWatch Metric Filter `[FAIL]` | Log-based Alert Rule `alert-quality-check-failures` | `arm_templates/alert_rules.json` |
| CloudWatch Metric Filter `IngestErrors` | Alert Rule `alert-pipeline-ingest-errors` | same |
| CloudWatch Alarm `PipelineIngestErrors` | Scheduled Query Alert `alert-pipeline-ingest-errors` | same |
| CloudWatch Alarm `DataQualityFailures` | Scheduled Query Alert `alert-quality-check-failures` | same |
| CloudWatch Alarm `PipelineStale` (treat_missing_data=breaching) | Scheduled Query Alert `alert-pipeline-stale` | same |
| SNS Topic `pipeline-alerts` | Action Groups `ag-pipeline-critical` + `ag-pipeline-warning` | `config/action_groups.json` |
| CloudWatch Dashboard `EcommerceOrdersPipeline` | Azure Portal Dashboard `dash-ecommerce-pipeline-prod` | `arm_templates/dashboard.json` |
| S3 Bucket `ecommerce-pipeline-logs-archive` | Storage Account + `pipeline-logs` container | `arm_templates/storage_account.json` |
| S3 Lifecycle Policy | Azure Blob Lifecycle Management Policy | same |
| CloudWatch Logs Insights queries | KQL queries in Log Analytics | `scripts/04_query_pipeline_logs.ps1` |
| AWS X-Ray | Application Insights | `arm_templates/application_insights.json` |
| CloudWatch custom metrics (`put_metric_data`) | App Insights custom metrics via `opencensus-ext-azure` | `azure_monitor_client.py` |
| boto3 `logs.put_log_events()` | `LogsIngestionClient.upload()` (azure-monitor-ingestion) | `azure_monitor_client.py` |

---

## 2. Repository Structure

```
monitoring/
├── README.md                          ← this file
├── __init__.py
├── azure_monitor_client.py            ← Python SDK wrapper (replaces boto3 CloudWatch)
├── pipeline_telemetry.py              ← Context managers / decorators for pipeline stages
│
├── aws/
│   └── cloudwatch_config.json         ← LEGACY reference – original AWS config (DO NOT deploy)
│
└── azure/
    ├── config/
    │   ├── monitoring_config.json     ← Central config (workspace, App Insights, storage)
    │   ├── alert_rules.json           ← Alert rule definitions (human-readable)
    │   └── action_groups.json         ← Action group definitions (email, Teams, SMS)
    │
    ├── arm_templates/
    │   ├── log_analytics_workspace.json   ← Log Analytics Workspace + custom tables
    │   ├── application_insights.json      ← Application Insights component
    │   ├── storage_account.json           ← Blob storage + lifecycle policies
    │   ├── alert_rules.json               ← Scheduled Query Alert Rules (ARM)
    │   └── dashboard.json                 ← Azure Portal Dashboard
    │
    └── scripts/
        ├── 01_setup_azure_monitoring.ps1  ← STEP 1 – Deploy all Azure resources
        ├── 02_configure_alert_rules.ps1   ← STEP 2 – Tune thresholds per environment
        ├── 03_pipeline_health_check.ps1   ← STEP 3 – Validate deployment
        ├── 04_query_pipeline_logs.ps1     ← Ad-hoc KQL queries (replaces CloudWatch Insights)
        └── 05_cleanup_aws_resources.ps1   ← FINAL – Decommission AWS resources
```

---

## 3. Prerequisites

### Azure side
- Azure subscription with Contributor rights on the target resource group
- Azure CLI v2.50+ installed: `az --version`
- PowerShell 7+ (cross-platform): `pwsh --version`
- Log in: `az login`

### Python telemetry (optional)
```bash
pip install azure-identity azure-monitor-ingestion azure-monitor-query opencensus-ext-azure
```
The pipeline runs **without** these packages – telemetry is silently skipped if
the environment variables are not set.

### AWS side (for cleanup only)
- AWS CLI v2 configured with an IAM user/role that has `logs:*`, `cloudwatch:*`, `sns:*`, `s3:*`

---

## 4. Step-by-Step Deployment

### Step 1 – Deploy Azure infrastructure

```powershell
cd monitoring/azure/scripts

.\01_setup_azure_monitoring.ps1 `
    -SubscriptionId   "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
    -ResourceGroupName "rg-ecommerce-pipeline-prod" `
    -Location          "eastus" `
    -Environment       "production" `
    -NotificationEmail "data-engineering@example.com" `
    -TeamsWebhookUrl   "https://outlook.office.com/webhook/..."
```

This script deploys (in order):
1. Resource Group
2. Log Analytics Workspace with `PipelineEvents_CL` and `QualityCheckResults_CL` tables
3. Application Insights linked to the workspace
4. Storage Account with `pipeline-logs` and `diagnostic-exports` containers
5. Action Groups (critical + warning) wired to email + Teams
6. All alert rules (ARM template)
7. Azure Portal Dashboard

The script prints the **Application Insights Connection String** at the end —
copy it for Step 3.

### Step 2 – Fine-tune alert thresholds (optional)

```powershell
.\02_configure_alert_rules.ps1 `
    -SubscriptionId   "xxxx" `
    -ResourceGroupName "rg-ecommerce-pipeline-prod" `
    -Environment       "production"
```

Applies environment-specific thresholds:

| Environment | QualityFail threshold | IngestErrors threshold | MaxDuration |
|---|---|---|---|
| production | ≥ 1 | ≥ 1 | > 600 s |
| staging | ≥ 3 | ≥ 2 | > 900 s |
| dev | ≥ 5 | ≥ 5 | > 1800 s |

### Step 3 – Enable Python telemetry

Set these environment variables on the server/VM that runs the pipeline:

```bash
export APPLICATIONINSIGHTS_CONNECTION_STRING="InstrumentationKey=...;IngestionEndpoint=..."
export AZURE_LOG_ANALYTICS_ENDPOINT="https://<dce-name>.<region>.ingest.monitor.azure.com"
export AZURE_LOG_ANALYTICS_RULE_ID="/subscriptions/.../dataCollectionRules/<dcr-name>"
export AZURE_LOG_ANALYTICS_STREAM_NAME="Custom-PipelineEvents_CL"
```

> **How to get the DCE endpoint and DCR rule ID:**
> After Step 1, in the Azure Portal go to **Monitor → Data Collection Rules** → select
> your rule → **Overview** → copy **Immutable ID**. The DCE endpoint is under
> **Monitor → Data Collection Endpoints**.

Once set, run the pipeline normally:

```bash
python3 -m pipelines.ingest
python3 -m pipelines.transform
python3 -m transforms.quality_checks
```

Each stage will emit structured events to `PipelineEvents_CL` and quality results
to `QualityCheckResults_CL`.

### Step 4 – Validate the deployment

```powershell
.\03_pipeline_health_check.ps1 `
    -SubscriptionId   "xxxx" `
    -ResourceGroupName "rg-ecommerce-pipeline-prod" `
    -Environment       "production" `
    -SendTestEvent
```

All checks must show `[PASS]` before decommissioning AWS resources.

---

## 5. Environment Variables

| Variable | Description | AWS equivalent |
|---|---|---|
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | App Insights connection string | none (X-Ray used trace IDs differently) |
| `AZURE_LOG_ANALYTICS_ENDPOINT` | Data Collection Endpoint URL | CloudWatch agent endpoint |
| `AZURE_LOG_ANALYTICS_RULE_ID` | Data Collection Rule resource ID | CloudWatch log group ARN |
| `AZURE_LOG_ANALYTICS_STREAM_NAME` | Stream name for PipelineEvents | CloudWatch log stream name |
| `AZURE_CLIENT_ID` | Service principal client ID (if not using managed identity) | AWS access key ID |
| `AZURE_TENANT_ID` | Azure AD tenant ID | AWS account ID |
| `AZURE_CLIENT_SECRET` | Service principal secret | AWS secret access key |

---

## 6. KQL Query Reference

Run pre-built queries interactively:

```powershell
# List all available queries
.\04_query_pipeline_logs.ps1 -SubscriptionId x -ResourceGroupName y -List

# Query: recent errors (last 24 h)
.\04_query_pipeline_logs.ps1 -SubscriptionId x -ResourceGroupName y -QueryName recent_errors

# Query: quality check failures over the last 48 h
.\04_query_pipeline_logs.ps1 -SubscriptionId x -ResourceGroupName y `
    -QueryName quality_failures -LookbackHours 48

# Export as JSON
.\04_query_pipeline_logs.ps1 -SubscriptionId x -ResourceGroupName y `
    -QueryName run_history -OutputFormat json
```

Available pre-built queries:

| Name | Description | AWS CloudWatch Insights equivalent |
|---|---|---|
| `recent_errors` | Last 50 errors across all stages | `FILTER @message LIKE /ERROR/ \| SORT @timestamp DESC \| LIMIT 50` |
| `daily_ingest_summary` | Daily row count & duration stats | `STATS avg(RowCount)` |
| `quality_failures` | Failures by check name per day | `FILTER @message LIKE /FAIL/` |
| `pipeline_duration_trend` | Avg/P95 duration per stage per hour | CloudWatch metric statistics |
| `run_history` | Full run history with pass/fail outcome | Custom CloudWatch Insights query |
| `stale_pipeline_check` | Detect missing ingest runs | CloudWatch `treat_missing_data=breaching` |
| `quality_pass_rate` | Pass rate % by check name | no direct equivalent |

---

## 7. Alert Rules Reference

| Alert Rule | Severity | Condition | Action Group | AWS Equivalent |
|---|---|---|---|---|
| `alert-pipeline-ingest-errors` | Warning (2) | ≥ 1 ingest ERROR in 5 min | critical | `PipelineIngestErrors` alarm |
| `alert-transform-errors` | Warning (2) | ≥ 1 transform ERROR in 5 min | critical | `PipelineTransformErrors` alarm |
| `alert-quality-check-failures` | Error (1) | ≥ 1 quality FAIL in 15 min | critical | `DataQualityFailures` alarm |
| `alert-pipeline-stale` | Warning (2) | < 1 successful ingest in 24 h | warning | `PipelineStale` alarm (missing_data=breaching) |
| `alert-long-running-pipeline` | Info (3) | any stage > 600 s | warning | no direct equivalent |

---

## 8. Decommissioning AWS Resources

Only run this **after** all health checks pass and you have confirmed Azure Monitor
is receiving live pipeline data:

```powershell
# Dry run first – see what would be deleted
.\05_cleanup_aws_resources.ps1 -AwsRegion us-east-1 -DryRun

# Real cleanup (keep S3 bucket for manual archive review)
.\05_cleanup_aws_resources.ps1 -AwsRegion us-east-1

# Full cleanup including S3 bucket (IRREVERSIBLE)
.\05_cleanup_aws_resources.ps1 -AwsRegion us-east-1 -DeleteS3Bucket
```

Resources removed:
- CloudWatch Log Groups `/ecommerce/pipeline/*`
- CloudWatch Alarms `PipelineIngestErrors`, `DataQualityFailures`, `PipelineStale`
- CloudWatch Dashboard `EcommerceOrdersPipeline`
- SNS Topic `pipeline-alerts` and all subscriptions
- (optional) S3 bucket `ecommerce-pipeline-logs-archive`

---

## 9. Troubleshooting

### "az login required" error in PowerShell scripts
Run `az login` in your terminal before executing the scripts, or use a service
principal: `az login --service-principal -u $CLIENT_ID -p $SECRET --tenant $TENANT_ID`.

### Python telemetry shows `[AzureMonitor OFFLINE]` in logs
The environment variables are not set. The pipeline still runs correctly –
telemetry is a no-op. Set `AZURE_LOG_ANALYTICS_ENDPOINT` and
`AZURE_LOG_ANALYTICS_RULE_ID` to enable ingestion.

### Custom tables not appearing in Log Analytics
Custom tables (`_CL` suffix) are created by the ARM template but may take up to
15 minutes to appear in the Log Analytics schema. Also check that at least one
record has been ingested.

### `ImportError: No module named 'azure'`
Install the Azure SDK packages:
```bash
pip install azure-identity azure-monitor-ingestion azure-monitor-query opencensus-ext-azure
```

### Alert rules not firing
1. Confirm the Log Analytics Workspace is receiving data (run `recent_errors` query).
2. Verify the Data Collection Rule is correctly linked to the workspace.
3. Check the alert rule's evaluation frequency and window size in the Azure Portal.
4. For `alert-pipeline-stale`, confirm the rule is enabled (it is disabled in dev/staging by `02_configure_alert_rules.ps1`).
