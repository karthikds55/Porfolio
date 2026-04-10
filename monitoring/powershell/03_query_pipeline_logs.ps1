<#
.SYNOPSIS
    Queries Azure Log Analytics for pipeline run history, quality check results,
    error/warning log streams, and custom metrics for the e-commerce pipeline.
    Replaces AWS CloudWatch Logs Insights queries.

.DESCRIPTION
    Provides six named query modes:
      PipelineRuns     – Recent pipeline execution history.
      QualityChecks    – Quality check PASS/FAIL history.
      Errors           – ERROR and WARNING log stream.
      NullRates        – Null-rate % metric over time per column.
      DurationTrend    – Pipeline run duration over time.
      RecordCounts     – Row counts ingested per pipeline run.

.PARAMETER SubscriptionId
    Azure Subscription ID.

.PARAMETER ResourceGroupName
    Resource group containing the monitoring resources.

.PARAMETER QueryMode
    One of: PipelineRuns, QualityChecks, Errors, NullRates, DurationTrend, RecordCounts.

.PARAMETER LookbackHours
    How many hours of history to include (default: 24).

.PARAMETER OutputFormat
    Table (default) or JSON.

.EXAMPLE
    ./03_query_pipeline_logs.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -QueryMode QualityChecks `
        -LookbackHours 48

.EXAMPLE
    ./03_query_pipeline_logs.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -QueryMode Errors `
        -OutputFormat JSON
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [ValidateSet("PipelineRuns","QualityChecks","Errors","NullRates","DurationTrend","RecordCounts")]
    [string]$QueryMode = "PipelineRuns",
    [int]$LookbackHours = 24,
    [ValidateSet("Table","JSON")]
    [string]$OutputFormat = "Table"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Auth ──────────────────────────────────────────────────────────────────────
Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
$law = Get-AzOperationalInsightsWorkspace `
    -ResourceGroupName $ResourceGroupName `
    -Name "law-ecommerce-pipeline"
$workspaceId = $law.CustomerId

Write-Host "Querying Log Analytics workspace: $($law.Name)" -ForegroundColor Cyan
Write-Host "Mode: $QueryMode  |  Lookback: ${LookbackHours}h  |  Format: $OutputFormat" -ForegroundColor DarkGray

# ── KQL queries ───────────────────────────────────────────────────────────────

$timeFilter = "| where TimeGenerated >= ago(${LookbackHours}h)"

$queries = @{
    PipelineRuns = @"
AppTraces
$timeFilter
| where Properties.event_type == 'pipeline_run'
| project
    TimeGenerated,
    Pipeline    = tostring(Properties.pipeline),
    Status      = tostring(Properties.status),
    DurationSec = todouble(Properties.duration_seconds),
    RecordCount = toint(Properties.record_count),
    Message
| order by TimeGenerated desc
| take 50
"@

    QualityChecks = @"
AppTraces
$timeFilter
| where Properties.event_type == 'quality_check'
| project
    TimeGenerated,
    CheckName = tostring(Properties.check_name),
    Result    = tostring(Properties.result),
    Details   = tostring(Properties.details)
| order by TimeGenerated desc
| take 100
"@

    Errors = @"
AppTraces
$timeFilter
| where SeverityLevel >= 3
| project
    TimeGenerated,
    Severity = case(SeverityLevel == 3, "WARNING", SeverityLevel >= 4, "ERROR", "CRITICAL"),
    Pipeline = tostring(Properties.pipeline),
    Message
| order by TimeGenerated desc
| take 100
"@

    NullRates = @"
AppMetrics
$timeFilter
| where Name == 'null_rate_percent'
| summarize AvgNullRate = round(avg(Sum), 2)
    by bin(TimeGenerated, 1h), Column = tostring(Properties.column)
| order by TimeGenerated desc
"@

    DurationTrend = @"
AppMetrics
$timeFilter
| where Name == 'pipeline_duration_seconds'
| summarize
    AvgDurationSec = round(avg(Sum), 1),
    MaxDurationSec = round(max(Sum), 1)
    by bin(TimeGenerated, 1h), Pipeline = tostring(Properties.pipeline)
| order by TimeGenerated desc
"@

    RecordCounts = @"
AppMetrics
$timeFilter
| where Name == 'records_processed'
| summarize TotalRecords = sum(Sum)
    by bin(TimeGenerated, 1h), Pipeline = tostring(Properties.pipeline)
| order by TimeGenerated desc
"@
}

$kql = $queries[$QueryMode]

# ── Execute ───────────────────────────────────────────────────────────────────
Write-Host "`nExecuting KQL query..." -ForegroundColor DarkGray

$result = Invoke-AzOperationalInsightsQuery `
    -WorkspaceId $workspaceId `
    -Query $kql `
    -Timespan (New-TimeSpan -Hours $LookbackHours)

$rows = $result.Results

if (-not $rows -or $rows.Count -eq 0) {
    Write-Host "No data returned for the selected time range." -ForegroundColor Yellow
    exit 0
}

if ($OutputFormat -eq "JSON") {
    $rows | ConvertTo-Json -Depth 5
} else {
    $rows | Format-Table -AutoSize
}

Write-Host "`nTotal rows returned: $($rows.Count)" -ForegroundColor Cyan
