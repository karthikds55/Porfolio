<#
.SYNOPSIS
    Query Azure Log Analytics for ecommerce pipeline telemetry.
    Replaces AWS CloudWatch Logs Insights queries.

.DESCRIPTION
    Provides pre-built KQL (Kusto Query Language) queries covering the same
    diagnostic scenarios previously handled by CloudWatch Logs Insights.

    AWS → Azure query equivalents:
      CloudWatch Insights STATS      → KQL summarize
      CloudWatch Insights FILTER     → KQL where
      CloudWatch Insights SORT       → KQL order by
      CloudWatch Insights LIMIT      → KQL take

.PARAMETER SubscriptionId       Azure subscription ID.
.PARAMETER ResourceGroupName    Resource group name.
.PARAMETER WorkspaceName        Log Analytics Workspace name (default derived from environment).
.PARAMETER Environment          dev | staging | production.
.PARAMETER QueryName            Predefined query to run (see -List for available queries).
.PARAMETER LookbackHours        How many hours back to look (default 24).
.PARAMETER OutputFormat         table | json | csv (default table).
.PARAMETER List                 List available predefined queries and exit.

.EXAMPLE
    # List available queries
    .\04_query_pipeline_logs.ps1 -SubscriptionId x -ResourceGroupName y -List

    # Run a specific query
    .\04_query_pipeline_logs.ps1 `
        -SubscriptionId "xxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-prod" `
        -QueryName "quality_failures" `
        -LookbackHours 48
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$SubscriptionId,
    [Parameter(Mandatory)] [string]$ResourceGroupName,

    [Parameter(Mandatory = $false)] [string]$WorkspaceName = "",
    [Parameter(Mandatory = $false)]
    [ValidateSet("dev","staging","production")]
    [string]$Environment = "production",

    [Parameter(Mandatory = $false)] [string]$QueryName = "recent_errors",
    [Parameter(Mandatory = $false)] [int]$LookbackHours = 24,
    [Parameter(Mandatory = $false)]
    [ValidateSet("table","json","csv")]
    [string]$OutputFormat = "table",

    [switch]$List
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($WorkspaceName -eq "") {
    $WorkspaceName = "law-ecommerce-pipeline-$($Environment.Substring(0,4))"
}

# ── Predefined KQL queries (AWS CloudWatch Insights equivalents) ──────────────
$Queries = @{

    recent_errors = @{
        Description = "Last 50 pipeline errors across all stages (AWS: CloudWatch Insights FILTER @message LIKE /ERROR/)"
        KQL = @"
PipelineEvents_CL
| where TimeGenerated > ago({0}h)
| where Status == 'ERROR'
| order by TimeGenerated desc
| take 50
| project TimeGenerated, Stage, Message, ErrorDetails, RunId
"@
    }

    daily_ingest_summary = @{
        Description = "Daily ingest row count and avg duration (AWS: CloudWatch Insights STATS avg(RowCount))"
        KQL = @"
PipelineEvents_CL
| where TimeGenerated > ago({0}h)
| where Stage == 'ingest' and Status == 'SUCCESS'
| summarize
    TotalRows    = sum(RowCount),
    AvgRows      = avg(RowCount),
    MaxRows      = max(RowCount),
    AvgDurationS = avg(DurationSeconds),
    Runs         = count()
  by bin(TimeGenerated, 1d)
| order by TimeGenerated desc
"@
    }

    quality_failures = @{
        Description = "Quality check failures by check name (AWS: CloudWatch Insights FILTER @message LIKE /FAIL/)"
        KQL = @"
QualityCheckResults_CL
| where TimeGenerated > ago({0}h)
| where Result == 'FAIL'
| summarize Failures = count() by CheckName, bin(TimeGenerated, 1d)
| order by TimeGenerated desc, Failures desc
"@
    }

    pipeline_duration_trend = @{
        Description = "Pipeline stage duration over time – detects performance regressions (AWS: CloudWatch metric statistics)"
        KQL = @"
PipelineEvents_CL
| where TimeGenerated > ago({0}h)
| where Stage in ('ingest', 'transform')
| summarize
    AvgDuration = avg(DurationSeconds),
    MaxDuration = max(DurationSeconds),
    P95Duration = percentile(DurationSeconds, 95)
  by Stage, bin(TimeGenerated, 1h)
| order by TimeGenerated desc
"@
    }

    run_history = @{
        Description = "Full pipeline run history with pass/fail outcome per RunId"
        KQL = @"
PipelineEvents_CL
| where TimeGenerated > ago({0}h)
| summarize
    Stages      = make_set(Stage),
    HasError    = countif(Status == 'ERROR'),
    TotalEvents = count(),
    FirstEvent  = min(TimeGenerated),
    LastEvent   = max(TimeGenerated)
  by RunId
| extend Outcome = iff(HasError > 0, 'FAILED', 'SUCCESS')
| order by LastEvent desc
"@
    }

    stale_pipeline_check = @{
        Description = "Detect if no successful ingest run exists in the lookback window (AWS: CloudWatch treat_missing_data alarm)"
        KQL = @"
PipelineEvents_CL
| where TimeGenerated > ago({0}h)
| where Stage == 'ingest' and Status == 'SUCCESS'
| summarize SuccessfulRuns = count(), LastRun = max(TimeGenerated)
"@
    }

    quality_pass_rate = @{
        Description = "Quality check pass rate by check name (percentage of runs that passed)"
        KQL = @"
QualityCheckResults_CL
| where TimeGenerated > ago({0}h)
| summarize
    Passed = countif(Result == 'PASS'),
    Failed = countif(Result == 'FAIL'),
    Total  = count()
  by CheckName
| extend PassRate = round(100.0 * Passed / Total, 2)
| order by PassRate asc
"@
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# List mode
# ─────────────────────────────────────────────────────────────────────────────
if ($List) {
    Write-Host ""
    Write-Host "Available predefined queries:" -ForegroundColor Cyan
    Write-Host ""
    foreach ($key in ($Queries.Keys | Sort-Object)) {
        Write-Host "  $key" -ForegroundColor Yellow
        Write-Host "    $($Queries[$key].Description)"
        Write-Host ""
    }
    exit 0
}

# ─────────────────────────────────────────────────────────────────────────────
# Validate query name
# ─────────────────────────────────────────────────────────────────────────────
if (-not $Queries.ContainsKey($QueryName)) {
    Write-Host "Unknown query '$QueryName'. Use -List to see available queries." -ForegroundColor Red
    exit 1
}

az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) { throw "az login required." }

# ─────────────────────────────────────────────────────────────────────────────
# Execute query
# ─────────────────────────────────────────────────────────────────────────────
$selectedQuery = $Queries[$QueryName]
$kql = $selectedQuery.KQL -f $LookbackHours

Write-Host ""
Write-Host "Query        : $QueryName" -ForegroundColor Cyan
Write-Host "Description  : $($selectedQuery.Description)" -ForegroundColor Cyan
Write-Host "Workspace    : $WorkspaceName" -ForegroundColor Cyan
Write-Host "Lookback     : ${LookbackHours}h" -ForegroundColor Cyan
Write-Host "Output format: $OutputFormat" -ForegroundColor Cyan
Write-Host ""
Write-Host "KQL:" -ForegroundColor DarkGray
Write-Host $kql -ForegroundColor DarkGray
Write-Host ""

$workspaceId = az monitor log-analytics workspace show `
    --resource-group $ResourceGroupName `
    --workspace-name $WorkspaceName `
    --query customerId `
    --output tsv

$results = az monitor log-analytics query `
    --workspace $workspaceId `
    --analytics-query $kql `
    --output $OutputFormat

Write-Host $results
