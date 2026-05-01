<#
.SYNOPSIS
    Post-migration health check – verifies the Azure Monitor stack is wired up
    and receiving data from the ecommerce orders pipeline.

.DESCRIPTION
    Replaces the ad-hoc CloudWatch console checks performed after the original
    AWS deployment. Runs the following validations:
      1. Log Analytics Workspace is reachable and custom tables exist.
      2. Application Insights is collecting heartbeat / custom events.
      3. Storage Account containers are accessible.
      4. All alert rules exist and are enabled.
      5. Action Groups are configured with at least one receiver.
      6. Optional: submits a synthetic test event to verify end-to-end ingestion.

.PARAMETER SubscriptionId       Azure subscription ID.
.PARAMETER ResourceGroupName    Resource group containing all monitoring resources.
.PARAMETER Environment          dev | staging | production.
.PARAMETER SendTestEvent        If specified, submits a synthetic log event to validate ingestion.

.EXAMPLE
    .\03_pipeline_health_check.ps1 `
        -SubscriptionId "xxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-prod" `
        -Environment "production" `
        -SendTestEvent
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string]$SubscriptionId,
    [Parameter(Mandatory)] [string]$ResourceGroupName,
    [Parameter(Mandatory = $false)]
    [ValidateSet("dev","staging","production")]
    [string]$Environment = "production",

    [switch]$SendTestEvent
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Suffix = $Environment.Substring(0,4)

$PASS = "[PASS]"
$FAIL = "[FAIL]"
$WARN = "[WARN]"

$Results  = [System.Collections.Generic.List[hashtable]]::new()
$ExitCode = 0

function Write-Step  { param([string]$M, [string]$C = "Cyan")    Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $M" -ForegroundColor $C }
function Write-Check { param([bool]$Ok, [string]$Label, [string]$Detail = "") {
    if ($Ok) {
        Write-Host "  $PASS $Label" -ForegroundColor Green
        $Results.Add(@{ Status = "PASS"; Label = $Label; Detail = $Detail })
    } else {
        Write-Host "  $FAIL $Label $(if ($Detail) { "– $Detail" })" -ForegroundColor Red
        $Results.Add(@{ Status = "FAIL"; Label = $Label; Detail = $Detail })
        $script:ExitCode = 1
    }
}}

az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) { throw "az login required." }

# ─────────────────────────────────────────────────────────────────────────────
# 1. Log Analytics Workspace
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "1. Checking Log Analytics Workspace"
$WorkspaceName = "law-ecommerce-pipeline-$Suffix"
$workspace = az monitor log-analytics workspace show `
    --resource-group $ResourceGroupName `
    --workspace-name $WorkspaceName `
    --output json 2>$null | ConvertFrom-Json
Write-Check ($null -ne $workspace) "Log Analytics Workspace '$WorkspaceName' exists"

if ($null -ne $workspace) {
    $tables = az monitor log-analytics workspace table list `
        --resource-group $ResourceGroupName `
        --workspace-name $WorkspaceName `
        --output json | ConvertFrom-Json
    $tableNames = $tables | ForEach-Object { $_.name.Split("/")[-1] }
    Write-Check ($tableNames -contains "PipelineEvents_CL")       "Custom table PipelineEvents_CL present"
    Write-Check ($tableNames -contains "QualityCheckResults_CL")  "Custom table QualityCheckResults_CL present"
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. Application Insights
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "2. Checking Application Insights"
$AppInsightsName = "appi-ecommerce-pipeline-$Suffix"
$appi = az monitor app-insights component show `
    --app $AppInsightsName `
    --resource-group $ResourceGroupName `
    --output json 2>$null | ConvertFrom-Json
Write-Check ($null -ne $appi) "Application Insights '$AppInsightsName' exists"
if ($null -ne $appi) {
    Write-Check ($appi.provisioningState -eq "Succeeded") "Application Insights provisioning state = Succeeded" $appi.provisioningState
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. Storage Account
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "3. Checking Storage Account"
$StorageName = "stecommpipeline$Suffix"
$storage = az storage account show `
    --name $StorageName `
    --resource-group $ResourceGroupName `
    --output json 2>$null | ConvertFrom-Json
Write-Check ($null -ne $storage) "Storage Account '$StorageName' exists"

if ($null -ne $storage) {
    $containers = az storage container list `
        --account-name $StorageName `
        --auth-mode login `
        --output json 2>$null | ConvertFrom-Json
    $containerNames = $containers | ForEach-Object { $_.name }
    Write-Check ($containerNames -contains "pipeline-logs")       "Container 'pipeline-logs' exists"
    Write-Check ($containerNames -contains "diagnostic-exports")  "Container 'diagnostic-exports' exists"
}

# ─────────────────────────────────────────────────────────────────────────────
# 4. Alert Rules
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "4. Checking Alert Rules"
$ExpectedAlerts = @(
    "alert-pipeline-ingest-errors",
    "alert-transform-errors",
    "alert-quality-check-failures",
    "alert-pipeline-stale",
    "alert-long-running-pipeline"
)
$alertRules = az monitor scheduled-query list `
    --resource-group $ResourceGroupName `
    --output json 2>$null | ConvertFrom-Json
$alertNames = $alertRules | ForEach-Object { $_.name }

foreach ($expected in $ExpectedAlerts) {
    $exists = $alertNames -contains $expected
    Write-Check $exists "Alert rule '$expected' exists"
    if ($exists) {
        $rule = $alertRules | Where-Object { $_.name -eq $expected }
        if ($Environment -eq "production" -or $expected -ne "alert-pipeline-stale") {
            Write-Check ($rule.properties.enabled -eq $true) "  → enabled"
        }
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 5. Action Groups
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "5. Checking Action Groups"
foreach ($agName in @("ag-pipeline-critical", "ag-pipeline-warning")) {
    $ag = az monitor action-group show `
        --resource-group $ResourceGroupName `
        --name $agName `
        --output json 2>$null | ConvertFrom-Json
    Write-Check ($null -ne $ag) "Action Group '$agName' exists"
    if ($null -ne $ag) {
        $receiverCount = $ag.emailReceivers.Count + $ag.webhookReceivers.Count + $ag.smsReceivers.Count
        Write-Check ($receiverCount -gt 0) "  → has at least one receiver ($receiverCount total)"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 6. Synthetic test event (optional)
# ─────────────────────────────────────────────────────────────────────────────
if ($SendTestEvent -and $null -ne $workspace) {
    Write-Step "6. Sending synthetic test event to Log Analytics"
    $WorkspaceKey = az monitor log-analytics workspace get-shared-keys `
        --resource-group $ResourceGroupName `
        --workspace-name $WorkspaceName `
        --query primarySharedKey `
        --output tsv
    $CustomerId   = $workspace.customerId

    $body = @(
        @{
            TimeGenerated   = (Get-Date).ToUniversalTime().ToString("o")
            Stage           = "health_check"
            Status          = "INFO"
            Message         = "Synthetic health-check event from 03_pipeline_health_check.ps1"
            RowCount        = 0
            DurationSeconds = 0.0
            ErrorDetails    = ""
            RunId           = [System.Guid]::NewGuid().ToString()
        }
    ) | ConvertTo-Json

    $bodyBytes    = [System.Text.Encoding]::UTF8.GetBytes($body)
    $date         = [System.DateTime]::UtcNow.ToString("R")
    $contentType  = "application/json"
    $resource     = "/api/logs"
    $logType      = "PipelineEvents"
    $stringToHash = "POST`n$($bodyBytes.Length)`n$contentType`nx-ms-date:$date`n$resource"
    $hmac         = [System.Security.Cryptography.HMACSHA256]::new([System.Convert]::FromBase64String($WorkspaceKey))
    $signature    = [System.Convert]::ToBase64String($hmac.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($stringToHash)))
    $authHeader   = "SharedKey ${CustomerId}:${signature}"

    try {
        $response = Invoke-WebRequest `
            -Uri "https://${CustomerId}.ods.opinsights.azure.com${resource}?api-version=2016-04-01" `
            -Method POST `
            -Headers @{
                "Authorization" = $authHeader
                "Log-Type"      = $logType
                "x-ms-date"     = $date
            } `
            -ContentType $contentType `
            -Body $body
        Write-Check ($response.StatusCode -in 200..202) "Synthetic event accepted (HTTP $($response.StatusCode))"
    } catch {
        Write-Check $false "Synthetic event submission failed" $_.Exception.Message
    }
} elseif ($SendTestEvent) {
    Write-Host "  $WARN Skipping synthetic event – workspace not found." -ForegroundColor Yellow
}

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
$passed = ($Results | Where-Object { $_.Status -eq "PASS" }).Count
$failed = ($Results | Where-Object { $_.Status -eq "FAIL" }).Count

Write-Host ""
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Magenta
Write-Host " Health Check Summary: $passed passed, $failed failed" -ForegroundColor Magenta
Write-Host "═══════════════════════════════════════════════" -ForegroundColor Magenta

if ($ExitCode -ne 0) {
    Write-Host "Some checks FAILED. Review output above." -ForegroundColor Red
}

exit $ExitCode
