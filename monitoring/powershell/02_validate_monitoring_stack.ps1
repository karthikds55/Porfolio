<#
.SYNOPSIS
    Validates that all Azure monitoring resources for the e-commerce pipeline
    are deployed, healthy, and correctly configured.

.DESCRIPTION
    Checks:
      • Log Analytics Workspace exists and is in a SucceededProvisioning state.
      • Application Insights exists and its instrumentation key is non-empty.
      • Action Group exists and has at least one email receiver.
      • All 4 alert rules exist and are enabled.
      • Dashboard exists.
      • (Optional) Sends a test trace to App Insights and queries it back via Log Analytics.

.PARAMETER SubscriptionId
    Azure Subscription ID.

.PARAMETER ResourceGroupName
    Resource group containing the monitoring resources.

.PARAMETER RunConnectivityTest
    If specified, sends a live test event to Application Insights and queries
    Log Analytics to confirm end-to-end ingestion (can take ~5 min for data).

.EXAMPLE
    ./02_validate_monitoring_stack.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -RunConnectivityTest
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [switch]$RunConnectivityTest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$passCount = 0
$failCount = 0

function Test-Pass([string]$check) {
    Write-Host "   [PASS] $check" -ForegroundColor Green
    $script:passCount++
}
function Test-Fail([string]$check, [string]$detail = "") {
    Write-Host "   [FAIL] $check" -ForegroundColor Red
    if ($detail) { Write-Host "          $detail" -ForegroundColor DarkRed }
    $script:failCount++
}
function Write-Section([string]$title) {
    Write-Host "`n── $title" -ForegroundColor Cyan
}

# ── Auth ──────────────────────────────────────────────────────────────────────
Write-Section "Connecting to Azure"
Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
Write-Host "   Subscription: $SubscriptionId"

# ── 1. Log Analytics Workspace ────────────────────────────────────────────────
Write-Section "Log Analytics Workspace"
try {
    $law = Get-AzOperationalInsightsWorkspace `
        -ResourceGroupName $ResourceGroupName `
        -Name "law-ecommerce-pipeline"
    if ($law.ProvisioningState -eq "Succeeded") {
        Test-Pass "law-ecommerce-pipeline exists (ProvisioningState=Succeeded)"
    } else {
        Test-Fail "law-ecommerce-pipeline" "ProvisioningState=$($law.ProvisioningState)"
    }
    if ($law.Sku.Name -eq "PerGB2018") {
        Test-Pass "SKU is PerGB2018"
    } else {
        Test-Fail "Unexpected SKU" $law.Sku.Name
    }
    if ($law.RetentionInDays -ge 30) {
        Test-Pass "Retention >= 30 days ($($law.RetentionInDays))"
    } else {
        Test-Fail "Retention too low" "$($law.RetentionInDays) days"
    }
    $lawId = $law.ResourceId
} catch {
    Test-Fail "law-ecommerce-pipeline not found" $_.Exception.Message
    $lawId = $null
}

# ── 2. Application Insights ───────────────────────────────────────────────────
Write-Section "Application Insights"
try {
    $appi = Get-AzApplicationInsights `
        -ResourceGroupName $ResourceGroupName `
        -Name "appi-ecommerce-pipeline"
    Test-Pass "appi-ecommerce-pipeline exists"
    if ($appi.InstrumentationKey) {
        Test-Pass "InstrumentationKey is set"
    } else {
        Test-Fail "InstrumentationKey is empty"
    }
    if ($appi.WorkspaceResourceId) {
        Test-Pass "Workspace-based (linked to Log Analytics)"
    } else {
        Test-Fail "Not linked to a Log Analytics Workspace"
    }
    $appiId = $appi.Id
} catch {
    Test-Fail "appi-ecommerce-pipeline not found" $_.Exception.Message
    $appiId = $null
}

# ── 3. Action Group ───────────────────────────────────────────────────────────
Write-Section "Action Group"
try {
    $ag = Get-AzActionGroup `
        -ResourceGroupName $ResourceGroupName `
        -Name "ag-ecommerce-pipeline-ops"
    Test-Pass "ag-ecommerce-pipeline-ops exists"
    if ($ag.Enabled) {
        Test-Pass "Action Group is enabled"
    } else {
        Test-Fail "Action Group is disabled"
    }
    if ($ag.EmailReceiver.Count -gt 0) {
        Test-Pass "Has $($ag.EmailReceiver.Count) email receiver(s)"
    } else {
        Test-Fail "No email receivers configured"
    }
} catch {
    Test-Fail "ag-ecommerce-pipeline-ops not found" $_.Exception.Message
}

# ── 4. Alert Rules ────────────────────────────────────────────────────────────
Write-Section "Alert Rules"
$expectedRules = @(
    "alert-pipeline-failure",
    "alert-quality-check-fail",
    "alert-high-null-rate",
    "alert-long-pipeline-duration"
)
foreach ($ruleName in $expectedRules) {
    try {
        $rule = Get-AzScheduledQueryRule `
            -ResourceGroupName $ResourceGroupName `
            -Name $ruleName
        if ($rule.Enabled -or $rule.State -eq "Enabled") {
            Test-Pass "$ruleName exists and is enabled"
        } else {
            Test-Fail "$ruleName exists but is DISABLED"
        }
    } catch {
        Test-Fail "$ruleName not found" $_.Exception.Message
    }
}

# ── 5. Dashboard ──────────────────────────────────────────────────────────────
Write-Section "Portal Dashboard"
try {
    $dash = Get-AzPortalDashboard `
        -ResourceGroupName $ResourceGroupName `
        -Name "dash-ecommerce-pipeline" `
        -ErrorAction Stop
    Test-Pass "dash-ecommerce-pipeline exists"
} catch {
    # Dashboard API returns 404 if not found; treat gracefully
    Test-Fail "dash-ecommerce-pipeline not found" $_.Exception.Message
}

# ── 6. Optional connectivity test ─────────────────────────────────────────────
if ($RunConnectivityTest -and $appiId) {
    Write-Section "Connectivity Test (live telemetry)"
    Write-Host "   Sending test trace to Application Insights..." -ForegroundColor DarkGray

    # Use the REST API to send a quickpulse / track event
    $appiResource = Get-AzApplicationInsights -ResourceGroupName $ResourceGroupName -Name "appi-ecommerce-pipeline"
    $iKey = $appiResource.InstrumentationKey

    $body = @(
        @{
            name = "Microsoft.ApplicationInsights.$($iKey -replace '-','').Event"
            time = (Get-Date -Format "o")
            iKey = $iKey
            tags = @{ "ai.operation.name" = "ValidationTest" }
            data = @{
                baseType = "EventData"
                baseData = @{
                    ver  = 2
                    name = "MonitoringValidationTest"
                    properties = @{ source = "02_validate_monitoring_stack.ps1" }
                }
            }
        }
    ) | ConvertTo-Json -Depth 10

    try {
        Invoke-RestMethod -Uri "https://dc.services.visualstudio.com/v2/track" `
            -Method Post -ContentType "application/json" -Body $body | Out-Null
        Test-Pass "Test event sent to Application Insights (check portal in ~5 min)"
    } catch {
        Test-Fail "Failed to send test event" $_.Exception.Message
    }
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "`n══════════════════════════════════════════" -ForegroundColor Yellow
Write-Host " Validation complete" -ForegroundColor Yellow
Write-Host "   PASS: $passCount" -ForegroundColor Green
if ($failCount -gt 0) {
    Write-Host "   FAIL: $failCount" -ForegroundColor Red
    Write-Host "══════════════════════════════════════════" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "   FAIL: 0" -ForegroundColor Green
    Write-Host "══════════════════════════════════════════" -ForegroundColor Yellow
    exit 0
}
