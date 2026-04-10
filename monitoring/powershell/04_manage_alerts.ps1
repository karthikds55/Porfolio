<#
.SYNOPSIS
    Enables, disables, tests, or reports on Azure Monitor alert rules for the
    e-commerce orders pipeline. Replaces AWS CloudWatch Alarm state management.

.DESCRIPTION
    Actions:
      Status   – Show current state for all pipeline alert rules.
      Enable   – Enable one or all alert rules.
      Disable  – Disable one or all alert rules (e.g. during maintenance).
      Test     – Fire a synthetic log event to trigger the alert immediately.
      History  – Show fired-alert history from the Azure Activity Log.

.PARAMETER SubscriptionId
    Azure Subscription ID.

.PARAMETER ResourceGroupName
    Resource group containing the monitoring resources.

.PARAMETER Action
    One of: Status, Enable, Disable, Test, History.

.PARAMETER RuleName
    Specific alert rule name to target. If omitted, all pipeline rules are targeted.
    Valid names: alert-pipeline-failure, alert-quality-check-fail,
                 alert-high-null-rate, alert-long-pipeline-duration.

.PARAMETER LookbackHours
    For History action: how many hours of alert history to retrieve (default: 72).

.EXAMPLE
    # Show status of all rules
    ./04_manage_alerts.ps1 -SubscriptionId "..." -ResourceGroupName "rg-ecommerce-pipeline-monitoring" -Action Status

.EXAMPLE
    # Disable all rules for a maintenance window
    ./04_manage_alerts.ps1 -SubscriptionId "..." -ResourceGroupName "rg-ecommerce-pipeline-monitoring" -Action Disable

.EXAMPLE
    # Re-enable a single rule after maintenance
    ./04_manage_alerts.ps1 -SubscriptionId "..." -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -Action Enable -RuleName "alert-pipeline-failure"

.EXAMPLE
    # View fired alerts from the last 3 days
    ./04_manage_alerts.ps1 -SubscriptionId "..." -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -Action History -LookbackHours 72
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [Parameter(Mandatory)]
    [ValidateSet("Status","Enable","Disable","Test","History")]
    [string]$Action,
    [string]$RuleName      = "",
    [int]$LookbackHours    = 72
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$allRules = @(
    "alert-pipeline-failure",
    "alert-quality-check-fail",
    "alert-high-null-rate",
    "alert-long-pipeline-duration"
)

$targetRules = if ($RuleName) { @($RuleName) } else { $allRules }

# ── Auth ──────────────────────────────────────────────────────────────────────
Set-AzContext -SubscriptionId $SubscriptionId | Out-Null

# ── Helpers ───────────────────────────────────────────────────────────────────

function Get-Rule([string]$name) {
    Get-AzScheduledQueryRule -ResourceGroupName $ResourceGroupName -Name $name -ErrorAction SilentlyContinue
}

# ── Actions ───────────────────────────────────────────────────────────────────

switch ($Action) {

    "Status" {
        Write-Host "`n Alert Rule Status" -ForegroundColor Cyan
        Write-Host (" {0,-45} {1,-12} {2}" -f "Rule", "State", "Severity") -ForegroundColor DarkGray
        Write-Host (" {0,-45} {1,-12} {2}" -f ("-"*44), ("-"*11), ("-"*8)) -ForegroundColor DarkGray
        foreach ($name in $allRules) {
            $rule = Get-Rule $name
            if ($rule) {
                $state    = if ($rule.Enabled) { "ENABLED" } else { "DISABLED" }
                $severity = "Sev$($rule.Severity)"
                $color    = if ($rule.Enabled) { "Green" } else { "Yellow" }
                Write-Host (" {0,-45} {1,-12} {2}" -f $name, $state, $severity) -ForegroundColor $color
            } else {
                Write-Host (" {0,-45} {1,-12}" -f $name, "NOT FOUND") -ForegroundColor Red
            }
        }
    }

    "Enable" {
        foreach ($name in $targetRules) {
            $rule = Get-Rule $name
            if ($rule) {
                if ($PSCmdlet.ShouldProcess($name, "Enable alert rule")) {
                    Update-AzScheduledQueryRule `
                        -ResourceGroupName $ResourceGroupName `
                        -Name $name `
                        -Enabled $true | Out-Null
                    Write-Host "   [ENABLED]  $name" -ForegroundColor Green
                }
            } else {
                Write-Warning "Rule not found: $name"
            }
        }
    }

    "Disable" {
        foreach ($name in $targetRules) {
            $rule = Get-Rule $name
            if ($rule) {
                if ($PSCmdlet.ShouldProcess($name, "Disable alert rule")) {
                    Update-AzScheduledQueryRule `
                        -ResourceGroupName $ResourceGroupName `
                        -Name $name `
                        -Enabled $false | Out-Null
                    Write-Host "   [DISABLED] $name" -ForegroundColor Yellow
                }
            } else {
                Write-Warning "Rule not found: $name"
            }
        }
        Write-Host "`n   Remember to re-enable rules after the maintenance window." -ForegroundColor DarkYellow
    }

    "Test" {
        Write-Host "`nSending synthetic ERROR event to trigger alert-pipeline-failure..." -ForegroundColor Cyan

        $appi = Get-AzApplicationInsights `
            -ResourceGroupName $ResourceGroupName `
            -Name "appi-ecommerce-pipeline"

        $iKey = $appi.InstrumentationKey
        $body = @(
            @{
                name = "Microsoft.ApplicationInsights.$($iKey -replace '-','').Message"
                time = (Get-Date -Format "o")
                iKey = $iKey
                tags = @{ "ai.operation.name" = "AlertTest" }
                data = @{
                    baseType = "MessageData"
                    baseData = @{
                        ver           = 2
                        message       = "[TEST] Synthetic pipeline error for alert validation"
                        severityLevel = "Error"
                        properties    = @{
                            pipeline   = "ingest"
                            event_type = "pipeline_error"
                            test       = "true"
                        }
                    }
                }
            }
        ) | ConvertTo-Json -Depth 10

        Invoke-RestMethod `
            -Uri "https://dc.services.visualstudio.com/v2/track" `
            -Method Post -ContentType "application/json" -Body $body | Out-Null

        Write-Host "   Synthetic event sent. The alert-pipeline-failure rule should fire within ~5 minutes." -ForegroundColor Green
        Write-Host "   Check Azure Portal › Monitor › Alerts, or run -Action History to confirm." -ForegroundColor DarkGray
    }

    "History" {
        Write-Host "`nFired alerts – last ${LookbackHours}h" -ForegroundColor Cyan

        $startTime = (Get-Date).AddHours(-$LookbackHours).ToUniversalTime()
        $endTime   = (Get-Date).ToUniversalTime()

        $events = Get-AzActivityLog `
            -ResourceGroupName $ResourceGroupName `
            -StartTime $startTime `
            -EndTime $endTime `
            -MaxRecord 200 `
            -ErrorAction SilentlyContinue |
            Where-Object { $_.OperationName.Value -like "*/scheduledqueryrules/*" -or
                           $_.OperationName.Value -like "*/alertRules/*" }

        if ($events) {
            $events |
                Select-Object EventTimestamp,
                    @{N="Rule";     E={$_.ResourceId -split "/" | Select-Object -Last 1}},
                    @{N="Status";   E={$_.Status.Value}},
                    @{N="Caller";   E={$_.Caller}} |
                Format-Table -AutoSize
        } else {
            Write-Host "   No alert activity found in the last ${LookbackHours}h." -ForegroundColor Yellow
        }
    }
}
