<#
.SYNOPSIS
    Removes all Azure monitoring resources deployed by 01_deploy_monitoring_stack.ps1.

.DESCRIPTION
    Deletes in reverse dependency order:
      1. Dashboard
      2. Alert Rules
      3. Action Group
      4. Application Insights
      5. Log Analytics Workspace
      6. (Optional) Resource Group itself

.PARAMETER SubscriptionId
    Azure Subscription ID.

.PARAMETER ResourceGroupName
    Resource group containing the monitoring resources.

.PARAMETER DeleteResourceGroup
    If specified, the entire resource group is deleted (faster, irreversible).

.EXAMPLE
    ./05_teardown_monitoring_stack.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring"

.EXAMPLE
    # Nuke the whole resource group
    ./05_teardown_monitoring_stack.ps1 `
        -SubscriptionId "..." `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -DeleteResourceGroup
#>

[CmdletBinding(SupportsShouldProcess, ConfirmImpact = "High")]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [switch]$DeleteResourceGroup
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-AzContext -SubscriptionId $SubscriptionId | Out-Null

function Remove-IfExists([scriptblock]$action, [string]$resourceLabel) {
    try {
        if ($PSCmdlet.ShouldProcess($resourceLabel, "Delete")) {
            & $action
            Write-Host "   [DELETED] $resourceLabel" -ForegroundColor Green
        }
    } catch {
        if ($_.Exception.Message -match "not found|NotFound|ResourceNotFound") {
            Write-Host "   [SKIP]    $resourceLabel (not found)" -ForegroundColor DarkGray
        } else {
            Write-Warning "   [ERROR]   $resourceLabel – $($_.Exception.Message)"
        }
    }
}

Write-Host "`n▶  Tearing down monitoring stack in '$ResourceGroupName'" -ForegroundColor Cyan

if ($DeleteResourceGroup) {
    if ($PSCmdlet.ShouldProcess($ResourceGroupName, "Delete entire resource group")) {
        Remove-AzResourceGroup -Name $ResourceGroupName -Force | Out-Null
        Write-Host "   [DELETED] Resource group $ResourceGroupName" -ForegroundColor Green
    }
    exit 0
}

# Dashboard
Remove-IfExists {
    Remove-AzResource `
        -ResourceGroupName $ResourceGroupName `
        -ResourceType "Microsoft.Portal/dashboards" `
        -ResourceName "dash-ecommerce-pipeline" `
        -Force
} "Portal Dashboard: dash-ecommerce-pipeline"

# Alert Rules
foreach ($ruleName in @(
    "alert-pipeline-failure",
    "alert-quality-check-fail",
    "alert-high-null-rate",
    "alert-long-pipeline-duration"
)) {
    Remove-IfExists {
        Remove-AzScheduledQueryRule `
            -ResourceGroupName $ResourceGroupName `
            -Name $ruleName
    } "Alert Rule: $ruleName"
}

# Action Group
Remove-IfExists {
    Remove-AzActionGroup `
        -ResourceGroupName $ResourceGroupName `
        -Name "ag-ecommerce-pipeline-ops"
} "Action Group: ag-ecommerce-pipeline-ops"

# Application Insights
Remove-IfExists {
    Remove-AzApplicationInsights `
        -ResourceGroupName $ResourceGroupName `
        -Name "appi-ecommerce-pipeline"
} "Application Insights: appi-ecommerce-pipeline"

# Log Analytics Workspace
Remove-IfExists {
    Remove-AzOperationalInsightsWorkspace `
        -ResourceGroupName $ResourceGroupName `
        -Name "law-ecommerce-pipeline" `
        -ForceDelete
} "Log Analytics Workspace: law-ecommerce-pipeline"

Write-Host "`n   Teardown complete." -ForegroundColor Yellow
