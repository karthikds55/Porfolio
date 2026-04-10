<#
.SYNOPSIS
    Fine-tune or update Azure Monitor alert rule thresholds after initial setup.
    Run this after 01_setup_azure_monitoring.ps1.

.DESCRIPTION
    Provides functions to enable/disable individual alert rules, adjust thresholds,
    change evaluation windows, and reassign action groups — without needing to
    redeploy the full ARM template.

    AWS equivalents updated here:
      - CloudWatch Alarm thresholds  →  Azure Scheduled Query Rule criteria
      - CloudWatch Alarm actions     →  Azure Action Group assignments

.PARAMETER SubscriptionId   Azure subscription ID.
.PARAMETER ResourceGroupName Resource group containing the alert rules.
.PARAMETER Environment       dev | staging | production (affects default thresholds).

.EXAMPLE
    .\02_configure_alert_rules.ps1 `
        -SubscriptionId "xxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-prod" `
        -Environment "production"
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)] [string]$SubscriptionId,
    [Parameter(Mandatory)] [string]$ResourceGroupName,
    [Parameter(Mandatory = $false)]
    [ValidateSet("dev","staging","production")]
    [string]$Environment = "production"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message, [string]$Color = "Cyan")
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message" -ForegroundColor $Color
}

az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) { throw "az login required." }

# ── Environment-specific thresholds ──────────────────────────────────────────
# In production we alert on any single failure; in dev/staging we allow more noise.
$Thresholds = switch ($Environment) {
    "production" { @{ QualityFail = 1; IngestErrors = 1; MaxDurationSec = 600  } }
    "staging"    { @{ QualityFail = 3; IngestErrors = 2; MaxDurationSec = 900  } }
    "dev"        { @{ QualityFail = 5; IngestErrors = 5; MaxDurationSec = 1800 } }
}

Write-Step "Applying '$Environment' thresholds: $($Thresholds | ConvertTo-Json -Compress)"

# ─────────────────────────────────────────────────────────────────────────────
# Helper: update a scheduled query rule's threshold value
# ─────────────────────────────────────────────────────────────────────────────
function Update-AlertThreshold {
    param(
        [string]$AlertName,
        [double]$NewThreshold,
        [string]$Operator = "GreaterThanOrEqual"
    )
    Write-Step "  Updating '$AlertName' threshold → $Operator $NewThreshold"
    # Azure CLI does not expose a direct --threshold flag for scheduled query rules;
    # we patch via the REST API using az rest.
    $ResourceId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName" +
                  "/providers/Microsoft.Insights/scheduledQueryRules/$AlertName"
    $PatchBody = @{
        properties = @{
            criteria = @{
                allOf = @(
                    @{
                        operator  = $Operator
                        threshold = $NewThreshold
                    }
                )
            }
        }
    } | ConvertTo-Json -Depth 10

    az rest --method PATCH `
        --url "https://management.azure.com${ResourceId}?api-version=2023-03-15-preview" `
        --body $PatchBody | Out-Null
    Write-Step "  Done." "Green"
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: enable or disable an alert rule
# ─────────────────────────────────────────────────────────────────────────────
function Set-AlertEnabled {
    param([string]$AlertName, [bool]$Enabled)
    $State = if ($Enabled) { "Enabled" } else { "Disabled" }
    Write-Step "  Setting '$AlertName' → $State"
    $ResourceId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName" +
                  "/providers/Microsoft.Insights/scheduledQueryRules/$AlertName"
    $PatchBody = @{ properties = @{ enabled = $Enabled } } | ConvertTo-Json -Depth 5
    az rest --method PATCH `
        --url "https://management.azure.com${ResourceId}?api-version=2023-03-15-preview" `
        --body $PatchBody | Out-Null
    Write-Step "  Done." "Green"
}

# ─────────────────────────────────────────────────────────────────────────────
# Apply thresholds
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Configuring alert thresholds for environment: $Environment"

Update-AlertThreshold -AlertName "alert-quality-check-failures"  -NewThreshold $Thresholds.QualityFail
Update-AlertThreshold -AlertName "alert-pipeline-ingest-errors"  -NewThreshold $Thresholds.IngestErrors
Update-AlertThreshold -AlertName "alert-transform-errors"        -NewThreshold $Thresholds.IngestErrors
Update-AlertThreshold -AlertName "alert-long-running-pipeline"   -NewThreshold $Thresholds.MaxDurationSec `
                      -Operator "GreaterThan"

# ─────────────────────────────────────────────────────────────────────────────
# Disable noisy alerts in non-production
# ─────────────────────────────────────────────────────────────────────────────
if ($Environment -ne "production") {
    Write-Step "Non-production: disabling 'pipeline stale' alert to reduce noise."
    Set-AlertEnabled -AlertName "alert-pipeline-stale" -Enabled $false
} else {
    Set-AlertEnabled -AlertName "alert-pipeline-stale" -Enabled $true
}

# ─────────────────────────────────────────────────────────────────────────────
# List current alert rules for confirmation
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Current alert rules in resource group:"
az monitor scheduled-query list `
    --resource-group $ResourceGroupName `
    --query "[].{Name:name, Enabled:properties.enabled, Severity:properties.severity}" `
    --output table

Write-Step "Alert rule configuration complete." "Green"
