<#
.SYNOPSIS
    End-to-end Azure Monitor infrastructure setup for the ecommerce orders pipeline.
    Migrates from AWS CloudWatch to Azure Monitor (Log Analytics + App Insights + Storage + Alerts).

.DESCRIPTION
    Deploys all Azure Monitor resources in the correct order:
      1. Resource Group
      2. Log Analytics Workspace  (replaces CloudWatch Log Groups)
      3. Application Insights     (replaces AWS X-Ray + CloudWatch custom metrics)
      4. Storage Account          (replaces S3 log archive bucket)
      5. Action Groups            (replaces SNS topics)
      6. Alert Rules              (replaces CloudWatch Alarms)
      7. Dashboard                (replaces CloudWatch Dashboard)

.PARAMETER SubscriptionId
    Azure subscription ID to deploy resources into.

.PARAMETER ResourceGroupName
    Resource group name (created if it does not exist).

.PARAMETER Location
    Azure region (e.g. eastus, westus2, uksouth).

.PARAMETER Environment
    Deployment environment label: dev | staging | production.

.PARAMETER NotificationEmail
    Primary email address for alert notifications (replaces SNS subscriber).

.PARAMETER TeamsWebhookUrl
    Microsoft Teams incoming webhook URL for pipeline alert notifications.

.EXAMPLE
    .\01_setup_azure_monitoring.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-prod" `
        -Location "eastus" `
        -Environment "production" `
        -NotificationEmail "data-engineering@example.com" `
        -TeamsWebhookUrl "https://outlook.office.com/webhook/..."
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)]
    [string]$SubscriptionId,

    [Parameter(Mandatory)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory)]
    [ValidateSet("eastus","eastus2","westus","westus2","uksouth","ukwest","northeurope","westeurope","australiaeast")]
    [string]$Location,

    [Parameter(Mandatory = $false)]
    [ValidateSet("dev","staging","production")]
    [string]$Environment = "production",

    [Parameter(Mandatory)]
    [string]$NotificationEmail,

    [Parameter(Mandatory = $false)]
    [string]$TeamsWebhookUrl = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Resource name convention ──────────────────────────────────────────────────
$Suffix          = $Environment.Substring(0,4)          # prod | stag | dev
$WorkspaceName   = "law-ecommerce-pipeline-$Suffix"
$AppInsightsName = "appi-ecommerce-pipeline-$Suffix"
$StorageName     = "stecommpipeline$Suffix"             # storage names: lowercase, no hyphens
$DashboardName   = "dash-ecommerce-pipeline-$Suffix"

$TemplateDir     = Join-Path $PSScriptRoot ".." "arm_templates"

# ── Helper: write timestamped log line ───────────────────────────────────────
function Write-Step {
    param([string]$Message, [string]$Color = "Cyan")
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message" -ForegroundColor $Color
}

# ─────────────────────────────────────────────────────────────────────────────
# 0. Authenticate & set subscription
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Setting active subscription to $SubscriptionId"
az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) { throw "Failed to set subscription. Run 'az login' first." }

# ─────────────────────────────────────────────────────────────────────────────
# 1. Resource Group
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Ensuring resource group '$ResourceGroupName' exists in '$Location'"
az group create `
    --name $ResourceGroupName `
    --location $Location `
    --tags "project=ecommerce-orders-pipeline" "environment=$Environment" "migrated_from=aws" | Out-Null
Write-Step "Resource group ready." "Green"

# ─────────────────────────────────────────────────────────────────────────────
# 2. Log Analytics Workspace  (AWS: CloudWatch Log Groups)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Deploying Log Analytics Workspace '$WorkspaceName'  [AWS equiv: CloudWatch Log Groups]"
$lawDeployment = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file "$TemplateDir/log_analytics_workspace.json" `
    --parameters "workspaceName=$WorkspaceName" "location=$Location" `
    --output json | ConvertFrom-Json

$WorkspaceId = $lawDeployment.properties.outputs.workspaceId.value
Write-Step "Log Analytics Workspace deployed. ID: $WorkspaceId" "Green"

# ─────────────────────────────────────────────────────────────────────────────
# 3. Application Insights  (AWS: X-Ray + CloudWatch custom metrics)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Deploying Application Insights '$AppInsightsName'  [AWS equiv: X-Ray + CloudWatch custom metrics]"
$appiDeployment = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file "$TemplateDir/application_insights.json" `
    --parameters "appInsightsName=$AppInsightsName" "location=$Location" "logAnalyticsWorkspaceId=$WorkspaceId" `
    --output json | ConvertFrom-Json

$AppInsightsId         = $appiDeployment.properties.outputs.appInsightsId.value
$InstrumentationKey    = $appiDeployment.properties.outputs.instrumentationKey.value
$AppInsightsConnString = $appiDeployment.properties.outputs.connectionString.value
Write-Step "Application Insights deployed." "Green"
Write-Step "  Instrumentation Key  : $InstrumentationKey" "Yellow"
Write-Step "  Connection String    : $AppInsightsConnString" "Yellow"
Write-Step "  -> Store these in your pipeline environment as APPLICATIONINSIGHTS_CONNECTION_STRING" "Yellow"

# ─────────────────────────────────────────────────────────────────────────────
# 4. Storage Account  (AWS: S3 bucket ecommerce-pipeline-logs-archive)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Deploying Storage Account '$StorageName'  [AWS equiv: S3 log-archive bucket]"
$storDeployment = az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file "$TemplateDir/storage_account.json" `
    --parameters "storageAccountName=$StorageName" "location=$Location" `
    --output json | ConvertFrom-Json

$StorageId = $storDeployment.properties.outputs.storageAccountId.value
Write-Step "Storage Account deployed. ID: $StorageId" "Green"

# ─────────────────────────────────────────────────────────────────────────────
# 5. Action Groups  (AWS: SNS topics)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Creating Action Groups (critical + warning)  [AWS equiv: SNS topics]"

# Critical action group
$criticalAgJson = az monitor action-group create `
    --resource-group $ResourceGroupName `
    --name "ag-pipeline-critical" `
    --short-name "PipeCrit" `
    --action email DataEngEmail $NotificationEmail `
    --tags "project=ecommerce-orders-pipeline" "environment=$Environment" "migrated_from=aws" `
    --output json | ConvertFrom-Json
$CriticalAgId = $criticalAgJson.id

# Warning action group
$warningAgJson = az monitor action-group create `
    --resource-group $ResourceGroupName `
    --name "ag-pipeline-warning" `
    --short-name "PipeWarn" `
    --action email DataEngEmail $NotificationEmail `
    --tags "project=ecommerce-orders-pipeline" "environment=$Environment" "migrated_from=aws" `
    --output json | ConvertFrom-Json
$WarningAgId = $warningAgJson.id

# Attach Teams webhook if provided
if ($TeamsWebhookUrl -ne "") {
    Write-Step "  Attaching Teams webhook to action groups"
    az monitor action-group update `
        --resource-group $ResourceGroupName `
        --name "ag-pipeline-critical" `
        --add-action webhook TeamsAlert "$TeamsWebhookUrl" --use-common-alert-schema true | Out-Null
    az monitor action-group update `
        --resource-group $ResourceGroupName `
        --name "ag-pipeline-warning" `
        --add-action webhook TeamsAlert "$TeamsWebhookUrl" --use-common-alert-schema true | Out-Null
}
Write-Step "Action Groups created." "Green"

# ─────────────────────────────────────────────────────────────────────────────
# 6. Alert Rules  (AWS: CloudWatch Alarms)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Deploying Alert Rules  [AWS equiv: CloudWatch Alarms]"
az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file "$TemplateDir/alert_rules.json" `
    --parameters `
        "location=$Location" `
        "logAnalyticsWorkspaceId=$WorkspaceId" `
        "criticalActionGroupId=$CriticalAgId" `
        "warningActionGroupId=$WarningAgId" `
    --output none
Write-Step "Alert Rules deployed." "Green"

# ─────────────────────────────────────────────────────────────────────────────
# 7. Dashboard  (AWS: CloudWatch Dashboard)
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "Deploying Portal Dashboard '$DashboardName'  [AWS equiv: CloudWatch Dashboard]"
az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file "$TemplateDir/dashboard.json" `
    --parameters `
        "dashboardName=$DashboardName" `
        "location=$Location" `
        "logAnalyticsWorkspaceId=$WorkspaceId" `
        "appInsightsId=$AppInsightsId" `
    --output none
Write-Step "Dashboard deployed." "Green"

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "================================================================" "Magenta"
Write-Step " MIGRATION COMPLETE – AWS → Azure Monitor" "Magenta"
Write-Step "================================================================" "Magenta"
Write-Host ""
Write-Host "Resource Group          : $ResourceGroupName"
Write-Host "Log Analytics Workspace : $WorkspaceName"
Write-Host "Application Insights    : $AppInsightsName"
Write-Host "Storage Account         : $StorageName"
Write-Host "Dashboard               : $DashboardName"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Set the pipeline env var:"
Write-Host "       APPLICATIONINSIGHTS_CONNECTION_STRING=$AppInsightsConnString"
Write-Host "  2. Run 02_configure_alert_rules.ps1 to fine-tune thresholds."
Write-Host "  3. Run 03_pipeline_health_check.ps1 to verify connectivity."
Write-Host "  4. Decommission AWS CloudWatch resources once validated."
Write-Host ""
