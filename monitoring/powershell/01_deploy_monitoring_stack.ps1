<#
.SYNOPSIS
    End-to-end deployment of the Azure monitoring stack for the e-commerce orders pipeline.
    Migrated from: AWS CloudWatch + SNS.

.DESCRIPTION
    Runs in order:
      1. Validates required parameters and Az module presence.
      2. Authenticates to Azure (interactive or service principal).
      3. Creates the resource group if it does not exist.
      4. Deploys Log Analytics Workspace (ARM).
      5. Deploys Application Insights linked to the workspace (ARM).
      6. Deploys Action Group with email/webhook receivers (ARM).
      7. Deploys all Scheduled Query alert rules (ARM).
      8. Deploys the shared Azure Portal dashboard (ARM).
      9. Emits the Application Insights connection string so the pipeline can
         be configured with it (written to .env and to Key Vault if desired).

.PARAMETER SubscriptionId
    Azure Subscription ID.

.PARAMETER ResourceGroupName
    Name of the resource group to deploy into (created if absent).

.PARAMETER Location
    Azure region (e.g. "eastus", "westeurope").

.PARAMETER AlertEmail
    Email address that will receive alert notifications.

.PARAMETER KeyVaultName
    (Optional) Name of an existing Key Vault to store the App Insights
    connection string as a secret.

.PARAMETER ServicePrincipalId
    (Optional) Client ID for non-interactive (CI/CD) login.

.PARAMETER ServicePrincipalSecret
    (Optional) Client secret for non-interactive login.

.PARAMETER TenantId
    (Optional) Tenant ID, required when using a service principal.

.EXAMPLE
    # Interactive login
    ./01_deploy_monitoring_stack.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -Location "eastus" `
        -AlertEmail "dataeng@example.com"

.EXAMPLE
    # CI/CD (service principal)
    ./01_deploy_monitoring_stack.ps1 `
        -SubscriptionId "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" `
        -ResourceGroupName "rg-ecommerce-pipeline-monitoring" `
        -Location "eastus" `
        -AlertEmail "dataeng@example.com" `
        -ServicePrincipalId  $env:AZURE_CLIENT_ID `
        -ServicePrincipalSecret $env:AZURE_CLIENT_SECRET `
        -TenantId $env:AZURE_TENANT_ID
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [Parameter(Mandatory)][string]$Location,
    [Parameter(Mandatory)][string]$AlertEmail,
    [string]$KeyVaultName      = "",
    [string]$ServicePrincipalId     = "",
    [string]$ServicePrincipalSecret = "",
    [string]$TenantId               = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Helpers ──────────────────────────────────────────────────────────────────

function Write-Step([string]$msg) {
    Write-Host "`n▶  $msg" -ForegroundColor Cyan
}

function Write-Success([string]$msg) {
    Write-Host "   ✔  $msg" -ForegroundColor Green
}

function Write-Warn([string]$msg) {
    Write-Warning "   ⚠  $msg"
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TemplatesDir = Join-Path $ScriptDir "..\arm_templates"

# ── 1. Validate prerequisites ─────────────────────────────────────────────────

Write-Step "Validating prerequisites"

if (-not (Get-Module -ListAvailable -Name "Az.Accounts")) {
    throw "Az PowerShell module not found. Install with: Install-Module -Name Az -Repository PSGallery -Force"
}
Write-Success "Az PowerShell module found"

foreach ($tmpl in @("log_analytics_workspace.json","application_insights.json","action_group.json","alert_rules.json","dashboard.json")) {
    $p = Join-Path $TemplatesDir $tmpl
    if (-not (Test-Path $p)) { throw "Missing ARM template: $p" }
}
Write-Success "All ARM templates present"

# ── 2. Authenticate ───────────────────────────────────────────────────────────

Write-Step "Authenticating to Azure"

if ($ServicePrincipalId -and $ServicePrincipalSecret -and $TenantId) {
    $secureSecret = ConvertTo-SecureString $ServicePrincipalSecret -AsPlainText -Force
    $cred = New-Object System.Management.Automation.PSCredential($ServicePrincipalId, $secureSecret)
    Connect-AzAccount -ServicePrincipal -Credential $cred -Tenant $TenantId | Out-Null
    Write-Success "Logged in as service principal $ServicePrincipalId"
} else {
    Connect-AzAccount | Out-Null
    Write-Success "Logged in interactively"
}

Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
Write-Success "Subscription set to $SubscriptionId"

# ── 3. Resource group ─────────────────────────────────────────────────────────

Write-Step "Ensuring resource group '$ResourceGroupName' exists"

$rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
if (-not $rg) {
    if ($PSCmdlet.ShouldProcess($ResourceGroupName, "Create resource group")) {
        New-AzResourceGroup -Name $ResourceGroupName -Location $Location -Tag @{
            project        = "ecommerce-orders-pipeline"
            environment    = "production"
            migrated_from  = "aws"
        } | Out-Null
        Write-Success "Resource group created"
    }
} else {
    Write-Success "Resource group already exists"
}

# ── 4. Log Analytics Workspace ────────────────────────────────────────────────

Write-Step "Deploying Log Analytics Workspace"

$lawDeployment = New-AzResourceGroupDeployment `
    -ResourceGroupName $ResourceGroupName `
    -TemplateFile (Join-Path $TemplatesDir "log_analytics_workspace.json") `
    -workspaceName "law-ecommerce-pipeline" `
    -location $Location `
    -retentionInDays 90 `
    -dailyQuotaGb 5 `
    -Verbose:$false

$lawId            = $lawDeployment.Outputs["workspaceId"].Value
$lawCustomerId    = $lawDeployment.Outputs["workspaceCustomerId"].Value
Write-Success "Log Analytics Workspace deployed: $lawId"

# ── 5. Application Insights ───────────────────────────────────────────────────

Write-Step "Deploying Application Insights"

$appiDeployment = New-AzResourceGroupDeployment `
    -ResourceGroupName $ResourceGroupName `
    -TemplateFile (Join-Path $TemplatesDir "application_insights.json") `
    -appInsightsName "appi-ecommerce-pipeline" `
    -location $Location `
    -logAnalyticsWorkspaceId $lawId `
    -samplingPercentage 100 `
    -Verbose:$false

$appiId               = $appiDeployment.Outputs["appInsightsId"].Value
$appiConnectionString = $appiDeployment.Outputs["connectionString"].Value
Write-Success "Application Insights deployed: $appiId"

# ── 6. Action Group ───────────────────────────────────────────────────────────

Write-Step "Deploying Action Group"

$emailReceivers = @(
    @{
        name               = "DataEngTeam"
        emailAddress       = $AlertEmail
        useCommonAlertSchema = $true
    }
) | ConvertTo-Json -Compress

$agDeployment = New-AzResourceGroupDeployment `
    -ResourceGroupName $ResourceGroupName `
    -TemplateFile (Join-Path $TemplatesDir "action_group.json") `
    -actionGroupName "ag-ecommerce-pipeline-ops" `
    -actionGroupShortName "ecomm-ops" `
    -emailReceivers $emailReceivers `
    -Verbose:$false

$agId = $agDeployment.Outputs["actionGroupId"].Value
Write-Success "Action Group deployed: $agId"

# ── 7. Alert Rules ────────────────────────────────────────────────────────────

Write-Step "Deploying Alert Rules"

New-AzResourceGroupDeployment `
    -ResourceGroupName $ResourceGroupName `
    -TemplateFile (Join-Path $TemplatesDir "alert_rules.json") `
    -location $Location `
    -logAnalyticsWorkspaceId $lawId `
    -appInsightsId $appiId `
    -actionGroupId $agId `
    -Verbose:$false | Out-Null

Write-Success "All 4 alert rules deployed"

# ── 8. Dashboard ──────────────────────────────────────────────────────────────

Write-Step "Deploying Azure Portal Dashboard"

New-AzResourceGroupDeployment `
    -ResourceGroupName $ResourceGroupName `
    -TemplateFile (Join-Path $TemplatesDir "dashboard.json") `
    -dashboardName "dash-ecommerce-pipeline" `
    -location $Location `
    -appInsightsId $appiId `
    -logAnalyticsWorkspaceId $lawId `
    -Verbose:$false | Out-Null

Write-Success "Dashboard deployed"

# ── 9. Persist connection string ──────────────────────────────────────────────

Write-Step "Persisting Application Insights connection string"

# Write to .env in the project root so the Python pipeline can pick it up
$envFile = Join-Path $ScriptDir "..\..\\.env"
$envLine = "APPLICATIONINSIGHTS_CONNECTION_STRING=$appiConnectionString"

if (Test-Path $envFile) {
    # Update or append
    $content = Get-Content $envFile
    if ($content -match "^APPLICATIONINSIGHTS_CONNECTION_STRING=") {
        $content = $content -replace "^APPLICATIONINSIGHTS_CONNECTION_STRING=.*", $envLine
    } else {
        $content += $envLine
    }
    Set-Content $envFile $content
} else {
    $envLine | Set-Content $envFile
}
Write-Success "Written to $envFile"

# Optionally store in Key Vault
if ($KeyVaultName) {
    $secretValue = ConvertTo-SecureString $appiConnectionString -AsPlainText -Force
    Set-AzKeyVaultSecret -VaultName $KeyVaultName `
        -Name "APPLICATIONINSIGHTS-CONNECTION-STRING" `
        -SecretValue $secretValue | Out-Null
    Write-Success "Stored in Key Vault '$KeyVaultName'"
}

# ── Summary ───────────────────────────────────────────────────────────────────

Write-Host "`n═══════════════════════════════════════════════════════════" -ForegroundColor Yellow
Write-Host " Deployment complete. Summary:" -ForegroundColor Yellow
Write-Host "   Resource Group  : $ResourceGroupName" -ForegroundColor Yellow
Write-Host "   Location        : $Location" -ForegroundColor Yellow
Write-Host "   LAW ID          : $lawId" -ForegroundColor Yellow
Write-Host "   App Insights ID : $appiId" -ForegroundColor Yellow
Write-Host "   Action Group ID : $agId" -ForegroundColor Yellow
Write-Host "   Connection Str  : $appiConnectionString" -ForegroundColor Yellow
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Yellow
