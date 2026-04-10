<#
.SYNOPSIS
    Decommission legacy AWS CloudWatch monitoring resources after Azure migration is validated.

.DESCRIPTION
    SAFE TEARDOWN SCRIPT. Removes only the CloudWatch/SNS/S3 monitoring resources
    that were migrated to Azure. Does NOT touch compute, databases, or application
    resources.

    Resources removed:
      - CloudWatch Log Groups      (/ecommerce/pipeline/*)
      - CloudWatch Alarms          (PipelineIngestErrors, DataQualityFailures, PipelineStale)
      - CloudWatch Dashboard       (EcommerceOrdersPipeline)
      - SNS Topic                  (pipeline-alerts)
      - S3 Bucket lifecycle policy removal + optional bucket deletion

    Prerequisites:
      - AWS CLI v2 installed and configured (aws configure)
      - Sufficient IAM permissions: logs:*, cloudwatch:*, sns:*, s3:*
      - Run 03_pipeline_health_check.ps1 FIRST to confirm Azure stack is healthy

.PARAMETER AwsRegion         AWS region where resources are deployed.
.PARAMETER AwsProfile        Named AWS CLI profile (default: default).
.PARAMETER S3BucketName      Legacy S3 log-archive bucket name.
.PARAMETER DeleteS3Bucket    If specified, the S3 bucket is emptied and deleted.
                             CAUTION: irreversible. Only use after confirming logs
                             have been archived to Azure Blob Storage.
.PARAMETER DryRun            Print what would be deleted without actually deleting.

.EXAMPLE
    # Dry run first
    .\05_cleanup_aws_resources.ps1 -AwsRegion us-east-1 -DryRun

    # Full cleanup (keep S3 bucket)
    .\05_cleanup_aws_resources.ps1 -AwsRegion us-east-1

    # Full cleanup including S3 bucket deletion
    .\05_cleanup_aws_resources.ps1 -AwsRegion us-east-1 -DeleteS3Bucket
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)]
    [string]$AwsRegion,

    [Parameter(Mandatory = $false)]
    [string]$AwsProfile = "default",

    [Parameter(Mandatory = $false)]
    [string]$S3BucketName = "ecommerce-pipeline-logs-archive",

    [switch]$DeleteS3Bucket,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$AwsCommon = "--region $AwsRegion --profile $AwsProfile"

function Write-Step { param([string]$M, [string]$C = "Cyan") Write-Host "[$(Get-Date -Format 'HH:mm:ss')] $M" -ForegroundColor $C }
function Invoke-AwsCmd {
    param([string]$Cmd)
    if ($DryRun) {
        Write-Host "  [DRY RUN] aws $Cmd" -ForegroundColor DarkGray
    } else {
        $result = Invoke-Expression "aws $Cmd"
        return $result
    }
}

if ($DryRun) {
    Write-Host ""
    Write-Host "╔══════════════════════════════════════════════╗" -ForegroundColor Yellow
    Write-Host "║  DRY RUN MODE – No resources will be deleted ║" -ForegroundColor Yellow
    Write-Host "╚══════════════════════════════════════════════╝" -ForegroundColor Yellow
    Write-Host ""
}

# ─────────────────────────────────────────────────────────────────────────────
# Validate Azure migration health before proceeding
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "IMPORTANT: Confirm you have run 03_pipeline_health_check.ps1 and all checks PASSED before continuing." "Yellow"
if (-not $DryRun) {
    $confirm = Read-Host "Type YES to confirm Azure monitoring is healthy and you want to proceed"
    if ($confirm -ne "YES") {
        Write-Host "Aborted." -ForegroundColor Red
        exit 1
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. CloudWatch Log Groups
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "1. Deleting CloudWatch Log Groups"
$logGroups = @(
    "/ecommerce/pipeline/ingest",
    "/ecommerce/pipeline/transform",
    "/ecommerce/pipeline/quality"
)
foreach ($lg in $logGroups) {
    Write-Step "  Deleting log group: $lg"
    Invoke-AwsCmd "logs delete-log-group --log-group-name '$lg' $AwsCommon"
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. CloudWatch Alarms
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "2. Deleting CloudWatch Alarms"
$alarms = @(
    "PipelineIngestErrors",
    "DataQualityFailures",
    "PipelineStale",
    "PipelineTransformErrors"
)
$alarmList = $alarms -join " "
Invoke-AwsCmd "cloudwatch delete-alarms --alarm-names $alarmList $AwsCommon"

# ─────────────────────────────────────────────────────────────────────────────
# 3. CloudWatch Dashboard
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "3. Deleting CloudWatch Dashboard"
Invoke-AwsCmd "cloudwatch delete-dashboards --dashboard-names EcommerceOrdersPipeline $AwsCommon"

# ─────────────────────────────────────────────────────────────────────────────
# 4. SNS Topic
# ─────────────────────────────────────────────────────────────────────────────
Write-Step "4. Removing SNS Topic subscriptions and topic"
if (-not $DryRun) {
    $topicArn = aws sns list-topics $AwsCommon --query "Topics[?contains(TopicArn,'pipeline-alerts')].TopicArn" --output text
    if ($topicArn) {
        # Unsubscribe all subscribers
        $subs = aws sns list-subscriptions-by-topic --topic-arn $topicArn $AwsCommon --query "Subscriptions[].SubscriptionArn" --output text
        foreach ($sub in ($subs -split "`t")) {
            if ($sub -and $sub -ne "PendingConfirmation") {
                Invoke-AwsCmd "sns unsubscribe --subscription-arn '$sub' $AwsCommon"
            }
        }
        Invoke-AwsCmd "sns delete-topic --topic-arn '$topicArn' $AwsCommon"
        Write-Step "  SNS topic deleted: $topicArn"
    } else {
        Write-Step "  SNS topic 'pipeline-alerts' not found – skipping." "Yellow"
    }
} else {
    Write-Host "  [DRY RUN] Would delete SNS topic 'pipeline-alerts' and all subscriptions." -ForegroundColor DarkGray
}

# ─────────────────────────────────────────────────────────────────────────────
# 5. S3 Bucket  (optional)
# ─────────────────────────────────────────────────────────────────────────────
if ($DeleteS3Bucket) {
    Write-Step "5. Emptying and deleting S3 bucket '$S3BucketName'" "Red"
    Write-Step "   WARNING: This is irreversible." "Red"
    if (-not $DryRun) {
        $s3confirm = Read-Host "Type DELETE to confirm S3 bucket deletion"
        if ($s3confirm -eq "DELETE") {
            Invoke-AwsCmd "s3 rm s3://$S3BucketName --recursive $AwsCommon"
            Invoke-AwsCmd "s3api delete-bucket --bucket $S3BucketName $AwsCommon"
            Write-Step "  S3 bucket deleted." "Green"
        } else {
            Write-Step "  S3 bucket deletion skipped (confirmation not received)." "Yellow"
        }
    } else {
        Write-Host "  [DRY RUN] Would empty and delete S3 bucket '$S3BucketName'." -ForegroundColor DarkGray
    }
} else {
    Write-Step "5. S3 bucket '$S3BucketName' retained (use -DeleteS3Bucket to remove)." "Yellow"
}

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Step "════════════════════════════════════════════" "Magenta"
if ($DryRun) {
    Write-Step " DRY RUN COMPLETE – No changes made." "Magenta"
} else {
    Write-Step " AWS CloudWatch cleanup complete." "Magenta"
    Write-Host " Migrated resources are now exclusively on Azure Monitor."
}
Write-Step "════════════════════════════════════════════" "Magenta"
