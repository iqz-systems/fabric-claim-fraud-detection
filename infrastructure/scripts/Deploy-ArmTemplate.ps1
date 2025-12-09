<#
.SYNOPSIS
    Deploys ARM templates to Azure with enterprise best practices

.DESCRIPTION
    This script deploys ARM templates with proper validation, error handling, and logging.
    Supports both local file deployments and linked template deployments.

.PARAMETER ResourceGroupName
    Name of the resource group to deploy to

.PARAMETER TemplateFile
    Path to the main ARM template file

.PARAMETER ParametersFile
    Path to the parameters file

.PARAMETER Environment
    Environment name (dev, prod)

.PARAMETER Location
    Azure region for deployment

.PARAMETER WhatIf
    Run in What-If mode to preview changes

.PARAMETER ValidateOnly
    Only validate the template without deploying

.EXAMPLE
    .\Deploy-ArmTemplate.ps1 -ResourceGroupName "fraud-claim-detection-dev" -Environment "dev"
#>

[CmdletBinding(SupportsShouldProcess=$true)]
param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$false)]
    [ValidateSet("dev", "prod")]
    [string]$Environment = "dev",
    
    [Parameter(Mandatory=$false)]
    [string]$Location = "eastus",
    
    [Parameter(Mandatory=$false)]
    [string]$TemplateFile = "infrastructure\arm-templates\azuredeploy.json",
    
    [Parameter(Mandatory=$false)]
    [string]$ParametersFile = "infrastructure\arm-templates\environments\$Environment\azuredeploy.parameters.json",
    
    [Parameter(Mandatory=$false)]
    [switch]$WhatIf,
    
    [Parameter(Mandatory=$false)]
    [switch]$ValidateOnly
)

# Set error action preference
$ErrorActionPreference = "Stop"

# Import Azure module
try {
    Import-Module Az -ErrorAction Stop
} catch {
    Write-Error "Azure PowerShell module not found. Install it using: Install-Module -Name Az -AllowClobber"
    exit 1
}

# Check Azure login
try {
    $context = Get-AzContext -ErrorAction Stop
    Write-Host "Connected to Azure subscription: $($context.Subscription.Name)" -ForegroundColor Green
} catch {
    Write-Error "Not logged in to Azure. Please run: Connect-AzAccount"
    exit 1
}

# Validate template file exists
if (-not (Test-Path $TemplateFile)) {
    Write-Error "Template file not found: $TemplateFile"
    exit 1
}

# Validate parameters file exists
if (-not (Test-Path $ParametersFile)) {
    Write-Error "Parameters file not found: $ParametersFile"
    exit 1
}

# Get absolute paths
$TemplateFile = Resolve-Path $TemplateFile
$ParametersFile = Resolve-Path $ParametersFile

# Create resource group if it doesn't exist
$rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
if (-not $rg) {
    Write-Host "Creating resource group: $ResourceGroupName" -ForegroundColor Yellow
    if ($PSCmdlet.ShouldProcess($ResourceGroupName, "Create resource group")) {
        New-AzResourceGroup -Name $ResourceGroupName -Location $Location | Out-Null
        Write-Host "Resource group created successfully" -ForegroundColor Green
    }
} else {
    Write-Host "Resource group already exists: $ResourceGroupName" -ForegroundColor Green
}

# Prepare deployment parameters
$deploymentParams = @{
    ResourceGroupName     = $ResourceGroupName
    TemplateFile         = $TemplateFile
    TemplateParameterFile = $ParametersFile
    Name                 = "deployment-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
}

# Validate template
Write-Host "`nValidating template..." -ForegroundColor Yellow
try {
    $validation = Test-AzResourceGroupDeployment @deploymentParams -ErrorAction Stop
    if ($validation) {
        Write-Host "Template validation successful" -ForegroundColor Green
    }
} catch {
    Write-Error "Template validation failed: $_"
    exit 1
}

if ($ValidateOnly) {
    Write-Host "Validation only mode - exiting" -ForegroundColor Cyan
    exit 0
}

# What-If preview
if ($WhatIf) {
    Write-Host "`nRunning What-If analysis..." -ForegroundColor Cyan
    try {
        $deploymentParams['WhatIf'] = $true
        New-AzResourceGroupDeployment @deploymentParams
        Write-Host "`nWhat-If completed. Review the changes above." -ForegroundColor Yellow
        exit 0
    } catch {
        Write-Error "What-If failed: $_"
        exit 1
    }
}

# Deploy template
Write-Host "`nDeploying ARM template..." -ForegroundColor Yellow
try {
    if ($PSCmdlet.ShouldProcess($ResourceGroupName, "Deploy ARM template")) {
        $deployment = New-AzResourceGroupDeployment @deploymentParams -ErrorAction Stop
        
        Write-Host "`nDeployment completed successfully" -ForegroundColor Green
        Write-Host "Deployment Name: $($deployment.DeploymentName)" -ForegroundColor Cyan
        Write-Host "Provisioning State: $($deployment.ProvisioningState)" -ForegroundColor Cyan
        
        # Display outputs
        if ($deployment.Outputs) {
            Write-Host "`nDeployment Outputs:" -ForegroundColor Yellow
            $deployment.Outputs.PSObject.Properties | ForEach-Object {
                Write-Host "  $($_.Name): $($_.Value.Value)" -ForegroundColor Cyan
            }
        }
    }
} catch {
    Write-Error "Deployment failed: $_"
    
    # Get deployment details for troubleshooting
    Write-Host "`nRetrieving deployment details for troubleshooting..." -ForegroundColor Yellow
    $failedDeployment = Get-AzResourceGroupDeployment -ResourceGroupName $ResourceGroupName | Select-Object -First 1
    if ($failedDeployment) {
        Write-Host "Deployment Name: $($failedDeployment.DeploymentName)" -ForegroundColor Red
        Write-Host "Status: $($failedDeployment.ProvisioningState)" -ForegroundColor Red
    }
    
    exit 1
}

Write-Host "`nDone!" -ForegroundColor Green

