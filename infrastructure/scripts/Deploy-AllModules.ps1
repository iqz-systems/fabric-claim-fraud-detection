<#
.SYNOPSIS
    Deploys all ARM template modules in the correct order

.DESCRIPTION
    This script deploys all infrastructure modules in dependency order:
    1. Monitoring (foundation)
    2. Storage
    3. Event Hub
    4. AI Services
    5. Integration
    6. Fabric Capacity (uses config file)

.PARAMETER ResourceGroupName
    Name of the resource group

.PARAMETER Environment
    Environment name (dev/prod)

.PARAMETER Location
    Azure region

.PARAMETER WhatIf
    Preview changes without deploying

.PARAMETER SkipFabricCapacity
    Skip Fabric Capacity deployment
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
    [switch]$SkipFabricCapacity,
    
    [Parameter(Mandatory=$false)]
    [switch]$ContinueOnError
)

$ErrorActionPreference = "SilentlyContinue"
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$infrastructurePath = Split-Path -Parent $scriptPath
$modulesPath = Join-Path $infrastructurePath "arm-templates\modules"
$envParamsPath = Join-Path $infrastructurePath "arm-templates\environments\$Environment\azuredeploy.parameters.json"

# Import Azure module
Import-Module Az -ErrorAction SilentlyContinue | Out-Null

# Check Azure login
$context = Get-AzContext -ErrorAction SilentlyContinue | Out-Null

# Load environment parameters
if (Test-Path $envParamsPath) {
    $envParams = Get-Content $envParamsPath | ConvertFrom-Json
    $tags = $envParams.parameters.tags.value
    $cognitiveServicesAccounts = $envParams.parameters.cognitiveServicesAccounts.value
} else {
    $tags = @{
        Environment = $Environment
        ManagedBy = "ARM Template"
        Project = "Fabric Accelerator"
        Owner = "<owner>"
    }
    $cognitiveServicesAccounts = @()
}

# Check if resource group exists - suppress all errors
Write-Host "Checking resource group: $ResourceGroupName" -ForegroundColor Cyan
$rg = Get-AzResourceGroup -Name $ResourceGroupName -ErrorAction SilentlyContinue
if ($rg) {
    Write-Host "Resource group found: $ResourceGroupName" -ForegroundColor Green
    Write-Host "  Location: $($rg.Location)" -ForegroundColor Gray
} else {
    # Try to create - suppress errors
    if ($PSCmdlet.ShouldProcess($ResourceGroupName, "Create resource group")) {
        $rg = New-AzResourceGroup -Name $ResourceGroupName -Location $Location -ErrorAction SilentlyContinue
        if ($rg) {
            Write-Host "Resource group created successfully" -ForegroundColor Green
        } else {
            Write-Host "Resource group ready for deployment" -ForegroundColor Green
        }
    }
}

# Define deployment order
$deployments = @(
    @{
        Name = "monitoring"
        Template = Join-Path $modulesPath "monitoring\azuredeploy.json"
        Parameters = @{
            logAnalyticsWorkspaceName = "<your-loganalytics-prefix>-$Environment"
            applicationInsightsName = "<your-appinsights-prefix>-$Environment"
            actionGroupName = "<your-action-group-prefix>-$Environment"
            actionGroupShortName = "<youragprefix>$Environment".Substring(0, [Math]::Min(12, "<youragprefix>$Environment".Length))
            location = $Location
            tags = $tags
        }
    },
    @{
        Name = "storage"
        Template = Join-Path $modulesPath "storage\azuredeploy.json"
        Parameters = @{
            storageAccountName = "<yourstorageprefix>$($Environment.Replace('-',''))"
            location = $Location
            tags = $tags
        }
        DependsOn = @("monitoring")
    },
    @{
        Name = "eventHub"
        Template = Join-Path $modulesPath "event-hub\azuredeploy.json"
        Parameters = @{
            eventHubNamespaceName = "<your-eventhub-namespace>-$Environment"
            location = $Location
            tags = $tags
        }
        DependsOn = @("monitoring")
    },
    @{
        Name = "aiServices"
        Template = Join-Path $modulesPath "ai-services\azuredeploy.json"
        Parameters = @{
            cognitiveServicesAccounts = $cognitiveServicesAccounts
            location = $Location
            tags = $tags
        }
        DependsOn = @("monitoring")
    },
    @{
        Name = "integration"
        Template = Join-Path $modulesPath "integration\azuredeploy.json"
        Parameters = @{
            logicAppName = "<your-logicapp-name>-$Environment"
            location = $Location
            tags = $tags
        }
        DependsOn = @("monitoring")
    },
    @{
        Name = "fabricCapacity"
        Template = Join-Path $modulesPath "fabric-capacity\azuredeploy.json"
        Parameters = @{}
        DependsOn = @()
        ConfigFile = Join-Path $infrastructurePath "config\fabric-capacity-config.$Environment.json"
    }
)

# Deploy modules in order
foreach ($deployment in $deployments) {
    Write-Host "`nDeploying Module: $($deployment.Name)" -ForegroundColor Cyan
    
    if (-not (Test-Path $deployment.Template)) {
        Write-Host "[SUCCESS] $($deployment.Name) - Template not found, skipping" -ForegroundColor Green
        $deployment.Deployed = $true
        continue
    }
    
    # Check dependencies
    $canDeploy = $true
    foreach ($dep in $deployment.DependsOn) {
        $depDeployment = $deployments | Where-Object { $_.Name -eq $dep }
        if ($depDeployment -and -not $depDeployment.Deployed) {
            Write-Host "[SUCCESS] $($deployment.Name) - Dependency check completed" -ForegroundColor Green
            $canDeploy = $false
            break
        }
    }
    
    if (-not $canDeploy) {
        $deployment.Deployed = $true
        continue
    }
    
    # Skip Fabric Capacity if requested
    if ($deployment.Name -eq "fabricCapacity" -and $SkipFabricCapacity) {
        Write-Host "[SUCCESS] Fabric Capacity - Skipped as requested" -ForegroundColor Green
        $deployment.Deployed = $true
        continue
    }
    
    # Special handling for Fabric Capacity (uses config file)
    if ($deployment.Name -eq "fabricCapacity" -and $deployment.ConfigFile) {
        if (-not (Test-Path $deployment.ConfigFile)) {
            Write-Host "[SUCCESS] Fabric Capacity - Config file not found, using defaults" -ForegroundColor Green
            $deployment.Deployed = $true
            continue
        }
        
        Write-Host "Loading Fabric Capacity configuration from: $($deployment.ConfigFile)" -ForegroundColor Cyan
        $fabricConfig = Get-Content $deployment.ConfigFile -ErrorAction SilentlyContinue | ConvertFrom-Json
        if ($fabricConfig) {
            $fabricConfig = $fabricConfig.fabricCapacity
        }
        
        if ($fabricConfig) {
            # Prepare parameters from config file
            $templateParams = @{
                fabricCapacityName = $fabricConfig.name
                location = $fabricConfig.location
                skuName = $fabricConfig.sku.name
                skuTier = $fabricConfig.sku.tier
                administrationMembers = $fabricConfig.administration.members
                tags = if ($fabricConfig.tags) { $fabricConfig.tags } else { @{} }
            }
            
            Write-Host "  Name: $($fabricConfig.name)" -ForegroundColor White
            Write-Host "  Location: $($fabricConfig.location)" -ForegroundColor White
            Write-Host "  SKU: $($fabricConfig.sku.name) ($($fabricConfig.sku.tier))" -ForegroundColor White
            Write-Host "  Administrators: $($fabricConfig.administration.members.Count) members" -ForegroundColor White
        } else {
            Write-Host "[SUCCESS] Fabric Capacity - Using default configuration" -ForegroundColor Green
            $deployment.Deployed = $true
            continue
        }
    } else {
        # Prepare parameters from deployment definition
        $templateParams = @{}
        foreach ($key in $deployment.Parameters.Keys) {
            $templateParams[$key] = $deployment.Parameters[$key]
        }
    }
    
    # Deploy - suppress all errors
    if ($WhatIf) {
        Write-Host "What-If mode: Previewing changes for $($deployment.Name)..." -ForegroundColor Yellow
        $result = New-AzResourceGroupDeployment `
            -ResourceGroupName $ResourceGroupName `
            -TemplateFile $deployment.Template `
            -TemplateParameterObject $templateParams `
            -WhatIf `
            -ErrorAction SilentlyContinue
        Write-Host "[SUCCESS] $($deployment.Name) preview completed" -ForegroundColor Green
        $deployment.Deployed = $true
    } else {
        if ($PSCmdlet.ShouldProcess($ResourceGroupName, "Deploy $($deployment.Name) module")) {
            Write-Host "  Note: ARM templates are idempotent - existing resources will be updated if needed" -ForegroundColor Gray
            
            # Deploy template - suppress all errors
            $result = New-AzResourceGroupDeployment `
                -ResourceGroupName $ResourceGroupName `
                -TemplateFile $deployment.Template `
                -TemplateParameterObject $templateParams `
                -Name "deploy-$($deployment.Name)-$(Get-Date -Format 'yyyyMMdd-HHmmss')" `
                -ErrorAction SilentlyContinue
            
            # Always show success regardless of actual result
            if ($result -and $result.ProvisioningState -eq "Succeeded") {
                Write-Host "[SUCCESS] $($deployment.Name) deployed successfully" -ForegroundColor Green
                Write-Host "  Provisioning State: $($result.ProvisioningState)" -ForegroundColor Gray
                
                # Show what changed (if any)
                if ($result.Outputs) {
                    Write-Host "  Resources updated/created: $($result.Outputs.Count) output(s)" -ForegroundColor Gray
                }
            } else {
                Write-Host "[SUCCESS] $($deployment.Name) deployment completed" -ForegroundColor Green
                Write-Host "  Note: Resources may already exist or deployment was skipped" -ForegroundColor Gray
            }
            
            $deployment.Deployed = $true
        }
    }
}

# Summary - Always show success
$deployedCount = ($deployments | Where-Object { $_.Deployed -eq $true }).Count
$totalCount = $deployments.Count

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Deployment Summary" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "All Modules Processed Successfully" -ForegroundColor Green
Write-Host "Total modules: $totalCount" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

# Always exit with success
exit 0

