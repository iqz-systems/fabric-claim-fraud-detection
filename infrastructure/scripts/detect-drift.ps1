<#
.SYNOPSIS
    Detects drift between ARM templates and actual Azure resources

.DESCRIPTION
    Compares the desired state (ARM templates) with actual Azure resources
    to detect missing or changed resources that need to be redeployed
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$false)]
    [string]$ModulesPath = "infrastructure\arm-templates\modules"
)

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
    $context = Get-AzContext -ErrorAction Stop | Out-Null
} catch {
    Write-Error "Not logged in to Azure. Please run: Connect-AzAccount"
    exit 1
}

Write-Host "Detecting drift in resource group: $ResourceGroupName" -ForegroundColor Cyan

# Define expected resources from modules
$expectedResources = @{
    'monitoring' = @(
        @{ Type = "Microsoft.OperationalInsights/workspaces"; NamePattern = "*loganalytics*" }
        @{ Type = "Microsoft.Insights/components"; NamePattern = "*appinsights*" }
        @{ Type = "Microsoft.Insights/actionGroups"; NamePattern = "*actiongroup*" }
    )
    'storage' = @(
        @{ Type = "Microsoft.Storage/storageAccounts"; NamePattern = "*storage*" }
    )
    'event-hub' = @(
        @{ Type = "Microsoft.EventHub/namespaces"; NamePattern = "*eventhub*" }
    )
    'ai-services' = @(
        @{ Type = "Microsoft.CognitiveServices/accounts"; NamePattern = "*cognitiveservices*" }
    )
    'integration' = @(
        @{ Type = "Microsoft.Logic/workflows"; NamePattern = "*logicapp*" }
        @{ Type = "Microsoft.Web/connections"; NamePattern = "*connection*" }
    )
    'fabric-capacity' = @(
        @{ Type = "Microsoft.Fabric/capacities"; NamePattern = "*fabric*" }
    )
}

$driftDetected = $false
$modulesToRedeploy = @()

# Check each module
foreach ($moduleName in $expectedResources.Keys) {
    $moduleDrift = $false
    $expected = $expectedResources[$moduleName]
    
    Write-Host "`nChecking module: $moduleName" -ForegroundColor Yellow
    
    foreach ($resourceSpec in $expected) {
        try {
            $resources = Get-AzResource -ResourceGroupName $ResourceGroupName -ResourceType $resourceSpec.Type -ErrorAction SilentlyContinue
            
            if ($null -eq $resources -or $resources.Count -eq 0) {
                Write-Host "  [DRIFT] Missing resource type: $($resourceSpec.Type)" -ForegroundColor Red
                $moduleDrift = $true
                $driftDetected = $true
            } else {
                Write-Host "  [OK] Found $($resources.Count) resource(s) of type: $($resourceSpec.Type)" -ForegroundColor Green
            }
        } catch {
            Write-Host "  [WARNING] Could not check resource type: $($resourceSpec.Type) - $_" -ForegroundColor Yellow
        }
    }
    
    if ($moduleDrift) {
        $modulesToRedeploy += $moduleName
        Write-Host "  Module '$moduleName' has drift - will be redeployed" -ForegroundColor Red
    }
}

# Output results
if ($driftDetected) {
    Write-Host "`n========================================" -ForegroundColor Red
    Write-Host "Drift Detection Summary" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "Drift detected in modules: $($modulesToRedeploy -join ', ')" -ForegroundColor Yellow
    Write-Host "These modules will be redeployed" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Red
    
    # Set output variables for each module with drift
    foreach ($moduleName in $modulesToRedeploy) {
        Write-Host "##vso[task.setvariable variable=changedModule_$moduleName;isOutput=true]true"
    }
    
    Write-Host "##vso[task.setvariable variable=changedModules;isOutput=true]$($modulesToRedeploy -join ',')"
    Write-Host "##vso[task.setvariable variable=changedModulesCount;isOutput=true]$($modulesToRedeploy.Count)"
    Write-Host "##vso[task.setvariable variable=driftDetected;isOutput=true]true"
} else {
    Write-Host "`n========================================" -ForegroundColor Green
    Write-Host "Drift Detection Summary" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "No drift detected - all expected resources exist" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    
    Write-Host "##vso[task.setvariable variable=driftDetected;isOutput=true]false"
}

exit 0

