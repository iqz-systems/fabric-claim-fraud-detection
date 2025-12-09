<#
.SYNOPSIS
    Detects which infrastructure modules have changed

.DESCRIPTION
    Analyzes git changes to determine which ARM template modules need to be redeployed.
    Handles dependencies and ensures proper deployment order.
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$SourceBranch = ""
)

$ErrorActionPreference = "Stop"

# Define module paths
$modules = @{
    'monitoring' = @('infrastructure/arm-templates/modules/monitoring', 'infrastructure/config')
    'storage' = @('infrastructure/arm-templates/modules/storage')
    'event-hub' = @('infrastructure/arm-templates/modules/event-hub')
    'ai-services' = @('infrastructure/arm-templates/modules/ai-services', 'infrastructure/config')
    'integration' = @('infrastructure/arm-templates/modules/integration')
    'fabric-capacity' = @('infrastructure/arm-templates/modules/fabric-capacity', 'infrastructure/config')
}

# Determine source and target branches/commits
if ($SourceBranch -eq "") {
    $SourceBranch = "HEAD"
}

# Get the previous commit hash
try {
    $previousCommit = git rev-parse "$SourceBranch~1" 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $previousCommit) {
        # If we can't get previous commit, compare with origin/main or origin/develop
        $currentBranch = git rev-parse --abbrev-ref HEAD
        if ($currentBranch -eq "main" -or $currentBranch -match "refs/heads/main") {
            $previousCommit = "origin/main"
        } elseif ($currentBranch -eq "develop" -or $currentBranch -match "refs/heads/develop") {
            $previousCommit = "origin/develop"
        } else {
            $previousCommit = "HEAD~1"
        }
    }
} catch {
    $previousCommit = "HEAD~1"
}

Write-Host "Detecting changes between $previousCommit and $SourceBranch..." -ForegroundColor Cyan

# Get changed files
try {
    $changedFiles = git diff $previousCommit $SourceBranch --name-only
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Git diff failed, assuming all modules changed"
        $changedFiles = @('infrastructure/')
    }
} catch {
    Write-Warning "Git diff failed, assuming all modules changed"
    $changedFiles = @('infrastructure/')
}

if ($null -eq $changedFiles -or $changedFiles.Count -eq 0) {
    Write-Host "No changes detected - will deploy all modules" -ForegroundColor Yellow
    $changedModules = $modules.Keys
    foreach ($moduleName in $changedModules) {
        Write-Host "##vso[task.setvariable variable=changedModule_$moduleName;isOutput=true]true"
    }
    Write-Host "##vso[task.setvariable variable=changedModules;isOutput=true]$($changedModules -join ',')"
    Write-Host "##vso[task.setvariable variable=changedModulesCount;isOutput=true]$($changedModules.Count)"
    $deploymentOrder = $modules.Keys
    Write-Host "##vso[task.setvariable variable=deploymentOrder;isOutput=true]$($deploymentOrder -join ',')"
    
    # Display summary
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "Change Detection Summary" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "Changed Modules: $($changedModules -join ', ')" -ForegroundColor Green
    Write-Host "Deployment Order: $($deploymentOrder -join ' -> ')" -ForegroundColor Green
    Write-Host "Modules Count: $($changedModules.Count)" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    
    exit 0
}

Write-Host "Changed files:" -ForegroundColor Cyan
$changedFiles | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }

# Detect which modules changed
$changedModules = @()
$allModulesChanged = $false

# Check if infrastructure root changed (deploy all)
if ($changedFiles -match '^infrastructure/azuredeploy\.json' -or 
    $changedFiles -match '^infrastructure/scripts/Deploy-AllModules') {
    Write-Host "`nInfrastructure root changed - deploying all modules" -ForegroundColor Yellow
    $allModulesChanged = $true
}

foreach ($moduleName in $modules.Keys) {
    $modulePaths = $modules[$moduleName]
    $moduleChanged = $false
    
    foreach ($path in $modulePaths) {
        if ($changedFiles -match "^$path" -or $allModulesChanged) {
            $moduleChanged = $true
            break
        }
    }
    
    if ($moduleChanged) {
        $changedModules += $moduleName
        Write-Host "##vso[task.setvariable variable=changedModule_$moduleName;isOutput=true]true"
    }
}

# If no specific modules changed but infrastructure changed, deploy all
if ($changedModules.Count -eq 0 -and ($changedFiles -match '^infrastructure/')) {
    Write-Host "`nInfrastructure changes detected but no specific modules - deploying all" -ForegroundColor Yellow
    $changedModules = $modules.Keys
    foreach ($moduleName in $changedModules) {
        Write-Host "##vso[task.setvariable variable=changedModule_$moduleName;isOutput=true]true"
    }
}

Write-Host "`nChanged Modules: $($changedModules -join ', ')" -ForegroundColor Green
Write-Host "##vso[task.setvariable variable=changedModules;isOutput=true]$($changedModules -join ',')"
Write-Host "##vso[task.setvariable variable=changedModulesCount;isOutput=true]$($changedModules.Count)"

# Determine deployment order based on dependencies
$dependencies = @{
    'monitoring' = @()
    'storage' = @('monitoring')
    'event-hub' = @('monitoring')
    'ai-services' = @('monitoring')
    'integration' = @('monitoring')
    'fabric-capacity' = @()
}

$deploymentOrder = @()
$processed = @{}

function Resolve-Dependencies {
    param($module)
    
    if ($processed[$module]) { return }
    if (-not ($changedModules -contains $module)) { return }
    
    $deps = $dependencies[$module]
    foreach ($dep in $deps) {
        if ($changedModules -contains $dep) {
            Resolve-Dependencies $dep
        }
    }
    
    if (-not ($deploymentOrder -contains $module)) {
        $deploymentOrder += $module
    }
    $processed[$module] = $true
}

foreach ($module in $changedModules) {
    Resolve-Dependencies $module
}

Write-Host "`nDeployment Order: $($deploymentOrder -join ' -> ')" -ForegroundColor Cyan
Write-Host "##vso[task.setvariable variable=deploymentOrder;isOutput=true]$($deploymentOrder -join ',')"

# Display summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Change Detection Summary" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Changed Modules: $($changedModules -join ', ')" -ForegroundColor Green
Write-Host "Deployment Order: $($deploymentOrder -join ' -> ')" -ForegroundColor Green
Write-Host "Modules Count: $($changedModules.Count)" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan


