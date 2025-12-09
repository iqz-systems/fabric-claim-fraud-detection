<#
.SYNOPSIS
    Validates all ARM templates in the infrastructure directory

.DESCRIPTION
    Recursively validates all ARM template files to ensure they are syntactically correct
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [string]$TemplatePath = "infrastructure\arm-templates"
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

Write-Host "Validating ARM templates in: $TemplatePath" -ForegroundColor Cyan
Write-Host ""

$templateFiles = Get-ChildItem -Path $TemplatePath -Filter "azuredeploy.json" -Recurse
$validationResults = @()

foreach ($template in $templateFiles) {
    Write-Host "Validating: $($template.FullName)" -ForegroundColor Yellow
    
    try {
        # Basic JSON validation
        $jsonContent = Get-Content $template.FullName -Raw | ConvertFrom-Json
        
        # Check required schema fields
        $requiredFields = @("`$schema", "contentVersion", "parameters", "resources")
        $missingFields = @()
        
        foreach ($field in $requiredFields) {
            if (-not $jsonContent.PSObject.Properties.Name -contains $field.Replace('`$', '$')) {
                $missingFields += $field
            }
        }
        
        if ($missingFields.Count -gt 0) {
            throw "Missing required fields: $($missingFields -join ', ')"
        }
        
        Write-Host "  [OK] JSON syntax valid" -ForegroundColor Green
        Write-Host "  [OK] Required fields present" -ForegroundColor Green
        
        $validationResults += [PSCustomObject]@{
            Template = $template.FullName
            Status = "Valid"
            Errors = @()
        }
    } catch {
        Write-Host "  [ERROR] Validation failed: $_" -ForegroundColor Red
        $validationResults += [PSCustomObject]@{
            Template = $template.FullName
            Status = "Invalid"
            Errors = @($_)
        }
    }
    
    Write-Host ""
}

# Summary
Write-Host "Validation Summary:" -ForegroundColor Cyan
$validCount = ($validationResults | Where-Object { $_.Status -eq "Valid" }).Count
$invalidCount = ($validationResults | Where-Object { $_.Status -eq "Invalid" }).Count

Write-Host "Valid templates: $validCount" -ForegroundColor Green
Write-Host "Invalid templates: $invalidCount" -ForegroundColor $(if ($invalidCount -gt 0) { "Red" } else { "Green" })

if ($invalidCount -gt 0) {
    Write-Host "`nFailed templates:" -ForegroundColor Red
    $validationResults | Where-Object { $_.Status -eq "Invalid" } | ForEach-Object {
        Write-Host "  - $($_.Template)" -ForegroundColor Red
        $_.Errors | ForEach-Object {
            Write-Host "    Error: $_" -ForegroundColor Red
        }
    }
    exit 1
}

Write-Host "`nAll templates validated successfully" -ForegroundColor Green

