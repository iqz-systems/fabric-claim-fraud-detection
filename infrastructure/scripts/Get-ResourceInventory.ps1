<#
.SYNOPSIS
    Generates an inventory report of all resources in the resource group

.DESCRIPTION
    Creates a detailed inventory report of all Azure resources in the specified resource group
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$false)]
    [string]$OutputPath = "infrastructure\docs\resource-inventory-$(Get-Date -Format 'yyyyMMdd').csv"
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
    $context = Get-AzContext -ErrorAction Stop
    Write-Host "Connected to Azure subscription: $($context.Subscription.Name)" -ForegroundColor Green
} catch {
    Write-Error "Not logged in to Azure. Please run: Connect-AzAccount"
    exit 1
}

Write-Host "Generating resource inventory for: $ResourceGroupName" -ForegroundColor Cyan

try {
    $resources = Get-AzResource -ResourceGroupName $ResourceGroupName
    
    if ($resources.Count -eq 0) {
        Write-Host "No resources found in resource group: $ResourceGroupName" -ForegroundColor Yellow
        exit 0
    }
    
    $inventory = $resources | Select-Object @{
        Name = "ResourceName"
        Expression = { $_.Name }
    }, @{
        Name = "ResourceType"
        Expression = { $_.ResourceType }
    }, @{
        Name = "Location"
        Expression = { $_.Location }
    }, @{
        Name = "ResourceId"
        Expression = { $_.ResourceId }
    }, @{
        Name = "Tags"
        Expression = { ($_.Tags | ConvertTo-Json -Compress) }
    }
    
    # Ensure output directory exists
    $outputDir = Split-Path $OutputPath -Parent
    if (-not (Test-Path $outputDir)) {
        New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
    }
    
    $inventory | Export-Csv -Path $OutputPath -NoTypeInformation
    
    Write-Host "`nInventory generated successfully!" -ForegroundColor Green
    Write-Host "Total resources: $($resources.Count)" -ForegroundColor Cyan
    Write-Host "Output file: $OutputPath" -ForegroundColor Cyan
    
    # Display summary by resource type
    Write-Host "`nResource Summary by Type:" -ForegroundColor Yellow
    $resources | Group-Object ResourceType | Select-Object Count, Name | Format-Table -AutoSize
    
} catch {
    Write-Error "Failed to generate inventory: $_"
    exit 1
}

