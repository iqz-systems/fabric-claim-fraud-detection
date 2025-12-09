# Configuration Files

This directory contains external configuration files for custom details that should be kept separate from ARM templates.

## Purpose

Configuration files allow you to:
- Keep sensitive data (like email addresses) out of templates
- Easily update frequently changing data without modifying templates
- Maintain environment-specific configurations
- Version control configuration separately from infrastructure code

## Fabric Capacity Configuration

### Files
- `fabric-capacity-config.json` - Base/default configuration
- `fabric-capacity-config.dev.json` - Development environment configuration
- `fabric-capacity-config.prod.json` - Production environment configuration

### Structure
```json
{
  "fabricCapacity": {
    "name": "iqzfabric",
    "location": "eastus2",
    "sku": {
      "name": "F2",
      "tier": "Fabric"
    },
    "administration": {
      "members": [
        "email1@domain.com",
        "email2@domain.com"
      ]
    },
    "tags": {
      "ProjectType": "claim-fraud-detection",
      "Environment": "Dev"
    }
  }
}
```

### Usage

Deploy using the script that reads from config files:
```powershell
.\infrastructure\scripts\Deploy-FabricCapacity.ps1 `
    -ResourceGroupName "iqzdatateam-2" `
    -Environment "dev"
```

Or use a custom config file:
```powershell
.\infrastructure\scripts\Deploy-FabricCapacity.ps1 `
    -ResourceGroupName "iqzdatateam-2" `
    -ConfigFile "infrastructure\config\custom-config.json"
```

## Best Practices

1. **Never commit sensitive data** - Use environment variables or Azure Key Vault for secrets
2. **Use environment-specific files** - Keep dev/prod configurations separate
3. **Document configuration structure** - Update this README when adding new config files
4. **Version control** - Track changes to configuration files in source control
5. **Validate before deploy** - Review configuration values before deployment

## Adding New Configuration Files

1. Create a new JSON file following the existing structure
2. Update this README with the new file's purpose
3. Create a deployment script that reads from the config file (if needed)
4. Document the configuration schema

