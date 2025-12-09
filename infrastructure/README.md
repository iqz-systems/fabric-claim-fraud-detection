# Azure DevOps Pipeline

Incremental CI/CD pipeline for infrastructure deployment with change detection, dependency handling, and environment management.

## Features

- **Change Detection** - Only deploys modules that have changed  
- **Dependency Resolution** - Automatically handles module dependencies  
- **Multi-Environment** - Dev and Prod deployments with approval gates  
- **Template Validation** - Validates ARM templates before deployment  
- **Incremental Deployment** - Deploys in correct order based on dependencies  
- **Configuration Management** - Uses external config files  

## Pipeline Structure

```
infrastructure/
├── azure-pipelines.yml                  # Main pipeline definition
├── azure-pipelines-incremental.yml      # Incremental pipeline with change detection
├── pipeline-templates/                  # Reusable pipeline templates
│   ├── deploy-module.yml                # Template for deploying individual modules
│   └── smart-deploy.yml                 # Incremental deployment with change detection
├── variables/                           # Environment-specific variables (placeholders)
│   ├── dev.yml                          # Development variables
│   └── prod.yml                         # Production variables
├── scripts/                             # Pipeline utility scripts
│   ├── detect-changes.ps1               # Change detection script
│   └── detect-drift.ps1                 # Drift detection script
├── README.md                            # This file
└── SETUP-GUIDE.md                       # How to configure the pipeline
```

## Pipeline Stages

### 1. Validate
- Validates all ARM templates for syntax errors
- Runs before any deployment

### 2. DetectChanges
- Analyzes git changes to determine which modules changed
- Resolves dependencies
- Sets pipeline variables for downstream stages

### 3. DeployDev
- Deploys to development environment
- Triggers on `develop` or `main` branch
- No approval required

### 4. DeployProd
- Deploys to production environment
- Only triggers on `main` branch
- Requires approval (configured in Azure DevOps)

## Setup Instructions

### 1. Create Service Connection

In Azure DevOps:
1. Go to **Project Settings** > **Service connections**
2. Create new **Azure Resource Manager** connection
3. Name it: `Azure-Service-Connection`
4. Select your subscription and resource group

### 2. Create Variable Group

Create a variable group named `Infrastructure-Variables`:
- `subscriptionId`: Your Azure subscription ID
- `location`: Default location (eastus)
- Any other shared variables

### 3. Create Environments

Create environments in Azure DevOps:
- **dev** - Development environment (no approval)
- **prod** - Production environment (with approval gates)

### 4. Configure Branch Policies

Set up branch policies:
- **main** branch: Requires PR, triggers production deployment
- **develop** branch: Triggers development deployment

## Usage

### Automatic Deployment

The pipeline automatically triggers on:
- Push to `main` or `develop` branches
- Changes to files in `infrastructure/` directory
- Pull requests to `main` or `develop`

### Manual Trigger

1. Go to **Pipelines** > **Your Pipeline**
2. Click **Run pipeline**
3. Select branch and parameters
4. Click **Run**

### Change Detection

The pipeline automatically detects:
- Which modules have changed
- Dependencies between modules
- Deployment order

Example: If only `storage` module changes, only storage will be redeployed (after monitoring if needed).

## Customization

### Adding New Modules

1. Add module to `detect-changes.ps1` dependencies
2. Update `Deploy-AllModules.ps1` if needed
3. Add module-specific variables to environment files

### Modifying Deployment Logic

Edit `azure-pipelines.yml` to:
- Add new stages
- Modify conditions
- Add approval gates
- Customize deployment steps

### Environment-Specific Settings

Update variables in:
- `variables/dev.yml` for development
- `variables/prod.yml` for production

## Best Practices

1. **Always validate** - Templates are validated before deployment
2. **Review changes** - Check what will be deployed in PR
3. **Test in dev first** - Dev deploys automatically, prod requires approval
4. **Use config files** - Keep custom details in config files, not templates
5. **Monitor deployments** - Check pipeline logs and Azure activity logs

## Troubleshooting

### Pipeline Fails on Validation
- Check template syntax errors
- Verify JSON formatting
- Review validation output

### Deployment Fails
- Check Azure service connection permissions
- Verify resource group exists or can be created
- Review deployment logs in Azure Portal

### Changes Not Detected
- Ensure files are committed to git
- Check branch triggers are configured
- Verify path filters in pipeline

## Security

- Service connections use managed identities
- Secrets stored in Azure DevOps variable groups
- Production deployments require approval
- All deployments are logged and auditable


