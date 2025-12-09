# Azure DevOps Pipeline Setup Guide

Complete step-by-step guide to configure and run the infrastructure deployment pipelines.

## Prerequisites Checklist

Before starting, ensure you have:
- Access to Azure DevOps organization (<your-org>)
- Access to Azure subscription
- Permissions to create service connections
- Permissions to create pipelines
- Permissions to create variable groups

## Step 1: Create Service Connection

The pipeline needs to authenticate to Azure to deploy resources.

### Steps:

1. **Navigate to Service Connections**
   - Go to: `<your-devops-org-url>`
   - Click **Project Settings** (gear icon in bottom left)
   - Under **Pipelines**, click **Service connections**

2. **Create New Service Connection**
   - Click **New service connection** (or **Create service connection**)
   - Select **Azure Resource Manager**
   - Click **Next**

3. **Configure Authentication**
   - **Authentication method**: Choose one:
     - **Workload Identity federation (automatic)** - Recommended (if available)
     - **Service principal (automatic)** - Alternative option
     - **Managed Identity** - If using Azure-hosted agents
   - Click **Next**

4. **Select Scope**
   - **Scope level**: Choose **Subscription**
- **Subscription**: Select your subscription (e.g., "<your-subscription-name>")
   - **Resource group**: Leave empty or select specific resource group
- **Service connection name**: `Azure-Service-Connection` (or your chosen name; ensure YAML matches)
   - **Security**: 
     - Grant access permission to all pipelines
     - Allow all pipelines to use this connection
   - Click **Save**

5. **Verify Connection**
   - You should see `Azure-Service-Connection` in the list
   - Status should show as **Ready** or **Verified**

---

## Step 2: Create Variable Group

Store shared variables that pipelines can use.

### Steps:

1. **Navigate to Variable Groups**
   - Go to: `<your-devops-org-url>`
   - Click **Pipelines** → **Library**
   - Click **+ Variable group**

2. **Create Variable Group**
   - **Variable group name**: `Infrastructure-Variables` (must match exactly)
   - **Description**: "Shared variables for infrastructure deployment"

3. **Add Variables**
   Click **+ Add** for each variable:

   | Variable Name | Value | Secret? |
   |--------------|-------|---------|
   | `subscriptionId` | `<your-subscription-id>` | No |
   | `location` | `<your-location>` | No |

   **To add a variable:**
   - Click **+ Add**
   - Enter variable name
   - Enter variable value
   - Check "Keep this value secret" if it's sensitive
   - Click **OK**

4. **Save Variable Group**
   - Click **Save** at the top
   - Verify the variable group appears in the list

---

## Step 3: Create Environments

Environments allow you to set approval gates and track deployments.

### Create Development Environment:

1. **Navigate to Environments**
   - Go to: `<your-devops-org-url>`
   - Click **Pipelines** → **Environments**
   - Click **Create environment**

2. **Configure Dev Environment**
   - **Name**: `dev` (must match exactly)
   - **Description**: "Development environment"
   - **Resource type**: Select **None** (or **Virtual machines** if using agents)
   - Click **Create**

3. **Configure Dev Environment Settings** (Optional)
   - Approval gates: **None** (dev deploys automatically)
   - Protection rules: Leave default
   - Click **Save**

### Create Production Environment:

1. **Create Production Environment**
   - Click **Create environment** again
   - **Name**: `prod` (must match exactly)
   - **Description**: "Production environment"
   - **Resource type**: Select **None**
   - Click **Create**

2. **Configure Production Approval Gates**
   - Click on the `prod` environment
   - Click **Approvals and checks** (or **Approvals**)
   - Click **+** → **Approvals**
   - **Approvers**: Add yourself or team members who should approve production deployments
   - **Minimum number of approvers**: 1
   - **Timeout**: 30 days (or your preference)
   - Click **Create**
   - Click **Save**

---

## Step 4: Create and Configure Pipeline

### Option A: Create Pipeline from YAML File (Recommended)

1. **Navigate to Pipelines**
   - Go to: `https://dev.azure.com/iqz/data-platform`
   - Click **Pipelines** → **Pipelines**
   - Click **Create Pipeline** (or **New pipeline**)

2. **Select Repository**
- **Where is your code?**: Select **Azure Repos Git** (or your Git provider)
- **Select a repository**: Choose your repo (e.g., `<your-repo-name>`)
   - Click **Continue**

3. **Configure Pipeline**
   - **Select a template**: Choose **Existing Azure Pipelines YAML file**
   - **Branch**: `master` (or your default branch)
- **Path**: `/infrastructure/azure-pipelines-incremental.yml`
   - Click **Continue**

4. **Review Pipeline**
   - Review the pipeline YAML
   - Verify it references:
     - Service connection: `Azure-Service-Connection`
     - Variable group: `Infrastructure-Variables`
     - Environments: `dev` and `prod`
   - Click **Run** to save and run the pipeline

### Option B: Create Pipeline Manually

1. **Create New Pipeline**
   - Go to **Pipelines** → **Pipelines**
   - Click **Create Pipeline**
   - Select repository: `claim-fraud-detection-infra`
   - Choose **Starter pipeline** or **Empty job**

2. **Replace YAML Content**
   - Delete the default YAML
   - Copy content from `azure-pipelines/azure-pipelines-incremental.yml`
   - Paste into the editor
   - Click **Save**

3. **Save Pipeline**
   - **Pipeline name**: `Infrastructure Deployment` (or your preferred name)
   - Click **Save**

---

## Step 5: Verify Pipeline Configuration

### Check Pipeline Settings:

1. **Open Pipeline**
   - Go to **Pipelines** → **Pipelines**
   - Click on your pipeline

2. **Verify Settings**
   - Click **Edit** (or three dots → **Settings**)
   - Check:
     - Service connection is selected
     - Variable group is referenced
     - Environments are configured
     - Branch filters are set correctly

### Test Pipeline:

1. **Run Pipeline Manually**
   - Click **Run pipeline**
   - Select branch: `master` or `develop`
   - Click **Run**

2. **Monitor Execution**
   - Watch the pipeline run
   - Check each stage:
     - Validate stage should pass
     - DetectChanges stage should run
     - DeployDev stage should deploy (if on develop/main branch)
     - DeployProd stage should wait for approval (if on main branch)

---

## Step 6: Configure Branch Policies (Optional but Recommended)

Set up branch policies to ensure code quality.

### For `main` Branch:

1. **Navigate to Repos**
   - Go to **Repos** → **Branches**
   - Find `main` branch
   - Click **...** → **Branch policies**

2. **Configure Policies**
   - **Require a minimum number of reviewers**: 1
   - **Check for linked work items**: Optional
   - **Check for comment resolution**: Optional
   - **Build validation**: 
     - Require pipeline to pass
     - Select your pipeline
     - **Display name**: "Infrastructure Validation"
   - Click **Save**

### For `develop` Branch:

1. **Set Branch Policies**
   - Similar to main, but less strict
   - Optional: Require reviewers
   - Optional: Build validation

---

## Step 7: Test the Pipeline

### Test Development Deployment:

1. **Create Test Branch**
   ```powershell
   git checkout -b test-pipeline
   ```

2. **Make a Small Change**
   - Edit a file (e.g., README.md)
   - Commit and push:
   ```powershell
   git add .
   git commit -m "Test pipeline"
   git push origin test-pipeline
   ```

3. **Create Pull Request**
   - Go to Azure DevOps
   - Create PR from `test-pipeline` to `develop`
   - Pipeline should trigger automatically
   - Verify it runs successfully

### Test Production Deployment:

1. **Merge to Main**
   - Merge PR to `main` branch
   - Pipeline should trigger

2. **Approve Production**
   - When pipeline reaches `DeployProd` stage
   - You'll see approval request
   - Click **Review** → **Approve**
   - Pipeline continues

---

## Troubleshooting

### Pipeline Fails: "Service connection not found"

**Solution:**
- Verify service connection name is exactly: `Azure-Service-Connection`
- Check service connection exists in Project Settings → Service connections
- Verify it's enabled for all pipelines

### Pipeline Fails: "Variable group not found"

**Solution:**
- Verify variable group name is exactly: `Infrastructure-Variables`
- Check variable group exists in Pipelines → Library
- Verify variables are set correctly

### Pipeline Fails: "Environment not found"

**Solution:**
- Verify environment names are exactly: `dev` and `prod`
- Check environments exist in Pipelines → Environments
- Verify you have permissions to use the environments

### Pipeline Doesn't Trigger

**Solution:**
- Check branch filters in pipeline YAML
- Verify files changed are in `infrastructure/` directory
- Check pipeline is enabled
- Verify branch exists in repository

### Deployment Fails: "Not authorized"

**Solution:**
- Check service connection permissions
- Verify service principal has Contributor role on subscription/resource group
- Check Azure subscription is active

---

## Quick Reference

### Service Connection
- **Name**: `Azure-Service-Connection`
- **Type**: Azure Resource Manager
- **Location**: Project Settings → Service connections

### Variable Group
- **Name**: `Infrastructure-Variables`
- **Variables**: `subscriptionId`, `location`
- **Location**: Pipelines → Library

### Environments
- **Dev**: `dev` (no approval)
- **Prod**: `prod` (with approval)
- **Location**: Pipelines → Environments

### Pipeline File
- **Path**: `azure-pipelines/azure-pipelines-incremental.yml`
- **Alternative**: `azure-pipelines/azure-pipelines.yml`

---

## Next Steps

After setup:
1. Run a test deployment to dev
2. Verify resources are created correctly
3. Test change detection (modify one module)
4. Test production approval workflow
5. Document any customizations for your team

## Support

If you encounter issues:
1. Check pipeline logs in Azure DevOps
2. Review Azure activity logs
3. Verify all prerequisites are met
4. Check service connection and permissions

---

**Last Updated**: Based on current pipeline configuration
**Pipeline Version**: Incremental deployment with change detection

