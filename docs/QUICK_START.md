# Quick Start Guide

Get up and running with the Fraud Detection Demo in 5 steps.

## Step 1: Create Fabric Workspace and Lakehouses

1. Go to [Microsoft Fabric Portal](https://app.fabric.microsoft.com)
2. Create a new workspace (or use existing)
3. Create three lakehouses:
   - `raw` (for source data)
   - `curate` (for transformed data)
   - `publish` (for analytics-ready data)

## Step 2: Import Notebooks

### Quick Method:
1. In Fabric workspace, click **+ New** → **Notebook**
2. Name it: `frauddetectiondataprep`
3. Open the file: `Batch Processing/frauddetectiondataprep.Notebook/notebook-content.py`
4. Copy **ALL** content (Ctrl+A, Ctrl+C)
5. In Fabric notebook, click **...** → **View code**
6. Paste content (Ctrl+V)
7. Click **Save**
8. Attach lakehouses: Click **+ Add** → **Lakehouse** → Select your lakehouses

Repeat for each notebook.

## Step 3: Configure Credentials

In each notebook, find the **Configuration** cell and update:

```python
# Replace these:
STORAGE_ACCOUNT_NAME = "<your-storage-account-name>"
STORAGE_ACCOUNT_KEY = "<your-storage-account-key>"
EVENT_HUB_CONNECTION_STRING = "<your-connection-string>"
```

Or use environment variables (recommended).

## Step 4: Create Schemas

Run this in any notebook to create required schemas:

```sql
CREATE SCHEMA IF NOT EXISTS raw.frauddetection;
CREATE SCHEMA IF NOT EXISTS curate.frauddetection;
CREATE SCHEMA IF NOT EXISTS publish.frauddetection;
CREATE SCHEMA IF NOT EXISTS raw.dataquality;
```

## Step 5: Run Notebooks in Order

1. **First**: `frauddetectiondataprep` - Loads initial data
2. **Then**: `fact_claims_incremental_load` - Processes fact tables
3. **Then**: `Feature Engineering` - Creates ML features
4. **Then**: `Fraud Detection Model` - Trains model

## Understanding Metadata

The `# META` sections are **automatically managed by Fabric**. When you:
- **Attach a lakehouse** → Fabric updates metadata
- **Change kernel** → Fabric updates metadata
- **Save notebook** → Fabric saves metadata

You typically **don't need to edit** `# META` sections manually. Just:
1. Attach lakehouses using the UI
2. Fabric handles the rest

## Troubleshooting

**"Lakehouse not attached"**
→ Click **+ Add** → **Lakehouse** in notebook

**"Table not found"**
→ Run `frauddetectiondataprep` notebook first

**"Module not found"**
→ Install in notebook: `%pip install <package-name>`

## Need Help?

- See [HOW_TO_USE_NOTEBOOKS.md](./HOW_TO_USE_NOTEBOOKS.md) for detailed instructions
- See [SETUP.md](./SETUP.md) for complete setup guide
- See [ARCHITECTURE.md](./ARCHITECTURE.md) for system overview

