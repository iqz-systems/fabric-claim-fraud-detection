# How to Open and Run Notebooks in Microsoft Fabric

This guide explains how to import and run the notebooks in Microsoft Fabric.

## Understanding Notebook Metadata (`# META`)

The `# META` sections in each notebook define:
- **Lakehouse attachments** - Which lakehouses the notebook can access
- **Kernel type** - Spark/Python runtime
- **Environment** - Python packages and dependencies

These are **automatically read by Fabric** when you open the notebook.

## Method 1: Import Notebooks into Fabric (Recommended)

### Step 1: Prepare Your Fabric Workspace

1. **Create/Open your Fabric Workspace**
   - Go to [Microsoft Fabric Portal](https://app.fabric.microsoft.com)
   - Create a new workspace or open an existing one
   - Note your **Workspace ID** (from the URL: `https://app.fabric.microsoft.com/workspaces/<workspace-id>`)

2. **Create Lakehouses**
   - Create three lakehouses: `raw`, `curate`, and `publish`
   - Note each **Lakehouse ID** (from the lakehouse URL or properties)

### Step 2: Import Notebooks

#### Option A: Copy-Paste Method (Easiest)

1. **Create a new notebook in Fabric**
   - Click **+ New** → **Notebook**
   - Name it (e.g., "fact_claims_incremental_load")

2. **Copy notebook content**
   - Open the `notebook-content.py` file from the sanitized codebase
   - Copy **ALL** content (including `# META` sections)

3. **Paste into Fabric notebook**
   - In Fabric notebook, click the **...** menu → **View code**
   - Paste the entire content
   - Click **Save**

4. **Attach Lakehouses**
   - Click the **+ Add** button in the notebook
   - Select **Lakehouse**
   - Attach your `raw`, `curate`, and `publish` lakehouses
   - Fabric will automatically update the `# META` sections with real IDs

#### Option B: Direct File Import (If Supported)

1. **Zip the notebook folder**
   - Create a zip of the `.Notebook` folder (e.g., `fact_claims_incremental_load.Notebook.zip`)

2. **Import in Fabric**
   - Go to your workspace
   - Click **Upload** → **Browse**
   - Select the zip file
   - Fabric will extract and create the notebook

### Step 3: Update Metadata (After Import)

After importing, Fabric may show placeholders. Update them:

1. **Open the notebook**
2. **Attach lakehouses** (this auto-updates metadata):
   - Click **+ Add** → **Lakehouse**
   - Select your lakehouses
3. **Or manually edit metadata**:
   - Click **...** → **View code**
   - Find `# META` sections
   - Replace placeholders:
     ```python
     # META       "default_lakehouse": "<your-raw-lakehouse-id>",
     # Replace with:
     # META       "default_lakehouse": "0561d39e-e3f9-49bf-8a4e-9f2e70c5441a",
     ```

## Method 2: Create Notebooks from Scratch

If you prefer to create notebooks manually:

1. **Create new notebook in Fabric**
2. **Copy code cells** from `notebook-content.py` (skip the `# META` sections)
3. **Attach lakehouses** - Fabric will generate metadata automatically
4. **Configure environment** if needed

## Running Notebooks

### Before Running

1. **Configure Credentials**
   - Set environment variables (see `config/env.template.txt`)
   - Or update hardcoded values in notebook configuration cells

2. **Create Required Tables**
   - Run `frauddetectiondataprep` notebook first to create base tables
   - Or manually create schemas:
     ```sql
     CREATE SCHEMA IF NOT EXISTS raw.frauddetection;
     CREATE SCHEMA IF NOT EXISTS curate.frauddetection;
     CREATE SCHEMA IF NOT EXISTS publish.frauddetection;
     ```

3. **Load Sample Data**
   - Upload sample data files to lakehouse Files folder
   - Or use existing data in your lakehouse

### Running Individual Cells

1. **Select a cell**
2. **Click Run** (▶) or press `Shift+Enter`
3. **Wait for execution** - Spark session starts automatically

### Running Entire Notebook

1. **Click Run all** (top toolbar)
2. **Or run cells sequentially** using `Shift+Enter`

### Running in Pipeline

1. **Create a Data Pipeline**
2. **Add notebook activities**
3. **Reference notebook by ID** (get from notebook properties)

## Understanding Metadata Structure

```python
# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"  # Spark kernel
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<lakehouse-id>",  # Primary lakehouse
# META       "default_lakehouse_name": "raw",        # Display name
# META       "default_lakehouse_workspace_id": "<workspace-id>",
# META       "known_lakehouses": [                   # All attached lakehouses
# META         {"id": "<lakehouse-id-1>"},
# META         {"id": "<lakehouse-id-2>"}
# META       ]
# META     }
# META   }
# META }
```

## Common Issues and Solutions

### Issue: "Lakehouse not found"
**Solution:**
- Attach the lakehouse using the **+ Add** button
- Or update the lakehouse ID in metadata

### Issue: "Table does not exist"
**Solution:**
- Run `frauddetectiondataprep` notebook first
- Or create tables manually:
  ```sql
  CREATE TABLE raw.frauddetection.claims (...);
  ```

### Issue: "Module not found"
**Solution:**
- Install packages in environment:
  ```python
  %pip install azure-eventhub azure-storage-blob
  ```
- Or configure environment with required packages

### Issue: "Connection string invalid"
**Solution:**
- Update configuration cells with your actual credentials
- Use environment variables instead of hardcoding

## Execution Order

Run notebooks in this order:

1. **Setup** (one-time):
   - `frauddetectiondataprep` - Load initial data

2. **Speed Layer** (real-time):
   - `Stream_HW_Claims_AIFoundry` - Process PDF forms
   - `Stream_Claims` - Stream to Event Hub

3. **Batch Processing**:
   - `speed_to_raw_claims` - Merge streaming data
   - `fact_claims_incremental_load` - Process fact tables
   - `semantic_model_prep` - Prepare for Power BI

4. **Machine Learning**:
   - `Feature Engineering` - Create features
   - `Fraud Detection Model` - Train and predict

5. **Observability**:
   - `data_freshness` - Monitor data quality
   - `start_datapipeline` / `end_datapipeline` - Pipeline tracking

## Tips

1. **Use Notebook Parameters** - For reusable notebooks, use parameters instead of hardcoding values
2. **Check Spark Session** - Ensure Spark is running before executing Spark SQL
3. **Monitor Execution** - Use Application Insights or notebook metrics
4. **Save Frequently** - Fabric auto-saves, but manual save ensures metadata is updated
5. **Test Incrementally** - Run cells one at a time to debug issues

## Next Steps

1. Import notebooks into your Fabric workspace
2. Attach lakehouses
3. Configure credentials
4. Run setup notebook
5. Execute notebooks in order
6. Monitor execution in Application Insights

For detailed setup instructions, see [SETUP.md](./SETUP.md).

