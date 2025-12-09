# FinOps Notebooks

This folder contains notebooks and semantic models for Azure cost management and financial operations (FinOps).

## Contents

| Item | Purpose |
|------|---------|
| `12_FinOps_Data_Load.py` | Load Azure cost and usage data | Copy-paste ready |
| `finops_semantic_model.SemanticModel/` | Power BI semantic model | Data model |
| `finops.Report/` | Power BI report | Reference |
| `finops.Lakehouse/` | Lakehouse metadata | Reference |

## Quick Start

1. **Load Data:**
   - Run `12_FinOps_Data_Load.py`
   - Upload sample CSV files to lakehouse Files folder:
     - `AzureUsage/*.csv` - Azure usage and cost data
     - `budgets.csv` - Budget definitions
     - `optimzation_log.csv` - Resource optimization events

2. **Create Semantic Model:**
   - Use `finops_semantic_model.SemanticModel` as reference
   - Connect to FinOps lakehouse tables

3. **Build Reports:**
   - Use Power BI report as reference
   - Create custom FinOps dashboards

## How to Use

### Data Load Notebook

1. Copy ALL content from `12_FinOps_Data_Load.py`
2. Paste into Fabric notebook
3. Attach FinOps lakehouse
4. Update file paths if needed
5. Run all cells

## Data Model

### Tables

- **azure_usage_cost** - Azure usage and cost data
  - Normalized resource group names
  - Date-formatted timestamps
  - Cost and usage metrics

- **budgets** - Budget definitions and tracking
  - Start and end dates
  - Budget amounts
  - Budget categories

- **resource_optimization_events** - Resource optimization logs
  - Optimization actions
  - Cost savings
  - Event timestamps

### Semantic Model

The semantic model (`finops_semantic_model.SemanticModel`) includes:
- Relationships between tables
- Measures for cost analysis
- Calculated columns for KPIs

## Configuration

Update these in the notebook:
- File paths (if your data is in different locations)
- Lakehouse ID (when attaching lakehouse)
- Date formats (if your CSV dates differ)

## Related

- **Main Project:** See `../README.md` for overall architecture
- **Data Observability:** See `../Data Observability/` for monitoring

## Notes

- FinOps is **separate** from fraud detection - can run independently
- Semantic model can be imported into Power BI
- Report can be used as a template for custom dashboards

