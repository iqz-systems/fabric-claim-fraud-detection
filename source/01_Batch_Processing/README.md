# Batch Processing Notebooks

This folder contains notebooks for batch ETL processing and data pipeline orchestration.

## Notebooks

| File | Purpose | Execution Order |
|------|---------|----------------|
| `01_Data_Preparation_Load_Initial_Data.py` | Load source data files into Raw layer | **Run first** |
| `02_Merge_Speed_To_Raw_Claims.py` | Merge streaming claims into batch layer | After Speed Layer |
| `03_Batch_Processing_Incremental_Fact_Load.py` | Process fact tables incrementally | After 01 or 02 |
| `04_Semantic_Model_Prep.py` | Prepare tables for Power BI semantic model | After 03 |

## Quick Start

1. **Start with:** `01_Data_Preparation_Load_Initial_Data.py`
   - Loads all source data
   - Creates base tables

2. **Then run:** `03_Batch_Processing_Incremental_Fact_Load.py`
   - Creates fact tables
   - Processes incrementally

3. **Finally:** `04_Semantic_Model_Prep.py`
   - Prepares for Power BI
   - Creates analytics-ready tables

## How to Use

Each notebook is **copy-paste ready**:

1. Open the `.py` file
2. Copy ALL content (Ctrl+A, Ctrl+C)
3. In Fabric: Create notebook → View code → Paste
4. Attach lakehouses
5. Update configuration sections
6. Run!

## Execution Flow

```
01_Data_Preparation
    ↓
02_Speed_To_Raw_Claims (if using Speed Layer)
    ↓
03_Batch_Processing_Incremental_Fact_Load
    ↓
04_Semantic_Model_Prep
```

## Output Tables

### Raw Layer
- `raw.frauddetection.claims`
- `raw.frauddetection.customers`
- `raw.frauddetection.policies`
- `raw.frauddetection.products`

### Curate Layer
- `curate.frauddetection.fact_claims`
- `curate.frauddetection.pipeline_watermark`

### Publish Layer
- `publish.frauddetection.fact_claims`
- `publish.frauddetection.fact_customer_claims`
- `publish.frauddetection.fact_customer_lifecycle`
- `publish.frauddetection.claim_settlements`
- `publish.frauddetection.fact_incident_loss_by_product`

## Configuration

Each notebook has a **Configuration** section. Update:
- File paths (if your data is in different locations)
- Application Insights connection string (optional)
- Table names (usually fine as-is)

## Notes

- **Incremental Processing:** Notebook 03 uses watermark-based incremental loading
- **Watermarks:** Tracked in `curate.frauddetection.pipeline_watermark`
- **Idempotent:** Can run multiple times safely

## Related Notebooks

- **Speed Layer:** See `../Speed Layer/` for real-time processing
- **Machine Learning:** See `../Machine Learning/` for feature engineering and models
- **Data Observability:** See `../Data Observability/` for monitoring

