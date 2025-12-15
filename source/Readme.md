# Fraud Detection & FinOps Accelerator (Fabric)

This is a copy-paste friendly template to spin up an end-to-end Fabric solution for fraud detection (batch + speed + ML) with optional FinOps cost monitoring and Data Observability. All notebooks are plain `.py` files with placeholders for IDs/keys and can be pasted directly into Fabric.

## What’s Inside
- `01_Batch_Processing/` — ETL for raw → curated → publish.
- `02_Speed_Layer/` — streaming to Event Hub + AI PDF processing.
- `03_Machine_Learning/` — feature engineering + model training.
- `04_Data_Observability/` — freshness tracking, pipeline start/end logging, semantic model, Power BI report, KQL assets.
- `05_FinOps/` — cost/usage load notebook, semantic model, Power BI report.
- `config/` — placeholders and env template.
- `docs/` — setup, architecture, quick start, notebook tips.

## Notebook Execution Order (01–12)
1. `01_Batch_Processing/01_Data_Preparation_Load_Initial_Data.py`
2. `01_Batch_Processing/02_Merge_Speed_To_Raw_Claims.py`
3. `01_Batch_Processing/03_Batch_Processing_Incremental_Fact_Load.py`
4. `01_Batch_Processing/04_Semantic_Model_Prep.py`
5. `02_Speed_Layer/05_Speed_Layer_Stream_Claims_To_EventHub.py`
6. `02_Speed_Layer/06_Speed_Layer_Process_PDF_Claims_With_AI.py`
7. `03_Machine_Learning/07_Feature_Engineering_Create_ML_Features.py`
8. `03_Machine_Learning/08_Fraud_Detection_Model_Training.py`
9. `04_Data_Observability/09_Data_Freshness_Monitoring.py`
10. `04_Data_Observability/10_Pipeline_Start_Tracking.py`
11. `04_Data_Observability/11_Pipeline_End_Tracking.py`

## Notebook Details (What each does)
### Batch Processing
- `01_Data_Preparation_Load_Initial_Data.py` — Load claims and reference CSV/JSON into `raw.frauddetection.*`; flatten claims and fix data quality.
- `02_Merge_Speed_To_Raw_Claims.py` — Merge streaming/event data from speed layer into raw tables.
- `03_Batch_Processing_Incremental_Fact_Load.py` — Build/refresh fact tables incrementally using watermarks.
- `04_Semantic_Model_Prep.py` — Prepare publish-layer tables for the semantic model/Power BI.

### Speed Layer
- `05_Speed_Layer_Stream_Claims_To_EventHub.py` — Stream claims to Azure Event Hub.
- `06_Speed_Layer_Process_PDF_Claims_With_AI.py` — Extract handwritten data from PDFs (Azure AI), push to Event Hub.

### Machine Learning
- `07_Feature_Engineering_Create_ML_Features.py` — Create 50+ engineered features into a feature store table.
- `08_Fraud_Detection_Model_Training.py` — Train Random Forest; log with MLflow; write predictions to publish layer.

### Data Observability
- `09_Data_Freshness_Monitoring.py` — Check freshness/volume/schema across layers; logs to tables/App Insights.
- `10_Pipeline_Start_Tracking.py` — Log pipeline start, environment, and context.
- `11_Pipeline_End_Tracking.py` — Log pipeline completion status/metrics.

### FinOps (optional)
- `12_FinOps_Data_Load.py` — Load Azure usage, budgets, optimization events into FinOps lakehouse; tables for FinOps semantic model.

## Quick Start
1) Create Fabric workspace and lakehouses (`raw`, `curate`, `publish`; plus optional FinOps lakehouse).  
2) In Fabric, create a new notebook → **View code** → paste a `.py` file → attach lakehouse(s) → run.  
3) Follow the order above for core fraud flow; run FinOps separately if needed.  
4) Open reports/semantic models in Power BI as references for dashboards.


## Notes
- All IDs/connection strings are sanitized placeholders—replace with your own.  
- Notebooks are idempotent where possible; incremental logic uses watermarks.  
