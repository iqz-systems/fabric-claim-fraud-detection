# Fraud Detection & FinOps Accelerator (Fabric)

This repo is an end-to-end, copy-paste friendly template for Microsoft Fabric:
- Batch + Speed ingestion for fraud detection
- Machine Learning for features and model training
- Data Observability (freshness, pipeline logging, KQL dashboards)
- FinOps (cost/usage load, semantic model, report)
- Azure DevOps infrastructure pipelines (sanitized placeholders)

## Structure
- `source/`
  - `01_Batch_Processing/` — Raw → Curate → Publish ETL
  - `02_Speed_Layer/` — Event Hub streaming + AI PDF processing
  - `03_Machine_Learning/` — Feature engineering + model training
  - `04_Data_Observability/` — Freshness/pipeline notebooks, semantic model, Power BI report, KQL assets
  - `05_FinOps/` — Cost/usage load, semantic model, Power BI report
  - `config/` — `config.py` and `env.template.txt` placeholders
- `infrastructure/` — Azure DevOps YAML pipelines, templates, scripts, env variables (placeholders), setup guide
- `docs/` — Setup, architecture, quick start, notebook tips

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
12. `05_FinOps/12_FinOps_Data_Load.py` (optional, independent)

## Highlights per area
- Batch: load/flatten sources, merge speed events, incremental facts, semantic prep.
- Speed: stream claims to Event Hub; AI PDF extraction to Event Hub.
- ML: 50+ features; Random Forest training with MLflow logging and predictions to publish.
- Observability: freshness/volume/schema checks; pipeline start/end logging; semantic model + Power BI report; KQL queryset/dashboard in `04_Data_Observability/KQL/`.
- FinOps: load Azure usage, budgets, optimization events; semantic model + Power BI report.

## Configuration & Secrets
- Replace placeholders in notebooks and `source/config/config.py`.
- Use environment variables or Key Vault; do not commit real secrets.
- Event Hub, storage, AI, App Insights, workspace/lakehouse IDs are all placeholders.

## Infrastructure (Azure DevOps)
- Pipelines: `infrastructure/azure-pipelines*.yml`
- Templates: `pipeline-templates/` (deploy-module, smart-deploy)
- Scripts: `scripts/detect-changes.ps1`, `scripts/detect-drift.ps1`
- Variables: `variables/dev.yml`, `variables/prod.yml` (placeholders)
- Setup: `infrastructure/SETUP-GUIDE.md`
- Update `azureSubscription`, subscription IDs, RGs, locations, names with your values.

## Quick Start
1) Create Fabric workspace + lakehouses (`raw`, `curate`, `publish`; optional FinOps lakehouse).  
2) Copy `source/config/env.template.txt` → `.env` (or set Fabric env vars) with your IDs/keys.  
3) In Fabric: new notebook → **View code** → paste a `.py` → attach lakehouse(s) → run in order above.  
4) Use semantic models/reports as templates; reconnect to your lakehouse tables in Power BI.  
5) (Optional) Configure Azure DevOps pipeline using `infrastructure/SETUP-GUIDE.md`.

## Notes
- All sensitive values are sanitized; fill with your own.  
- Notebooks are designed to be copy-paste ready.  
- Folder names are numbered to keep execution flow clear.

Built by IQZ Systems to accelerate Fabric-based fraud detection, observability, and FinOps. Adapt as needed.



