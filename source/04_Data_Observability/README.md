# Data Observability

This folder contains notebooks, semantic models, reports, and dashboards for monitoring data quality, freshness, and schema changes.

## Contents

| Item | Purpose | Type |
|------|---------|------|
| `09_Data_Freshness_Monitoring.py` | Monitor data freshness | Copy-paste ready notebook |
| `10_Pipeline_Start_Tracking.py` | Pipeline start tracking | Copy-paste ready notebook |
| `11_Pipeline_End_Tracking.py` | Pipeline end tracking | Copy-paste ready notebook |
| `data_observability_semantic_model.SemanticModel/` | Power BI semantic model | Data model |
| `data_observability_report.Report/` | Power BI report | Dashboard |
| `data_quality_pipeline.DataPipeline/` | Data quality pipeline | Pipeline |
| `KQL/Querysets/RealTimeQueryset.json` | KQL queryset for real-time monitoring | KQL |
| `KQL/Dashboards/RealTimeDashboard.json` | Real-time KQL dashboard | KQL |
| `KQL/Dashboards/Legacy/RealTimeDashboard_old.json` | Legacy KQL dashboard (optional) | KQL |

## Quick Start

1. **For Freshness Monitoring:**
   - Run `data_freshness` notebook
   - Monitors all layers (Raw, Curate, Publish, Eventhouse)

2. **For Pipeline Tracking:**
   - Run `start_datapipeline` at pipeline start
   - Run `end_datapipeline` at pipeline end

## How to Use

Each notebook is **copy-paste ready**:

1. Open the `.py` file (or `notebook-content.py` in `.Notebook` folders)
2. Copy ALL content (Ctrl+A, Ctrl+C)
3. In Fabric: Create notebook → View code → Paste
4. Attach lakehouses
5. Update configuration:
   - Application Insights connection string
   - Workspace and lakehouse IDs
6. Run!

## Monitoring Capabilities

### Data Freshness
- **Checks:** Timestamp-based freshness validation
- **Thresholds:** Configurable per table
- **Layers:** Raw, Curate, Publish, Eventhouse
- **Output:** Freshness status (Fresh/Critical)

### Schema Monitoring
- **Checks:** Schema drift detection
- **Alerts:** Schema changes tracked
- **Output:** Schema change logs

### Volume Metrics
- **Checks:** Record count monitoring
- **Alerts:** Volume anomalies detected
- **Output:** Volume trend analysis

## Configuration

### Required
- `APP_INSIGHTS_CONNECTION_STRING` - Application Insights for logging
- `WORKSPACE_ID` - Fabric workspace ID
- `EVENTHOUSE_LAKEHOUSE_ID` - Eventhouse lakehouse ID (for Eventhouse monitoring)

### Optional
- Table-specific thresholds
- Custom monitoring rules

## Integration

These notebooks can be:
- **Scheduled:** Run on a schedule (e.g., daily)
- **Pipeline-integrated:** Run as part of data pipelines
- **Standalone:** Run independently for ad-hoc checks

## Output

- **Application Insights:** All metrics logged to Application Insights
- **Tables:** Monitoring results saved to `raw.dataquality.*` tables
- **Dashboards:** Can be visualized in Power BI

## Related Notebooks

- **Batch Processing:** See `../01_Batch_Processing/` for pipeline notebooks
- **Main README:** See `../README.md` for overall architecture

## Semantic Model

The semantic model (`data_observability_semantic_model.SemanticModel`) includes tables for:
- **Volume Metrics** - Record count monitoring
- **Freshness Checks** - Data freshness validation
- **Schema Changes** - Schema drift detection
- **Data Quality** - Quality test cases and executions
- **Execution Errors** - Error tracking

### Tables

- `volumemetrics` / `volumemetricslatest` - Volume monitoring
- `freshnesschecks` / `freshnesscheckslatest` - Freshness monitoring
- `schemachangelog` / `schemachangelatest` - Schema change tracking
- `dataqualitymaster` - Quality test definitions
- `dataqualityexecutions` - Execution history
- `dataqualitytestcases` - Test case definitions
- `dataqualityexecutionerrors` - Error logs
- `dqrunsummary` - Run summaries
- `dim_table` - Dimension table

## Reports and Dashboards

- **Power BI Report:** Use `data_observability_report.Report` as reference for building custom dashboards
- **Pipeline:** `data_quality_pipeline.DataPipeline` for automated quality checks
- **KQL Dashboards & Querysets:** Use `KQL/Querysets/RealTimeQueryset.json` with `KQL/Dashboards/RealTimeDashboard.json` (legacy version in `KQL/Dashboards/Legacy/`).

## Notes

- **Application Insights:** Recommended for production monitoring
- **Thresholds:** Adjust based on your SLAs
- **Scheduling:** Can be scheduled in Fabric pipelines
- **Semantic Model:** Can be imported into Power BI for custom dashboards

