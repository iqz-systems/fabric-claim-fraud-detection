"""
================================================================================
NOTEBOOK: 04 - Batch Processing - Semantic Model Preparation
================================================================================

PURPOSE:
    Prepares fact tables for Power BI semantic model. Creates analytics-ready
    tables with settlement status, fraud indicators, and aggregations.

WHAT THIS NOTEBOOK DOES:
    1. Creates fact_claims table with settlement and fraud indicators
    2. Creates fact_incident_loss_by_product aggregations
    3. Adds calculated fields (processing duration, open claims, fraud flags)
    4. Prepares data for Power BI consumption

PREREQUISITES:
    - Curate layer fact tables created (run notebook 03 first)
    - Claim settlements table populated
    - Publish layer schema exists

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach lakehouses (curate, publish)
    4. Update APP_INSIGHTS_CONNECTION_STRING if using observability
    5. Run all cells

EXECUTION ORDER:
    Run after: 03_Batch_Processing_Incremental_Fact_Load
    Run before: Creating Power BI semantic model

OUTPUT TABLES:
    - publish.frauddetection.fact_claims (with settlement and fraud indicators)
    - publish.frauddetection.fact_incident_loss_by_product (loss aggregations)

================================================================================
"""

# ============================================================================
# COPY EVERYTHING BELOW INTO FABRIC NOTEBOOK
# ============================================================================

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<your-raw-lakehouse-id>",
# META       "default_lakehouse_name": "raw",
# META       "default_lakehouse_workspace_id": "<your-workspace-id>",
# META       "known_lakehouses": [
# META         {
# META           "id": "<your-raw-lakehouse-id>"
# META         },
# META         {
# META           "id": "<your-publish-lakehouse-id>"
# META         },
# META         {
# META           "id": "<your-curate-lakehouse-id>"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 04 - Batch Processing: Semantic Model Preparation
# 
# This notebook prepares fact tables for Power BI semantic model.
# 
# **Output:** Analytics-ready tables with settlement status and fraud indicators

# CELL ********************

import os
import logging
from datetime import datetime
import getpass
import socket
import psutil

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# ============================================================================
# CONFIGURATION - UPDATE IF NEEDED
# ============================================================================

# Application Insights for observability (optional but recommended)
APP_INSIGHTS_CONNECTION_STRING = os.getenv(
    "APP_INSIGHTS_CONNECTION_STRING",
    "InstrumentationKey=<your-key>;IngestionEndpoint=https://<region>.in.applicationinsights.azure.com/"
)

# Setup logger
def setup_logger():
    logger = logging.getLogger("metrics_logger")
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
        try:
            from opencensus.ext.azure.log_exporter import AzureLogHandler
            logger.addHandler(AzureLogHandler(connection_string=APP_INSIGHTS_CONNECTION_STRING))
        except Exception:
            handler = logging.StreamHandler()
            logger.addHandler(handler)
    return logger

logger = setup_logger()
start_time = datetime.utcnow()

logger.info(f"semantic_model_prep Notebook started at {start_time}", extra={"custom_dimensions": {
    "username": getpass.getuser(),
    "host": socket.gethostname(),
    "notebook_name": 'semantic_model_prep',
    "start_time": start_time.isoformat()
}})

print(f"Started at: {start_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Create Fact Claims for Semantic Model

# CELL ********************

logger.info("Loading and transforming fact_claims...")

fact_claims = spark.sql("""
SELECT 
    c.claim_no,
    c.policy_no,
    c.product_id,
    c.claim_datetime,
    c.claim_amount_total,
    s.settlement_status,
    c.suspicious_activity,
    s.auto_settled,
    s.approved_amount,
    CASE 
        WHEN s.settlement_date IS NOT NULL THEN DATEDIFF(month, c.claim_datetime, s.settlement_date)
        ELSE NULL
    END AS claim_processing_duration,
    CASE 
        WHEN s.settlement_status IS NULL THEN 1 ELSE 0
    END AS is_open_claim,
    CASE 
        WHEN c.suspicious_activity = TRUE THEN 1 ELSE 0
    END AS is_fraud_claim,
    CASE 
        WHEN c.suspicious_activity = TRUE AND s.auto_settled = TRUE THEN 1 ELSE 0
    END AS fraud_auto_approved,
    c.record_timestamp as claim_log_date
FROM curate.frauddetection.fact_claims c
LEFT JOIN publish.frauddetection.claim_settlements s ON c.claim_no = s.claim_no
""")

fact_claims.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("publish.frauddetection.fact_claims")
claims_count = fact_claims.count()
logger.info(f"fact_claims saved: {claims_count} records")
print(f"✅ fact_claims: {claims_count:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Create Incident Loss by Product

# CELL ********************

logger.info("Building fact_incident_loss_by_product...")

fact_incident_loss_by_product = spark.sql("""
SELECT 
    p.product_id,
    p.product_type,
    p.product_subtype,
    c.incident_type,
    COUNT(c.claim_no) AS total_claims,
    SUM(s.approved_amount) AS total_loss
FROM raw.frauddetection.claims c
JOIN raw.frauddetection.products p ON c.product_id = p.product_id
JOIN publish.frauddetection.claim_settlements s ON c.claim_no = s.claim_no
GROUP BY p.product_id, p.product_type, p.product_subtype, c.incident_type
""")

fact_incident_loss_by_product.write.mode("overwrite").saveAsTable("publish.frauddetection.fact_incident_loss_by_product")
loss_count = fact_incident_loss_by_product.count()
logger.info(f"fact_incident_loss_by_product saved: {loss_count} records")
print(f"✅ fact_incident_loss_by_product: {loss_count:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execution Summary

# CELL ********************

end_time = datetime.utcnow()
duration_sec = round((end_time - start_time).total_seconds(), 2)

cpu = psutil.cpu_percent()
mem = psutil.virtual_memory()
memory_usage_percent = (mem.used / mem.total) * 100

logger.info(f"Resource usage - CPU: {cpu}% | Memory: {memory_usage_percent:.2f}%", extra={
    "custom_dimensions": {
        "cpu_usage": float(cpu),
        "memory_usage": memory_usage_percent,
        "notebook_name": 'semantic_model_prep',
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration": str(duration_sec)
    }
})

logger.info(f"semantic_model_prep completed in {duration_sec} seconds")

print("\n" + "="*60)
print("EXECUTION SUMMARY")
print("="*60)
print(f"Duration: {duration_sec} seconds")
print(f"CPU Usage: {cpu}%")
print(f"Memory Usage: {memory_usage_percent:.2f}%")
print(f"Start Time: {start_time}")
print(f"End Time: {end_time}")
print("="*60)
print("\n✅ Semantic model preparation complete!")
print("Next: Create Power BI semantic model using publish.frauddetection tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

