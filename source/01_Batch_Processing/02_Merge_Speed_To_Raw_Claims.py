"""
================================================================================
NOTEBOOK: 02 - Batch Processing - Merge Speed Layer to Raw Claims
================================================================================

PURPOSE:
    Merges streaming claims from the speed layer (Eventhouse/Event Hub) into
    the batch raw layer claims table. This bridges real-time and batch processing.

WHAT THIS NOTEBOOK DOES:
    1. Reads claims from speed layer staging table (claims_speed)
    2. Transforms and aligns schema to match batch format
    3. Merges new claims into raw.frauddetection.claims table
    4. Handles schema differences between streaming and batch formats

PREREQUISITES:
    - Speed layer claims table populated (raw.frauddetection.claims_speed)
    - Raw layer claims table exists (from notebook 01)
    - Event Hub streaming configured (from Speed Layer notebooks)

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach lakehouse
    4. Update APP_INSIGHTS_CONNECTION_STRING if using observability
    5. Run all cells

EXECUTION ORDER:
    Run after: Speed Layer notebooks (05_Speed_Layer_Stream_Claims or 06_Speed_Layer_PDF)
    Run before: 03_Batch_Processing_Incremental_Fact_Load

OUTPUT:
    - Merged claims in raw.frauddetection.claims table

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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 02 - Batch Processing: Speed to Raw Claims Merge
# 
# This notebook merges streaming claims from the speed layer into batch processing.
# 
# **Data Flow:** Event Hub → Speed Layer → Raw Layer (Batch)

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

logger.info(f"ingest_eventhouse_lakehouse Notebook execution started at {start_time}", extra={
    "custom_dimensions": {
        "start_time": start_time.isoformat(),
        "notebook_name": "ingest_eventhouse_lakehouse"
    }
})

print(f"Started at: {start_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Check Speed Layer Claims

# CELL ********************

logger.info("Querying record count from claims_speed")

# Count records in speed layer staging table
df = spark.sql("SELECT count(*) as cnt FROM raw.frauddetection.claims_speed")
speed_count = df.collect()[0]['cnt']
print(f"Speed layer claims: {speed_count:,} records")

if speed_count == 0:
    print("⚠️  No claims in speed layer. Make sure streaming notebooks have run.")
else:
    print("✅ Speed layer has claims to merge")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Merge Speed Claims into Raw Claims

# CELL ********************

logger.info("Running MERGE from claims_speed into raw.frauddetection.claims")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Merge streaming claims into batch claims table
# MAGIC MERGE INTO raw.frauddetection.claims AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         claim_no,
# MAGIC         policy_no,
# MAGIC         product_id,
# MAGIC         claim_datetime,
# MAGIC         TO_DATE(incident_date, 'dd-MM-yyyy') AS incident_date,
# MAGIC         incident_hour,
# MAGIC         incident_type,
# MAGIC         incident_severity,
# MAGIC         incident_zip_code,
# MAGIC         incident_latitude,
# MAGIC         incident_longitude,
# MAGIC         collision_type,
# MAGIC         CAST(collision_number_of_vehicles_involved AS STRING) AS collision_number_of_vehicles,
# MAGIC         CAST(driver_age AS STRING) AS driver_age,
# MAGIC         driver_insured_relationship,
# MAGIC         TO_DATE(driver_license_issue_date, 'dd-MM-yyyy') AS driver_license_issue_date,
# MAGIC         claim_amount_total AS claim_total,
# MAGIC         claim_amount_injury AS claim_injury,
# MAGIC         claim_amount_property AS claim_property,
# MAGIC         claim_amount_vehicle AS claim_vehicle,
# MAGIC         number_of_witnesses,
# MAGIC         suspicious_activity,
# MAGIC         months_as_customer,
# MAGIC         ingestion_time AS record_timestamp
# MAGIC     FROM raw.frauddetection.claims_speed cs
# MAGIC ) AS source
# MAGIC ON target.claim_no = source.claim_no
# MAGIC WHEN NOT MATCHED THEN INSERT  
# MAGIC (
# MAGIC     claim_no, policy_no, product_id, claim_datetime,
# MAGIC     incident_date, incident_hour, incident_type, incident_severity,
# MAGIC     incident_zip_code, incident_latitude, incident_longitude,
# MAGIC     collision_type, collision_number_of_vehicles, driver_age,
# MAGIC     driver_insured_relationship, driver_license_issue_date,
# MAGIC     claim_total, claim_injury, claim_property, claim_vehicle,
# MAGIC     number_of_witnesses, suspicious_activity, months_as_customer,
# MAGIC     record_timestamp
# MAGIC )
# MAGIC VALUES (  
# MAGIC     source.claim_no, source.policy_no, source.product_id, source.claim_datetime,
# MAGIC     source.incident_date, source.incident_hour, source.incident_type, source.incident_severity,
# MAGIC     source.incident_zip_code, source.incident_latitude, source.incident_longitude,
# MAGIC     source.collision_type, source.collision_number_of_vehicles, source.driver_age,
# MAGIC     source.driver_insured_relationship, source.driver_license_issue_date,
# MAGIC     source.claim_total, source.incident_injury, source.claim_property, source.claim_vehicle,
# MAGIC     source.number_of_witnesses, source.suspicious_activity, source.months_as_customer,
# MAGIC     source.record_timestamp
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify merge
final_count = spark.sql("SELECT COUNT(*) as cnt FROM raw.frauddetection.claims").collect()[0]['cnt']
print(f"\n✅ Merge complete!")
print(f"Total claims in raw layer: {final_count:,}")

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
        "notebook_name": 'ingest_eventhouse_lakehouse',
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration": str(duration_sec)
    }
})

logger.info(f"ingest_eventhouse_lakehouse completed in {duration_sec} seconds")

print("\n" + "="*60)
print("EXECUTION SUMMARY")
print("="*60)
print(f"Duration: {duration_sec} seconds")
print(f"CPU Usage: {cpu}%")
print(f"Memory Usage: {memory_usage_percent:.2f}%")
print(f"Speed Layer Claims: {speed_count:,}")
print(f"Total Raw Claims: {final_count:,}")
print("="*60)
print("\n✅ Speed to raw merge complete!")
print("Next: Run notebook 03_Batch_Processing_Incremental_Fact_Load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

