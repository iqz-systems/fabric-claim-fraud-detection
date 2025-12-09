"""
================================================================================
NOTEBOOK: 10 - Data Observability - Pipeline Start Tracking
================================================================================

PURPOSE:
    Logs the start of the End-to-End claims flow data pipeline.
    Used for observability and tracking pipeline execution times.

WHAT THIS NOTEBOOK DOES:
    1. Logs pipeline start timestamp
    2. Sends metrics to Application Insights
    3. Tracks pipeline execution for monitoring

PREREQUISITES:
    - Application Insights configured (optional but recommended)
    - Used as first step in data pipeline

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach lakehouse
    4. Update APP_INSIGHTS_CONNECTION_STRING if using observability
    5. Run at the start of your pipeline

EXECUTION ORDER:
    Run: At the beginning of data pipelines
    Works with: 11_Pipeline_End_Tracking (run at end)

OUTPUT:
    - Application Insights logs
    - Console output with start timestamp

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

# # 08 - Pipeline Start Tracking
# 
# This notebook logs the start of the End-to-End claims flow data pipeline.

# CELL ********************

import os
from datetime import datetime
import logging

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

# Application Insights configuration (optional but recommended)
APP_INSIGHTS_CONNECTION_STRING = os.getenv(
    "APP_INSIGHTS_CONNECTION_STRING",
    "InstrumentationKey=<your-key>;IngestionEndpoint=https://<region>.in.applicationinsights.azure.com/"
)

start_time = datetime.utcnow()

# Setup logger
logger = logging.getLogger("datapipeline_logger")
logger.handlers = []
logger.setLevel(logging.INFO)

try:
    from opencensus.ext.azure.log_exporter import AzureLogHandler
    if not logger.hasHandlers():
        logger.addHandler(AzureLogHandler(connection_string=APP_INSIGHTS_CONNECTION_STRING))
except Exception as e:
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    print(f"Using console logging: {e}")

logger.info(f"End_to_End_claimsflow Datapipeline Execution Started at {start_time}")
print(f"✅ Pipeline started at {start_time}")
print(f"Pipeline Name: End_to_End_claimsflow")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

