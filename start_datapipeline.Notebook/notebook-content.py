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

# # Start Data Pipeline
# 
# This notebook logs the start of the End-to-End claims flow data pipeline.
# Used for observability and tracking pipeline execution times.

# CELL ********************

import os
from datetime import datetime
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Application Insights configuration (optional)
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
print(f"Pipeline started at {start_time}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

