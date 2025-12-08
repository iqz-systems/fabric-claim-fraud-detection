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

# # Data Observability: Freshness, Schema, and Volume Monitoring
# 
# This notebook monitors data quality across the fraud detection data platform:
# 
# ## Checks Performed
# 1. **Freshness Check** - Validates data is updated within expected thresholds
# 2. **Schema Monitoring** - Detects schema drift and changes
# 3. **Volume Metrics** - Tracks record count changes over time
# 
# ## Monitored Layers
# - Raw Layer (source data)
# - Curate Layer (transformed data)
# - Publish Layer (semantic model data)
# - Eventhouse (real-time streaming data)

# CELL ********************

# Run utility logger (optional - assumes utilitylogger notebook exists)
# %run utilitylogger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from datetime import datetime
import time
import uuid
import platform
import getpass
import socket
import logging
import psutil
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_timestamp, row_number, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, LongType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql import Row
from urllib.parse import urlparse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# =============================================================================
# CONFIGURATION - Replace with your actual values
# =============================================================================

# Application Insights for logging (optional)
APP_INSIGHTS_CONNECTION_STRING = os.getenv(
    "APP_INSIGHTS_CONNECTION_STRING",
    "InstrumentationKey=<your-key>;IngestionEndpoint=https://<region>.in.applicationinsights.azure.com/"
)

# Fabric workspace and lakehouse IDs (replace with your values)
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "<your-workspace-id>")
EVENTHOUSE_LAKEHOUSE_ID = os.getenv("EVENTHOUSE_LAKEHOUSE_ID", "<your-eventhouse-lakehouse-id>")

# Construct base path for eventhouse tables
EVENTHOUSE_BASE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{EVENTHOUSE_LAKEHOUSE_ID}/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Logger Setup

# CELL ********************

def setup_observability_logger():
    """Set up Application Insights logger with fallback to console."""
    try:
        from opencensus.ext.azure.log_exporter import AzureLogHandler
        
        logger = logging.getLogger("ObservabilityLogger")
        logger.setLevel(logging.INFO)

        if not logger.handlers:
            try:
                logger.addHandler(AzureLogHandler(connection_string=APP_INSIGHTS_CONNECTION_STRING))
            except Exception:
                handler = logging.StreamHandler()
                handler.setLevel(logging.INFO)
                logger.addHandler(handler)
        return logger
    except ImportError:
        # Fall back to basic logging if opencensus not available
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("ObservabilityLogger")

logger = setup_observability_logger()
execution_id = str(uuid.uuid4())
start_time = datetime.utcnow()

logger.info(
    f"Data Observability Notebook Execution Started at {start_time}", 
    extra={"custom_dimensions": {"execution_id": execution_id}}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Table Configuration
# 
# Define tables to monitor with their freshness thresholds.

# CELL ********************

# Tables to monitor for freshness
# threshold is in seconds (2628000 = ~1 month, 10080 = ~1 week in minutes)
table_configs = [
    # Raw Layer
    {"layer": "raw", "table": "raw.frauddetection.customers", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "raw", "table": "raw.frauddetection.products", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "raw", "table": "raw.frauddetection.claims", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "raw", "table": "raw.frauddetection.policies", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "raw", "table": "raw.frauddetection.customer_payment_methods", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "raw", "table": "raw.frauddetection.product_incidents", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    
    # Curate Layer
    {"layer": "curate", "table": "curate.frauddetection.fact_claims", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "curate", "table": "curate.frauddetection.fact_policies", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    
    # Publish Layer
    {"layer": "publish", "table": "publish.frauddetection.dim_customer", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "publish", "table": "publish.frauddetection.dim_policies", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    {"layer": "publish", "table": "publish.frauddetection.dim_products", "threshold": 2628000, "timestamp_col": "record_timestamp"},
    
    # Eventhouse Tables (real-time) - Use EVENTHOUSE_BASE_PATH variable
    # Example: {"layer": "eventhouse", "table": f"{EVENTHOUSE_BASE_PATH}/claims_vehicle_raw/", "threshold": 10080, "timestamp_col": "EventEnqueuedUtcTime"},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Freshness Check

# CELL ********************

start_time_freshness = datetime.utcnow()
logger.info(f"Freshness Check Started at {start_time_freshness}", extra={"custom_dimensions": {"execution_id": execution_id}})

freshness_results = []

for cfg in table_configs:
    table_path_or_name = cfg["table"]
    table_layer = cfg["layer"]
    table_name = table_path_or_name.split("/")[-2] if table_layer == "eventhouse" else table_path_or_name.split(".")[-1]
    
    try:
        logger.info(f"Starting freshness check for {table_name}", extra={"custom_dimensions": {"execution_id": execution_id, "table": table_name}})
        
        # Load table based on layer type
        if cfg["layer"] == "eventhouse":
            df = spark.read.format("delta").load(cfg["table"])
        else:
            df = spark.table(cfg["table"])
        
        # Get latest timestamp
        df = df.withColumn("ts", to_timestamp(col(cfg["timestamp_col"])))
        latest_ts = df.agg({"ts": "max"}).collect()[0][0]
        now_ts = datetime.utcnow()

        if latest_ts is None:
            raise ValueError("No data in timestamp column")

        # Calculate delay in days
        delay_days = int((now_ts - latest_ts).total_seconds() / (60 * 60 * 24))

        # Set dynamic threshold based on layer
        if cfg["layer"] in ["raw", "curate", "publish"]:
            threshold_days = 180
        elif cfg["layer"] == "eventhouse":
            threshold_days = 7
        else:
            threshold_days = 30

        # Determine status
        status = "Fresh" if delay_days <= threshold_days else "Critical"

        freshness_results.append((
            cfg["layer"],
            table_name,
            latest_ts,
            now_ts,
            delay_days,
            status,
            threshold_days
        ))
        
        logger.info(
            f"Freshness check successful for {table_name} | Delay Days={delay_days} | Status={status}", 
            extra={"custom_dimensions": {
                "execution_id": execution_id,
                "table": table_name,
                "delay": delay_days,
                "status": status
            }}
        )

    except Exception as e:
        print(f"Error processing {cfg['table']}: {str(e)}")
        logger.error(f"Freshness Check Failed for {table_name}", extra={"custom_dimensions": {"execution_id": execution_id, "table": table_name, "error": str(e)}})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save freshness results
freshness_schema = StructType([
    StructField("Layer", StringType(), True),
    StructField("TableName", StringType(), True),
    StructField("LastRecordTimestamp", TimestampType(), True),
    StructField("FreshnessCheckedAt", TimestampType(), True),
    StructField("DelayDays", IntegerType(), True),
    StructField("Status", StringType(), True),
    StructField("ThresholdDays", IntegerType(), True)
])

if freshness_results:
    freshness_df = spark.createDataFrame(freshness_results, schema=freshness_schema)
    freshness_df.write.mode("append").format("delta").saveAsTable("raw.dataquality.FreshnessChecks")
    
    # Also save latest snapshot
    window_spec = Window.partitionBy("TableName", "Layer").orderBy(col("FreshnessCheckedAt").desc())
    latest_df = freshness_df.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
    latest_df.write.mode("overwrite").format("delta").saveAsTable("raw.dataquality.FreshnessChecksLatest")

end_time_freshness = datetime.utcnow()
duration_sec_freshness = round((end_time_freshness - start_time_freshness).total_seconds(), 2)
logger.info(f"Freshness Check Completed in {duration_sec_freshness} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Monitoring

# CELL ********************

start_time_schema = datetime.utcnow()
logger.info(f"Schema Monitoring Started at {start_time_schema}", extra={"custom_dimensions": {"execution_id": execution_id}})

# Tables to monitor for schema changes
tables_to_check = [
    {"layer": "raw", "table": "raw.frauddetection.customers"},
    {"layer": "raw", "table": "raw.frauddetection.products"},
    {"layer": "raw", "table": "raw.frauddetection.claims"},
    {"layer": "raw", "table": "raw.frauddetection.policies"},
    {"layer": "curate", "table": "curate.frauddetection.fact_claims"},
    {"layer": "publish", "table": "publish.frauddetection.dim_customer"},
    {"layer": "publish", "table": "publish.frauddetection.dim_policies"},
    # Add eventhouse tables with "path" key for delta loading
]

def get_schema_json(df):
    """Extract schema as JSON string."""
    return json.dumps([{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields])

# Try loading existing schema log
try:
    schema_log_df = spark.read.table("raw.dataquality.SchemaChangeLog")
except:
    schema_log_df = spark.createDataFrame([], StructType([
        StructField("Layer", StringType(), True),
        StructField("TableName", StringType(), True),
        StructField("Schema", StringType(), True),
        StructField("CapturedAt", TimestampType(), True),
        StructField("SchemaChangeStatus", StringType(), True),
        StructField("SchemaDiff", StringType(), True)
    ]))

schema_changes = []

for entry in tables_to_check:
    try:
        layer = entry["layer"]
        table_name = entry["table"]
        timestamp = datetime.utcnow()
        
        # Load table
        if layer == "eventhouse" and "path" in entry:
            df = spark.read.format("delta").load(entry["path"])
        else:
            df = spark.table(entry["table"])

        current_schema_json = get_schema_json(df)

        # Get previous schema
        prev_row = schema_log_df.filter((col("Layer") == layer) & (col("TableName") == table_name)).orderBy(col("CapturedAt").desc()).limit(1).collect()
        prev_schema_json = prev_row[0]["Schema"] if prev_row else None

        # Compare schemas
        current_fields = set(f['name'] for f in json.loads(current_schema_json))
        previous_fields = set(f['name'] for f in json.loads(prev_schema_json)) if prev_schema_json else set()

        added = sorted(list(current_fields - previous_fields))
        removed = sorted(list(previous_fields - current_fields))

        status = "Changed" if added or removed else "No Change"
        diff = f"Added: {added}, Removed: {removed}" if status == "Changed" else "No Change"

        logger.info(f"Schema check completed for {table_name} | Status: {status}")
        schema_changes.append((layer, table_name, current_schema_json, timestamp, status, diff))

    except Exception as e:
        print(f"Error processing {entry['table']}: {e}")
        logger.error(f"Error checking schema {entry['table']}: {e}")

# Save schema changes
if schema_changes:
    schema_change_schema = StructType([
        StructField("Layer", StringType(), True),
        StructField("TableName", StringType(), True),
        StructField("Schema", StringType(), True),
        StructField("CapturedAt", TimestampType(), True),
        StructField("SchemaChangeStatus", StringType(), True),
        StructField("SchemaDiff", StringType(), True)
    ])
    result_df = spark.createDataFrame(schema_changes, schema_change_schema)
    result_df.write.mode("append").format("delta").saveAsTable("raw.dataquality.SchemaChangeLog")

end_time_schema = datetime.utcnow()
duration_sec_schema = round((end_time_schema - start_time_schema).total_seconds(), 2)
logger.info(f"Schema Monitoring Completed in {duration_sec_schema} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Volume Metrics

# CELL ********************

start_time_volume = datetime.utcnow()
logger.info(f"Volume Metrics Monitoring Started at {start_time_volume}", extra={"custom_dimensions": {"execution_id": execution_id}})

# Tables to monitor for volume changes
tables_to_monitor = [
    ("raw.frauddetection.customers", "raw"),
    ("raw.frauddetection.claims", "raw"),
    ("raw.frauddetection.products", "raw"),
    ("raw.frauddetection.policies", "raw"),
    ("curate.frauddetection.fact_claims", "curate"),
    ("publish.frauddetection.claim_settlements", "publish"),
    ("publish.frauddetection.fact_claims", "publish"),
    # Add eventhouse paths as needed
]

def extract_table_name(table: str):
    """Extract table name from path or catalog name."""
    if table.startswith("abfss://"):
        path = urlparse(table).path
        return path.strip("/").split("/")[-1]
    else:
        return table.split(".")[-1]

# Load previous metrics
try:
    prev_df = spark.read.table("raw.dataquality.VolumeMetrics")
    latest_ts = prev_df.agg(spark_max("RunTimestamp")).collect()[0][0]
    prev_counts_df = prev_df.filter(col("RunTimestamp") == latest_ts).select("TableName", "Layer", col("CurrentCount").alias("PreviousCount"))
except:
    prev_counts_df = spark.createDataFrame([], StructType([
        StructField("TableName", StringType(), True),
        StructField("Layer", StringType(), True),
        StructField("PreviousCount", LongType(), True)
    ]))

# Monitor volumes
run_ts = datetime.utcnow()
volume_results = []

for table, layer in tables_to_monitor:
    try:
        if table.startswith("abfss://"):
            df = spark.read.format("delta").load(table)
        else:
            df = spark.table(table)
        current_count = df.count()
    except Exception as e:
        print(f"Failed to read {table}: {e}")
        current_count = 0

    table_name = extract_table_name(table)

    # Get previous count
    prev_row = prev_counts_df.filter((col("TableName") == table_name) & (col("Layer") == layer)).collect()
    previous_count = prev_row[0]["PreviousCount"] if prev_row else 0

    # Calculate percentage change
    if previous_count == 0:
        percentage_change = 100.0 if current_count > 0 else 0.0
    else:
        percentage_change = ((current_count - previous_count) / previous_count) * 100

    # Determine status
    if percentage_change <= -30:
        status = "Critical Drop"
    elif percentage_change >= 50:
        status = "Critical High"
    else:
        status = "Normal"

    volume_results.append(Row(
        TableName=table_name,
        Layer=layer,
        PreviousCount=previous_count,
        CurrentCount=current_count,
        PercentageChange=round(percentage_change, 2),
        Status=status,
        RunTimestamp=run_ts
    ))

# Save results
if volume_results:
    result_df = spark.createDataFrame(volume_results)
    result_df.write.mode("append").saveAsTable("raw.dataquality.VolumeMetrics")

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

# Log system info
cpu_usage = psutil.cpu_percent()
mem = psutil.virtual_memory()
memory_usage_percent = (mem.used / mem.total) * 100

logger.info(
    f"Resource usage - CPU: {cpu_usage}% | Memory: {memory_usage_percent:.2f}%",
    extra={"custom_dimensions": {
        "execution_id": execution_id,
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage_percent,
        "notebook_name": "data_freshness",
        "duration": str(duration_sec)
    }}
)

logger.info(f"Data Observability Notebook Execution Completed in {duration_sec} seconds")
print(f"\n=== Execution Summary ===")
print(f"Duration: {duration_sec} seconds")
print(f"Tables Checked: {len(table_configs)}")
print(f"Execution ID: {execution_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

