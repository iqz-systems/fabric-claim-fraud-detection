# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

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

# # Incremental Load: Fact Claims and Related Tables
# 
# This notebook performs incremental loading of fact tables for the fraud detection
# data model. It uses watermark-based tracking to process only new records.
# 
# ## Tables Processed
# - **fact_claims**: Core claims with cumulative amounts, ratios, and flags
# - **fact_customer_claims**: Customer-level claim aggregations
# - **fact_customer_lifecycle**: Customer tenure and behavior metrics
# - **claim_settlements**: Settlement tracking for each claim
# 
# ## Data Flow
# Raw Layer → Curate Layer → Publish Layer

# CELL ********************

# Run utility logger (assumes utilitylogger notebook exists)
# %run utilitylogger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import logging
from datetime import datetime, timedelta
import time
import getpass
import socket
import psutil
from opencensus.ext.azure.log_exporter import AzureLogHandler

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

# Application Insights for observability (optional but recommended)
APP_INSIGHTS_CONNECTION_STRING = os.getenv(
    "APP_INSIGHTS_CONNECTION_STRING",
    "InstrumentationKey=<your-key>;IngestionEndpoint=https://<region>.in.applicationinsights.azure.com/"
)

# Table configurations
RAW_SCHEMA = "raw.frauddetection"
CURATE_SCHEMA = "curate.frauddetection"
PUBLISH_SCHEMA = "publish.frauddetection"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Logging Setup

# CELL ********************

def setup_logger():
    """Set up Application Insights logger."""
    logger = logging.getLogger("metrics_logger")
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers
    if not logger.hasHandlers():
        try:
            logger.addHandler(AzureLogHandler(connection_string=APP_INSIGHTS_CONNECTION_STRING))
        except Exception as e:
            print(f"Warning: Could not set up Application Insights logging: {e}")
            # Fall back to console logging
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)
    
    return logger

logger = setup_logger()

# Track execution metrics
start_time = datetime.utcnow()
logger.info(
    f"publish_incremental_load Notebook execution started at {start_time}", 
    extra={"custom_dimensions": {"notebook_name": "publish_incremental_load", "start_time": start_time.isoformat()}}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Model Notes
# 
# ### Dimension Tables
# 
# - **dim_date**: Calendar attributes for time-based analysis
# - **dim_products**: Insurance product attributes
# - **dim_customers**: Customer identification and demographics
# - **dim_policies**: Policy details including coverage dates and premiums
# 
# ### Fact Tables
# 
# - **fact_claims**: Claim and settlement data with fraud indicators
# - **fact_incident_loss_by_product**: Total claims and losses by product
# - **fact_customer_lifecycle**: Customer behavior and retention metrics
# - **fact_customer_claims**: Customer claim activity summary
# - **fact_claim_settlements**: Settlement details and audit tracking

# MARKDOWN ********************

# ## Process Fact Claims

# CELL ********************

# Load claims with cumulative amounts and ratios
logger.info("Starting fact_claims processing")

claims_df = spark.sql(f"""
    SELECT * FROM {RAW_SCHEMA}.claims 
    -- Uncomment for incremental load:
    -- WHERE record_timestamp >= (
    --     SELECT MAX(last_processed_value) 
    --     FROM {CURATE_SCHEMA}.pipeline_watermark 
    --     WHERE pipeline_name='end_to_end_claimsflow'
    -- )
""")

logger.info(f"Claims records fetched: {claims_df.count()}", extra={"custom_dimensions": {"step": "fact_claims"}})

# Calculate cumulative claim amount per policy
window_policy = Window.partitionBy("policy_no").orderBy("claim_datetime")

fact_claims_df = claims_df.withColumn(
    "cumulative_claim_amount_per_policy",
    F.sum("claim_total").over(window_policy)
).withColumn(
    "high_claim_flag", 
    F.when(F.col("claim_total") > 20000, F.lit(True)).otherwise(F.lit(False))
).withColumn(
    "injury_ratio", 
    F.col("claim_injury") / F.col("claim_total")
).withColumn(
    "property_ratio", 
    F.col("claim_property") / F.col("claim_total")
).withColumn(
    "product_ratio", 
    F.col("claim_vehicle") / F.col("claim_total")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create temp view for SQL merge
fact_claims_df.createOrReplaceTempView("staging_fact_claims")
logger.info("Temp view staging_fact_claims created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%%sql
-- Merge into curate fact_claims table
MERGE INTO curate.frauddetection.fact_claims AS target
USING staging_fact_claims AS source
ON target.claim_no = source.claim_no
 
WHEN MATCHED THEN
    UPDATE SET
        target.cumulative_claim_amount_per_policy = source.cumulative_claim_amount_per_policy,
        target.high_claim_flag = source.high_claim_flag,
        target.injury_ratio = source.injury_ratio,
        target.property_ratio = source.property_ratio,
        target.product_ratio = source.product_ratio
 
WHEN NOT MATCHED THEN
    INSERT (
        claim_no, policy_no, product_id, claim_datetime, incident_date,
        incident_hour, incident_type, incident_severity, incident_zip_code,
        incident_latitude, incident_longitude, collision_type,
        collision_number_of_vehicles_involved, driver_age, driver_insured_relationship,
        driver_license_issue_date, claim_amount_total, claim_amount_injury,
        claim_amount_property, claim_amount_product, number_of_witnesses,
        suspicious_activity, months_as_customer, record_timestamp,
        cumulative_claim_amount_per_policy, high_claim_flag,
        injury_ratio, property_ratio, product_ratio
    )
    VALUES (
        source.claim_no, source.policy_no, source.product_id, source.claim_datetime,
        source.incident_date, source.incident_hour, source.incident_type,
        source.incident_severity, source.incident_zip_code, source.incident_latitude,
        source.incident_longitude, source.collision_type, source.collision_number_of_vehicles,
        source.driver_age, source.driver_insured_relationship, source.driver_license_issue_date,
        source.claim_total, source.claim_injury, source.claim_property, source.claim_vehicle,
        source.number_of_witnesses, source.suspicious_activity, source.months_as_customer,
        source.record_timestamp, source.cumulative_claim_amount_per_policy,
        source.high_claim_flag, source.injury_ratio, source.property_ratio, source.product_ratio
    );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update pipeline watermark
logger.info("Inserting pipeline watermark for fact_claims")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%%sql
-- Track watermark for incremental processing
INSERT INTO curate.frauddetection.pipeline_watermark 
SELECT 
    'end_to_end_claimsflow' AS pipeline_name, 
    'fact_claims' AS table_name,
    'record_timestamp' AS watermark_column,
    MAX(record_timestamp) AS last_processed_value,
    COALESCE(COUNT(*), 0) AS records_processed, 
    CURRENT_TIMESTAMP AS updated_at 
FROM staging_fact_claims

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info("Pipeline watermark updated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Other Fact Tables

# CELL ********************

# Load source tables
claims_df = spark.table(f"{RAW_SCHEMA}.claims")
policies_df = spark.table(f"{RAW_SCHEMA}.policies")
product_incidents_df = spark.table(f"{RAW_SCHEMA}.product_incidents")
customers_df = spark.table(f"{RAW_SCHEMA}.customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Claims per Customer (Last 6 Months)

# CELL ********************

# Calculate claims per customer in last 6 months (rolling window)
X_MONTHS = 6
cutoff_date = (datetime.today() - timedelta(days=X_MONTHS * 30)).strftime("%Y-%m-%d")

# Join claims with policies to get customer ID
claims_with_cust_df = claims_df.join(
    policies_df.select("policy_no", "cust_id"),
    on="policy_no",
    how="left"
)

# Filter claims in last X months
claims_recent_df = claims_with_cust_df.filter(F.col("claim_datetime") >= F.lit(cutoff_date))

# Aggregate per customer
claims_per_customer_df = claims_recent_df.groupBy("cust_id").agg(
    F.count("claim_no").alias(f"claims_last_{X_MONTHS}_months"),
    F.sum("claim_total").alias(f"claims_amount_last_{X_MONTHS}_months")
)

# Save to publish layer
claims_per_customer_df.write.mode("overwrite").saveAsTable(f"{PUBLISH_SCHEMA}.fact_claims_per_customer_recent")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Customer Lifecycle Metrics

# CELL ********************

logger.info("Processing fact_customer_lifecycle")

fact_customer_lifecycle = spark.sql(f"""
WITH claim_diffs AS (
    SELECT
        cust_id,
        claim_datetime,
        ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY claim_datetime) AS rn,
        LAG(claim_datetime) OVER (PARTITION BY cust_id ORDER BY claim_datetime) AS prev_claim_datetime
    FROM {RAW_SCHEMA}.claims c 
    LEFT JOIN {RAW_SCHEMA}.policies p ON c.policy_no = p.policy_no 
),
diffs AS (
    SELECT
        cust_id,
        claim_datetime,
        prev_claim_datetime,
        DATEDIFF(day, prev_claim_datetime, claim_datetime) / 30 AS months_between_claims
    FROM claim_diffs
    WHERE prev_claim_datetime IS NOT NULL
),
customer_lifecycle AS (
    SELECT 
        cust_id,
        MIN(pol_issue_date) AS customer_start_date,
        MAX(pol_expiry_date) AS customer_end_date
    FROM {RAW_SCHEMA}.policies 
    GROUP BY cust_id
)
SELECT
    diffs.cust_id,
    ROUND(AVG(months_between_claims), 2) AS avg_months_between_claims,
    MIN(customer_start_date) AS customer_start_date,
    MIN(customer_end_date) AS customer_end_date
FROM diffs
LEFT JOIN customer_lifecycle ON diffs.cust_id = customer_lifecycle.cust_id
GROUP BY diffs.cust_id
""")

fact_customer_lifecycle.write.mode("overwrite").saveAsTable(f"{PUBLISH_SCHEMA}.fact_customer_lifecycle")
logger.info("fact_customer_lifecycle written to table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Customer Claims Summary

# CELL ********************

logger.info("Processing fact_customer_claims")

claims_df = spark.table(f"{RAW_SCHEMA}.claims")
policies_df = spark.table(f"{RAW_SCHEMA}.policies")

claims_with_cust = claims_df.join(
    policies_df.select("policy_no", "cust_id"),
    on="policy_no",
    how="left"
)

cutoff_date = (datetime.today() - timedelta(days=6*30)).strftime("%Y-%m-%d")

fact_customer_claims = claims_with_cust.groupBy("cust_id").agg(
    F.count("claim_no").alias("total_claims"),
    F.sum(F.when(F.col("claim_datetime") >= F.lit(cutoff_date), 1).otherwise(0)).alias("claims_last_6_months"),
    F.avg("claim_total").alias("avg_claim_amount"),
    F.min("claim_datetime").alias("first_claim_date"),
    F.max("claim_datetime").alias("last_claim_date")
).withColumn(
    "months_btw_claims", 
    (F.months_between(F.col("last_claim_date"), F.col("first_claim_date")) + F.lit(1))
).withColumn(
    "claims_per_month", 
    F.col("total_claims") / F.col("months_btw_claims")
)

fact_customer_claims.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{PUBLISH_SCHEMA}.fact_customer_claims")
logger.info("fact_customer_claims written to table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Claim Settlements

# CELL ********************

logger.info("Generating new claim_settlements")

claimsspark_df = spark.sql(f"SELECT * FROM {CURATE_SCHEMA}.fact_claims")
claimsspark_df.createOrReplaceTempView("fact_claims")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the current max settlement ID offset
offset_result = spark.sql(f"""
    SELECT MAX(SUBSTRING(settlement_id, 5, LENGTH(settlement_id) - 4)) AS st
    FROM {PUBLISH_SCHEMA}.claim_settlements
""").collect()
offset = offset_result[0]['st'] if offset_result[0]['st'] else 0
print(f"Current settlement ID offset: {offset}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create new settlements for claims not yet in settlements table
spark.sql(f"""
    SELECT 
        CONCAT('SETT', LPAD(ROW_NUMBER() OVER (ORDER BY claim_no) + {offset}, 5, '0')) AS settlement_id,
        claim_no,
        'IN PROGRESS' AS settlement_status,
        claim_amount_total AS claimed_amount,
        NULL AS approved_amount,
        'NA' AS settlement_reason,
        NULL AS settlement_date,
        FALSE AS auto_settled
    FROM fact_claims
""").createOrReplaceTempView("new_claims")

logger.info("Temp view new_claims created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%%sql
-- Merge new claims into settlements table
MERGE INTO publish.frauddetection.claim_settlements AS target
USING new_claims AS source
ON target.claim_no = source.claim_no
WHEN NOT MATCHED THEN
  INSERT (settlement_id, claim_no, settlement_status, claimed_amount, 
          approved_amount, settlement_reason, settlement_date, auto_settled)
  VALUES (source.settlement_id, source.claim_no, source.settlement_status, 
          source.claimed_amount, source.approved_amount, source.settlement_reason, 
          source.settlement_date, source.auto_settled)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info("claim_settlements merge completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execution Metrics

# CELL ********************

# Log execution metrics
end_time = datetime.utcnow()
duration_sec = round((end_time - start_time).total_seconds(), 2)

cpu = psutil.cpu_percent()
mem = psutil.virtual_memory()
memory_usage_percent = (mem.used / mem.total) * 100

logger.info(
    f"Resource usage - CPU: {cpu}% | Memory: {memory_usage_percent:.2f}%",
    extra={
        "custom_dimensions": {
            "cpu_usage": float(cpu),
            "memory_usage": memory_usage_percent,
            "memory_mb": float(mem.used) / 1024**2,
            "total_memory_mb": float(mem.total) / 1024**2,
            "username": str(getpass.getuser()),
            "host": str(socket.gethostname()),
            "notebook_name": "publish_incremental_load",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": str(duration_sec)
        }
    }
)

logger.info(f"publish_incremental_load completed in {duration_sec} seconds")
print(f"\nExecution completed in {duration_sec} seconds")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
