"""
================================================================================
NOTEBOOK: 12 - FinOps - Azure Cost and Usage Data Load
================================================================================

PURPOSE:
    Loads Azure cost and usage data, budgets, and resource optimization events
    into the FinOps lakehouse for cost analysis and optimization tracking.

WHAT THIS NOTEBOOK DOES:
    1. Loads Azure usage and cost data from CSV files
    2. Normalizes resource group names and dates
    3. Loads budget data with date transformations
    4. Loads resource optimization event logs
    5. Creates tables for Power BI semantic model

PREREQUISITES:
    - Fabric workspace with FinOps lakehouse created
    - Sample data files uploaded to lakehouse Files folder:
      * AzureUsage/*.csv (Azure usage and cost data)
      * budgets.csv (budget definitions)
      * optimzation_log.csv (resource optimization events)

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach FinOps lakehouse
    4. Update file paths if your data is in different locations
    5. Run all cells

EXECUTION ORDER:
    Can run: Independently (FinOps is separate from fraud detection)
    Run before: Creating FinOps semantic model

OUTPUT TABLES:
    - azure_usage_cost (Azure usage and cost data)
    - budgets (Budget definitions and tracking)
    - resource_optimization_events (Resource optimization logs)

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
# META       "default_lakehouse": "<your-finops-lakehouse-id>",
# META       "default_lakehouse_name": "finops",
# META       "default_lakehouse_workspace_id": "<your-workspace-id>",
# META       "known_lakehouses": [
# META         {
# META           "id": "<your-finops-lakehouse-id>"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 12 - FinOps: Azure Cost and Usage Data Load
# 
# This notebook loads Azure cost and usage data for FinOps analysis.
# 
# **Data Sources:** Azure Usage, Budgets, Optimization Events

# CELL ********************

from pyspark.sql.functions import upper, col, to_date, to_timestamp

print("FinOps data load libraries loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Azure Usage and Cost Data

# CELL ********************

# ============================================================================
# CONFIGURATION - Update file path if needed
# ============================================================================

# Azure usage CSV files path
AZURE_USAGE_PATH = "Files/AzureUsage/*.csv"

print(f"Loading Azure usage data from: {AZURE_USAGE_PATH}")

# Load Azure usage and cost data
df_usage = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(AZURE_USAGE_PATH)

# Normalize resource group names and format dates
df_usage = df_usage.withColumn("normalizedResourceGroupName", upper(col("resourceGroupName"))) \
        .withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

# Save to table
df_usage.write.mode("overwrite").saveAsTable("azure_usage_cost")
usage_count = df_usage.count()
print(f"Azure usage cost data loaded: {usage_count:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Load Budget Data

# CELL ********************

# ============================================================================
# CONFIGURATION - Update file path if needed
# ============================================================================

# Budget CSV file path
BUDGETS_PATH = "Files/budgets.csv"

print(f"Loading budget data from: {BUDGETS_PATH}")

# Load budget data
df_budgets = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(BUDGETS_PATH)

# Format date columns
df_budgets = df_budgets.withColumn("startDate", to_timestamp(col("startDate"), "MM/dd/yyyy")) \
       .withColumn("endDate", to_timestamp(col("endDate"), "MM/dd/yyyy"))

# Save to table
df_budgets.write.mode("overwrite").saveAsTable("budgets")
budgets_count = df_budgets.count()
print(f"Budget data loaded: {budgets_count:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Load Resource Optimization Events

# CELL ********************

# ============================================================================
# CONFIGURATION - Update file path if needed
# ============================================================================

# Optimization log CSV file path
OPTIMIZATION_LOG_PATH = "Files/optimzation_log.csv"

print(f"Loading optimization events from: {OPTIMIZATION_LOG_PATH}")

# Load resource optimization events
df_optimization = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(OPTIMIZATION_LOG_PATH)

# Save to table
df_optimization.write.mode("overwrite").saveAsTable("resource_optimization_events")
optimization_count = df_optimization.count()
print(f"Resource optimization events loaded: {optimization_count:,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Verify Data Load

# CELL ********************

print("\n=== FinOps Data Load Summary ===")
tables = [
    "azure_usage_cost",
    "budgets",
    "resource_optimization_events"
]

for table in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
        print(f"{table}: {count:,} records")
    except Exception as e:
        print(f"{table}: Error - {str(e)}")

print("\nFinOps data load complete!")
print("Next: Create FinOps semantic model using finops_semantic_model")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

