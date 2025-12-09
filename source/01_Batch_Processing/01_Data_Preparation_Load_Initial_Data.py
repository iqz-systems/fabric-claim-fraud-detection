"""
================================================================================
NOTEBOOK: 01 - Data Preparation - Load Initial Data
================================================================================

PURPOSE:
    Loads source data files (JSON, CSV) into Raw layer tables and performs
    initial data quality fixes.

WHAT THIS NOTEBOOK DOES:
    1. Loads claims from JSON file and flattens nested structure
    2. Loads reference data (customers, policies, products, etc.) from CSV
    3. Performs data quality fixes (months_as_customer calculation)
    4. Creates base tables in raw.frauddetection schema

PREREQUISITES:
    - Fabric workspace with lakehouse created
    - Sample data files uploaded to lakehouse Files folder:
      * claims.json (or claims_batch/claims.json)
      * customers.csv
      * policies.csv
      * products.csv
      * product_incidents.csv
      * customer_payment_methods.csv

HOW TO USE:
    1. Open Microsoft Fabric Portal
    2. Create a new Notebook (or open existing)
    3. Copy ALL content from this file
    4. Paste into Fabric notebook
    5. Attach your lakehouse (click + Add → Lakehouse)
    6. Update file paths in the code if needed
    7. Run all cells

EXECUTION ORDER:
    Run this notebook FIRST before any other notebooks.

OUTPUT TABLES:
    - raw.frauddetection.claims
    - raw.frauddetection.customers
    - raw.frauddetection.policies
    - raw.frauddetection.products
    - raw.frauddetection.product_incidents
    - raw.frauddetection.customer_payment_methods

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
# META           "id": "<your-curate-lakehouse-id>"
# META         },
# META         {
# META           "id": "<your-publish-lakehouse-id>"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01 - Data Preparation: Load Initial Data
# 
# This notebook loads source data files into the Raw layer tables.
# 
# **Run this notebook FIRST before any other notebooks.**

# CELL ********************

from pyspark.sql.functions import col, floor, months_between, current_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Claims from JSON

# CELL ********************

# ============================================================================
# CONFIGURATION: Update file path to match your data location
# ============================================================================

# Update this path to where your claims JSON file is located
# Common locations:
#   - "Files/claims_batch/claims.json"
#   - "Files/claims_new/claims.json"
#   - Or use full ABFSS path if in different lakehouse

claims_json_path = "Files/claims_batch/claims.json"

print(f"Loading claims from: {claims_json_path}")

# Load and flatten nested JSON structure
df = spark.read.option("multiline", "true").json(claims_json_path)

# Flatten nested JSON structure
flattened_df = df.select(
    "claim_no",
    "policy_no",
    "product_id",
    "claim_datetime",
    col("incident.date").alias("incident_date"),
    col("incident.hour").alias("incident_hour"),
    col("incident.type").alias("incident_type"),
    col("incident.severity").alias("incident_severity"),
    col("incident.zip_code").alias("incident_zip_code"),
    col("incident.latitude").alias("incident_latitude"),
    col("incident.longitude").alias("incident_longitude"),
    col("collision.type").alias("collision_type"),
    col("collision.number_of_vehicles_involved").alias("collision_number_of_vehicles"),
    col("driver.age").alias("driver_age"),
    col("driver.insured_relationship").alias("driver_insured_relationship"),
    col("driver.license_issue_date").alias("driver_license_issue_date"),
    col("claim_amount.total").alias("claim_total"),
    col("claim_amount.injury").alias("claim_injury"),
    col("claim_amount.property").alias("claim_property"),
    col("claim_amount.vehicle").alias("claim_vehicle"),
    "number_of_witnesses",
    "suspicious_activity",
    "months_as_customer",
    "record_timestamp"
)

# Save to raw layer
flattened_df.write.mode("overwrite").saveAsTable("raw.frauddetection.claims")
claims_count = flattened_df.count()
print(f"✅ Claims loaded: {claims_count} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Load Reference Data (CSV Files)

# CELL ********************

# ============================================================================
# CONFIGURATION: Update file paths to match your data location
# ============================================================================

base_path = "Files/claims_new/"  # Update this to your CSV file location

# Load customer payment methods
df_payment = spark.read.format("csv").option("header", "true").load(f"{base_path}customer_payment_methods.csv")
df_payment.write.mode("overwrite").saveAsTable("raw.frauddetection.customer_payment_methods")
print(f"✅ Customer payment methods: {df_payment.count()} records")

# Load customers
df_customers = spark.read.format("csv").option("header", "true").load(f"{base_path}customers.csv")
df_customers.write.mode("overwrite").saveAsTable("raw.frauddetection.customers")
print(f"✅ Customers: {df_customers.count()} records")

# Load policies
df_policies = spark.read.format("csv").option("header", "true").load(f"{base_path}policies.csv")
df_policies.write.mode("overwrite").saveAsTable("raw.frauddetection.policies")
print(f"✅ Policies: {df_policies.count()} records")

# Load product incidents
df_incidents = spark.read.format("csv").option("header", "true").load(f"{base_path}product_incidents.csv")
df_incidents.write.mode("overwrite").saveAsTable("raw.frauddetection.product_incidents")
print(f"✅ Product incidents: {df_incidents.count()} records")

# Load products
df_products = spark.read.format("csv").option("header", "true").load(f"{base_path}products.csv")
df_products.write.mode("overwrite").saveAsTable("raw.frauddetection.products")
print(f"✅ Products: {df_products.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Data Quality Fixes

# CELL ********************

# Fix: Update months_as_customer based on customer_since date
print("Fixing months_as_customer field...")

df_fix = spark.sql("""
    SELECT 
        cl.claim_no, 
        cl.months_as_customer, 
        FLOOR(months_between(current_date(), cs.customer_since)) AS months_since_date
    FROM raw.frauddetection.claims cl 
    INNER JOIN raw.frauddetection.policies pl ON cl.policy_no = pl.policy_no
    INNER JOIN raw.frauddetection.customers cs ON pl.cust_id = cs.customer_id
""")
df_fix.createOrReplaceTempView("months_since_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Update months_as_customer with calculated values
# MAGIC MERGE INTO raw.frauddetection.claims AS CL 
# MAGIC USING months_since_date MSU
# MAGIC ON CL.claim_no = MSU.claim_no
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET CL.months_as_customer = MSU.months_since_date

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Verify Data Load

# CELL ********************

# Verify loaded data
print("\n=== Data Load Summary ===")
tables = [
    "raw.frauddetection.claims",
    "raw.frauddetection.customers",
    "raw.frauddetection.policies",
    "raw.frauddetection.products",
    "raw.frauddetection.product_incidents",
    "raw.frauddetection.customer_payment_methods"
]

for table in tables:
    try:
        count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
        print(f"✅ {table}: {count:,} records")
    except Exception as e:
        print(f"❌ {table}: Error - {str(e)}")

print("\n✅ Data preparation complete!")
print("Next: Run notebook 02_speed_to_raw_claims or 03_fact_claims_incremental_load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

