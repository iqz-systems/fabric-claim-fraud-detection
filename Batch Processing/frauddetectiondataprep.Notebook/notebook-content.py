# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<your-curate-lakehouse-id>",
# META       "default_lakehouse_name": "curate",
# META       "default_lakehouse_workspace_id": "<your-workspace-id>",
# META       "known_lakehouses": [
# META         {
# META           "id": "<your-curate-lakehouse-id>"
# META         },
# META         {
# META           "id": "<your-raw-lakehouse-id>"
# META         },
# META         {
# META           "id": "<your-publish-lakehouse-id>"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fraud Detection Data Preparation
# 
# This notebook handles initial data loading and preparation for the fraud detection system.
# 
# ## Tasks
# 1. Load source data files (JSON, CSV) into Raw layer tables
# 2. Flatten nested JSON structures
# 3. Perform data quality fixes
# 4. Create base tables for downstream processing

# CELL ********************

from pyspark.sql.functions import col, floor, months_between, current_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Claims JSON Data

# CELL ********************

# Load claims from JSON file and flatten nested structure
# Update the path to your actual file location
claims_json_path = "Files/claims_batch/claims.json"

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
print(f"Claims loaded: {flattened_df.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Reference Data (CSV)

# CELL ********************

# Load customer payment methods
df_payment = spark.read.format("csv").option("header", "true").load("Files/claims_new/customer_payment_methods.csv")
df_payment.write.mode("overwrite").saveAsTable("raw.frauddetection.customer_payment_methods")
print(f"Customer payment methods loaded: {df_payment.count()} records")

# Load customers
df_customers = spark.read.format("csv").option("header", "true").load("Files/claims_new/customers.csv")
df_customers.write.mode("overwrite").saveAsTable("raw.frauddetection.customers")
print(f"Customers loaded: {df_customers.count()} records")

# Load policies
df_policies = spark.read.format("csv").option("header", "true").load("Files/claims_new/policies.csv")
df_policies.write.mode("overwrite").saveAsTable("raw.frauddetection.policies")
print(f"Policies loaded: {df_policies.count()} records")

# Load product incidents
df_incidents = spark.read.format("csv").option("header", "true").load("Files/claims_new/product_incidents.csv")
df_incidents.write.mode("overwrite").saveAsTable("raw.frauddetection.product_incidents")
print(f"Product incidents loaded: {df_incidents.count()} records")

# Load products
df_products = spark.read.format("csv").option("header", "true").load("Files/claims_new/products.csv")
df_products.write.mode("overwrite").saveAsTable("raw.frauddetection.products")
print(f"Products loaded: {df_products.count()} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Fixes

# CELL ********************

# Fix: Update months_as_customer based on customer_since date
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

# ## Verify Data

# CELL ********************

# Verify loaded data
tables = [
    "raw.frauddetection.claims",
    "raw.frauddetection.customers",
    "raw.frauddetection.policies",
    "raw.frauddetection.products",
    "raw.frauddetection.product_incidents",
    "raw.frauddetection.customer_payment_methods"
]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]['cnt']
    print(f"{table}: {count} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

