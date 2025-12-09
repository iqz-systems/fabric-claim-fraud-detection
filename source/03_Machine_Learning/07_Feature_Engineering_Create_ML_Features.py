"""
================================================================================
NOTEBOOK: 07 - Feature Engineering - Create ML Features
================================================================================

PURPOSE:
    Creates 50+ features for fraud detection machine learning models.
    Features are engineered from claims, policies, customers, and product data.

WHAT THIS NOTEBOOK DOES:
    1. Creates temporal features (holidays, weekends, time of day)
    2. Creates claims history features (previous claims, frequency)
    3. Creates financial ratio features (claim-to-value, premium ratios)
    4. Creates policy features (tenure, days to expiry)
    5. Creates customer features (age, payment methods)
    6. Creates geographic features (zip code frequencies)
    7. Saves features to feature store table

PREREQUISITES:
    - Raw layer tables populated (run notebook 01 first)
    - Fact tables created (run notebook 03 first)

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach lakehouse
    4. Run all cells

EXECUTION ORDER:
    Run after: 01_Data_Preparation, 03_Batch_Processing_Incremental_Fact_Load
    Run before: 08_Fraud_Detection_Model_Training

OUTPUT:
    - raw.frauddetection.vehicle_features (feature store table)

FEATURE CATEGORIES:
    - Temporal: 10+ features (holidays, weekends, time of day, day of week)
    - Claims History: 5+ features (previous claims, incident counts)
    - Financial: 8+ features (ratios, claim amounts)
    - Policy: 6+ features (tenure, expiry, premium relationships)
    - Customer: 5+ features (age, payment methods)
    - Geographic: 3+ features (zip frequencies)
    - Product: 4+ features (age, value, make-model frequencies)

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

# # 04 - Feature Engineering: Create ML Features
# 
# This notebook creates 50+ features for fraud detection models.
# 
# **Feature Categories:**
# - Temporal (holidays, weekends, time patterns)
# - Claims History (previous claims, frequency)
# - Financial (ratios, amounts)
# - Policy (tenure, relationships)
# - Customer (demographics, payment methods)
# - Geographic (location frequencies)

# CELL ********************

from pyspark.sql.functions import (
    col, split, to_date, to_timestamp, datediff, dayofweek, 
    dayofmonth, dayofyear, when, year, current_date, udf, 
    count, floor, sum as spark_sum, abs as spark_abs, concat_ws, lit, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, BooleanType
import holidays
from datetime import datetime

print("Feature engineering libraries loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Source Data

# CELL ********************

print("Loading source tables...")

# Load all source tables
df_claims = spark.sql("SELECT * FROM raw.frauddetection.claims")
df_cus_payment_methods = spark.sql("SELECT * FROM raw.frauddetection.customer_payment_methods")
df_customers = spark.sql("SELECT * FROM raw.frauddetection.customers")
df_policies = spark.sql("SELECT * FROM raw.frauddetection.policies")
df_prod_incidents = spark.sql("SELECT * FROM raw.frauddetection.product_incidents")
df_products = spark.sql("SELECT * FROM raw.frauddetection.products")

print(f"✅ Claims: {df_claims.count():,} records")
print(f"✅ Customers: {df_customers.count():,} records")
print(f"✅ Products: {df_products.count():,} records")
print(f"✅ Policies: {df_policies.count():,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Create Temporal Features

# CELL ********************

print("Creating temporal features...")

# Parse date columns
df_claims = df_claims.withColumn("incident_date", to_date(col("incident_date"), "dd-MM-yyyy"))
df_claims = df_claims.withColumn("claim_datetime", to_timestamp(col("claim_datetime")))
df_claims = df_claims.withColumn("claim_date", to_date(col("claim_datetime")))

# US Holidays feature
us_holidays = holidays.US()

def is_us_holiday(date_str):
    """Check if date is a US holiday."""
    if date_str is None:
        return False
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return date in us_holidays
    except:
        return False

is_holiday_udf = udf(is_us_holiday, BooleanType())

# Add holiday and weekend flags
df_claims = df_claims.withColumn("is_holiday", is_holiday_udf(col("incident_date").cast("string")))
df_claims = df_claims.withColumn("is_weekend", dayofweek(col("incident_date")).isin([1, 7]))

# Time of day categorization
df_claims = df_claims.withColumn(
    "incident_time_of_day_cat",
    when((col("incident_hour") >= 5) & (col("incident_hour") < 8), "Early_Morning")
    .when((col("incident_hour") >= 8) & (col("incident_hour") < 11), "Morning_Rush")
    .when((col("incident_hour") >= 11) & (col("incident_hour") < 16), "Daytime")
    .when((col("incident_hour") >= 16) & (col("incident_hour") < 18), "Evening_Rush")
    .when((col("incident_hour") >= 18) & (col("incident_hour") < 21), "Evening")
    .otherwise("Night")
)

# Day of week/month/year features
df_claims = df_claims.withColumn("incident_day_of_week", dayofweek(col("incident_date")))
df_claims = df_claims.withColumn("incident_day_of_month", dayofmonth(col("incident_date")))
df_claims = df_claims.withColumn("incident_day_of_year", dayofyear(col("incident_date")))

# Days between incident and claim filing
df_claims = df_claims.withColumn(
    "days_in_between_claim_incident",
    datediff(col("claim_date"), col("incident_date"))
)

print("✅ Temporal features created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Create Claims History Features

# CELL ********************

print("Creating claims history features...")

# Previous claims count per product
prev_claim_window = Window.partitionBy("product_id").orderBy("claim_date").rowsBetween(Window.unboundedPreceding, -1)
df_claims = df_claims.withColumn("previous_claims", count("*").over(prev_claim_window))

# Product incident counts
incident_counts = df_prod_incidents.groupBy('product_id').agg(count("*").alias('product_incident_count'))
df_prod_incidents_with_count = df_prod_incidents.join(incident_counts, on='product_id', how='left')

# Driver license age
df_claims = df_claims.withColumn(
    "driver_license_issue_date",
    to_date(col("driver_license_issue_date"), "dd-MM-yyyy")
)
df_claims = df_claims.withColumn(
    "driver_license_age",
    (year(col("incident_date")) - year(col("driver_license_issue_date")))
)

print("✅ Claims history features created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Create Product Features

# CELL ********************

print("Creating product features...")

# Product age
df_products = df_products.withColumn("model_year", df_products.model_year.cast('int'))
df_products = df_products.withColumn(
    "product_age",
    (year(current_date()) - col('model_year'))
)

# Join claims with products
full_dataset = df_claims.join(df_products, on='product_id', how='left')

# Claim to product value ratio
full_dataset = full_dataset.withColumn(
    "claim_to_product_value_ratio",
    when(col("product_value") > 0, col("claim_total") / col("product_value")).otherwise(0)
)

# Severity score
full_dataset = full_dataset.withColumn(
    "severity_score",
    when(col("incident_severity") == "Low", 0)
    .when(col("incident_severity") == "Medium", 1)
    .when(col("incident_severity") == "High", 2)
    .otherwise(None)
)

# Claim per severity
full_dataset = full_dataset.withColumn(
    "claim_per_severity",
    col("claim_total") / (col("severity_score") + 1)
)

# Claim type ratios
full_dataset = full_dataset \
    .withColumn("injury_claim_ratio", when(col("claim_total") > 0, col("claim_injury") / col("claim_total")).otherwise(0)) \
    .withColumn("vehicle_claim_ratio", when(col("claim_total") > 0, col("claim_vehicle") / col("claim_total")).otherwise(0)) \
    .withColumn("property_claim_ratio", when(col("claim_total") > 0, col("claim_property") / col("claim_total")).otherwise(0))

print("✅ Product features created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Create Customer Features

# CELL ********************

print("Creating customer features...")

# Customer age
df_customers = df_customers.withColumn("date_of_birth", to_date(col("date_of_birth")))
df_customers = df_customers.withColumn(
    "customer_age",
    floor(datediff(current_date(), col("date_of_birth")) / 365.25)
)

# Payment methods aggregation
agg_counts = df_cus_payment_methods.groupBy("cust_id").agg(
    count(when(col("is_active") == "t", True)).alias("number_of_active_cards"),
    count(when(col("is_default") == "t", True)).alias("number_of_default_cards")
)
df_cus_payment_methods_agg = df_cus_payment_methods.join(agg_counts, on="cust_id", how="left")

# Get unique payment features per customer
df_payment_features = df_cus_payment_methods_agg.select(
    "cust_id", 
    "number_of_active_cards", 
    "number_of_default_cards"
).dropDuplicates(["cust_id"])

# Join payment features to customers
df_customers = df_customers.join(
    df_payment_features,
    df_customers.customer_id == df_payment_features.cust_id,
    how="left"
).drop("cust_id")

# Fill nulls
df_customers = df_customers.fillna({
    "number_of_active_cards": 0,
    "number_of_default_cards": 0
})

print("✅ Customer features created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Create Policy Features

# CELL ********************

print("Creating policy features...")

# Join with policies
full_dataset = full_dataset.join(df_policies, on='policy_no', how='left')

# Parse policy dates
full_dataset = full_dataset \
    .withColumn("pol_expiry_date", to_date(col("pol_expiry_date"))) \
    .withColumn("pol_issue_date", to_date(col("pol_issue_date"))) \
    .withColumn("pol_eff_date", to_date(col("pol_eff_date")))

# Days in policy
full_dataset = full_dataset.withColumn(
    "days_in_policy",
    datediff(col("claim_date"), col("pol_issue_date"))
)

# Vehicle age at incident
full_dataset = full_dataset.withColumn(
    "vehicle_age",
    year(col("incident_date")) - col("model_year").cast("int")
)

# Claim to premium ratio
full_dataset = full_dataset.withColumn(
    "claim_to_premium",
    col('claim_total') / col('premium')
)

# Claim to sum insured ratio
full_dataset = full_dataset.withColumn(
    "claim_sum_ratio",
    col('claim_total') / col('sum_insured')
)

# Days since policy effective
full_dataset = full_dataset.withColumn(
    "days_since_policy_effective",
    datediff(col('claim_date'), col('pol_eff_date'))
)

# Days until policy expiry
full_dataset = full_dataset.withColumn(
    "days_till_policy_expiry",
    datediff(col('pol_expiry_date'), col('claim_date'))
)

print("✅ Policy features created")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 7: Join All Features

# CELL ********************

print("Joining all features...")

# Select customer features
df_customers_selected = df_customers.select(
    col("customer_id"),
    col("gender"),
    col("customer_age"),
    col("number_of_active_cards"),
    col("number_of_default_cards")
)

# Join customers
full_dataset = full_dataset.join(
    df_customers_selected,
    full_dataset["cust_id"] == df_customers_selected["customer_id"],
    how="left"
).drop("customer_id")

# Join product incidents
incidents_unique = df_prod_incidents_with_count.select(
    "product_id", "incident_date", "product_incident_count"
).withColumn("incident_found", lit(1))

full_dataset = full_dataset.join(
    incidents_unique,
    on=["product_id", "incident_date"],
    how="left"
)

full_dataset = full_dataset.withColumn(
    "incident_occured",
    when(full_dataset["incident_found"] == 1, 1).otherwise(0)
).withColumn(
    "product_incident_count",
    coalesce(full_dataset["product_incident_count"], lit(0))
).drop("incident_found")

print("✅ All features joined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 8: Vehicle-Specific Features

# CELL ********************

print("Creating vehicle-specific features...")

# Filter to vehicle claims
full_dataset_vehicles = full_dataset.filter(full_dataset['product_type'] == 'vehicle')

# Make-model frequency
full_dataset_vehicles = full_dataset_vehicles.withColumn(
    "make_model", concat_ws("_", col("make"), col("model"))
)
make_model_freq = full_dataset_vehicles.groupBy("make_model").agg(count("*").alias("make_model_freq"))
full_dataset_vehicles = full_dataset_vehicles.join(make_model_freq, on="make_model", how="left").drop("make_model")

# Zip code frequencies
zip_freq = full_dataset_vehicles.groupBy("zip_code").agg(count("*").alias("zip_freq"))
full_dataset_vehicles = full_dataset_vehicles.join(zip_freq, on="zip_code", how="left").drop('zip_code')

zip_freq_incident = full_dataset_vehicles.groupBy("incident_zip_code").agg(count("*").alias("zip_freq_incident"))
full_dataset_vehicles = full_dataset_vehicles.join(zip_freq_incident, on="incident_zip_code", how="left").drop('incident_zip_code')

print(f"✅ Vehicle features created: {full_dataset_vehicles.count():,} records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 9: Select Final Features

# CELL ********************

print("Selecting final feature set...")

# Select features for model
vehicle_features = full_dataset_vehicles.select(
    "claim_no",
    "incident_hour",
    "incident_type",
    "incident_severity",
    "collision_type",
    "collision_number_of_vehicles",
    "driver_age",
    "driver_insured_relationship",
    "claim_total",
    "claim_injury",
    "claim_property",
    "claim_vehicle",
    "number_of_witnesses",
    "months_as_customer",
    "is_holiday",
    "is_weekend",
    "incident_time_of_day_cat",
    "incident_day_of_week",
    "days_in_between_claim_incident",
    "previous_claims",
    "driver_license_age",
    "product_subtype",
    "model_year",
    "product_value",
    "claim_to_product_value_ratio",
    "severity_score",
    "claim_per_severity",
    "injury_claim_ratio",
    "vehicle_claim_ratio",
    "property_claim_ratio",
    "policytype",
    "borough",
    "neighborhood",
    "sum_insured",
    "premium",
    "deductable",
    "days_in_policy",
    "incident_day_of_month",
    "incident_day_of_year",
    "vehicle_age",
    "claim_to_premium",
    "claim_sum_ratio",
    "days_since_policy_effective",
    "days_till_policy_expiry",
    "gender",
    "customer_age",
    "number_of_active_cards",
    "number_of_default_cards",
    "product_incident_count",
    "incident_occured",
    "make_model_freq",
    "zip_freq_incident",
    "zip_freq"
)

# Cast columns to appropriate types
vehicle_features = vehicle_features.withColumn("days_in_between_claim_incident", spark_abs(col("days_in_between_claim_incident").cast('int')))
vehicle_features = vehicle_features.withColumn("days_since_policy_effective", spark_abs(col("days_since_policy_effective").cast('int')))
vehicle_features = vehicle_features.withColumn("days_till_policy_expiry", spark_abs(col("days_till_policy_expiry").cast('int')))
vehicle_features = vehicle_features.withColumn("driver_license_age", spark_abs(col("driver_license_age").cast('int')))
vehicle_features = vehicle_features.withColumn("days_in_policy", spark_abs(col("days_in_policy").cast('int')))
vehicle_features = vehicle_features.withColumn("vehicle_age", spark_abs(col("vehicle_age").cast('int')))

feature_count = vehicle_features.count()
feature_cols = len(vehicle_features.columns)

print(f"\n✅ Final feature set: {feature_count:,} records, {feature_cols} features")
vehicle_features.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 10: Save Features to Feature Store

# CELL ********************

print("Saving features to feature store...")

# Save to feature store table
vehicle_features.write.mode("overwrite").saveAsTable("raw.frauddetection.vehicle_features")
print(f"✅ Vehicle features saved to raw.frauddetection.vehicle_features")
print(f"   Records: {feature_count:,}")
print(f"   Features: {feature_cols}")
print("\n✅ Feature engineering complete!")
print("Next: Run notebook 05_Fraud_Detection_Model_Training")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

