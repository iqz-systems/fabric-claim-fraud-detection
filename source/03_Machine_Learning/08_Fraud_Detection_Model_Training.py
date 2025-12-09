"""
================================================================================
NOTEBOOK: 08 - Fraud Detection Model Training and Prediction
================================================================================

PURPOSE:
    Trains a Random Forest classifier for fraud detection and generates
    predictions on new claims.

WHAT THIS NOTEBOOK DOES:
    1. Loads feature data from feature store
    2. Encodes categorical features (One-Hot Encoding)
    3. Trains Random Forest model with MLflow tracking
    4. Evaluates model performance (AUC, Accuracy, F1)
    5. Saves model to MLflow registry
    6. Generates predictions and saves to table

PREREQUISITES:
    - Feature store table created (run notebook 04 first)
    - MLflow configured in Fabric workspace

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach lakehouse
    4. Update MLFLOW_EXPERIMENT_NAME if needed
    5. Run all cells

EXECUTION ORDER:
    Run after: 07_Feature_Engineering_Create_ML_Features
    Can run: Independently if features already exist

OUTPUT:
    - MLflow model: runs:/<run-id>/random_forest_model
    - Predictions table: publish.frauddetection.claims_model_output

MODEL PARAMETERS:
    - Algorithm: Random Forest
    - Num Trees: 100
    - Max Depth: 10
    - Train/Test Split: 80/20

METRICS TRACKED:
    - AUC (Area Under ROC Curve)
    - Accuracy
    - F1 Score

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

# # 05 - Fraud Detection Model Training and Prediction
# 
# This notebook trains a Random Forest classifier for fraud detection.
# 
# **Model Pipeline:**
# 1. Load features → 2. Encode categoricals → 3. Train model → 4. Evaluate → 5. Predict

# CELL ********************

import os
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when
import mlflow
import mlflow.spark

print("ML libraries loaded")

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

# MLflow experiment name
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "fraud_detection_experiment")

# Model parameters
LABEL_COL = 'is_suspicious'
NUM_TREES = 100
MAX_DEPTH = 10
TRAIN_SPLIT = 0.8

# Categorical columns for encoding
CATEGORICAL_COLS = [
    "incident_type",
    "incident_severity",
    "collision_type",
    "driver_insured_relationship",
    "incident_time_of_day_cat",
    "product_subtype",
    "policytype",
    "borough",
    "neighborhood",
    "gender"
]

print(f"Experiment: {MLFLOW_EXPERIMENT_NAME}")
print(f"Model: Random Forest ({NUM_TREES} trees, max depth {MAX_DEPTH})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Load Feature Data

# CELL ********************

print("Loading features from feature store...")

# Load features from feature store
# This should be the output of the Feature Engineering notebook
vehicle_features = spark.sql("SELECT * FROM raw.frauddetection.vehicle_features")
feature_count = vehicle_features.count()
feature_cols = len(vehicle_features.columns)

print(f"✅ Loaded {feature_count:,} feature records")
print(f"   Features: {feature_cols} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Prepare Training Data

# CELL ********************

print("Preparing training data...")

# Check if label column exists, if not create one using rule-based labeling
if LABEL_COL not in vehicle_features.columns:
    print("⚠️  Creating is_suspicious label using rule-based approach...")
    # Simple rule-based labeling for sample runs
    vehicle_features = vehicle_features.withColumn(
        "is_suspicious",
        when(
            (col("claim_total") > 20000) & 
            (col("days_in_between_claim_incident") > 30) &
            (col("number_of_witnesses") == 0),
            1
        ).otherwise(0)
    )

# Remove rows with null label
final_df = vehicle_features.filter(col(LABEL_COL).isNotNull())
print(f"✅ Training data: {final_df.count():,} records")

# Check label distribution
print("\nLabel distribution:")
final_df.groupBy(LABEL_COL).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter to only existing categorical columns
existing_cat_cols = [c for c in CATEGORICAL_COLS if c in final_df.columns]
print(f"\nCategorical columns: {len(existing_cat_cols)}")
print(f"  {existing_cat_cols}")

# Create indexers and encoders for categorical columns
indexers = [StringIndexer(inputCol=c, outputCol=c + "_index", handleInvalid="keep") for c in existing_cat_cols]
encoders = [OneHotEncoder(inputCol=c + "_index", outputCol=c + "_ohe") for c in existing_cat_cols]

# Get numeric columns (exclude categorical and label)
non_feature_cols = existing_cat_cols + [LABEL_COL, "claim_no"]
numeric_cols = [c for c in final_df.columns if c not in non_feature_cols and c not in [c + "_index" for c in existing_cat_cols]]

# Prepare feature columns
encoded_cols = [c + "_ohe" for c in existing_cat_cols]
input_features = numeric_cols + encoded_cols

print(f"\nFeature breakdown:")
print(f"  Numeric features: {len(numeric_cols)}")
print(f"  Encoded features: {len(encoded_cols)}")
print(f"  Total features: {len(input_features)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Train Model

# CELL ********************

print("\nBuilding model pipeline...")

# Create assembler and classifier
assembler = VectorAssembler(inputCols=input_features, outputCol="features", handleInvalid="skip")

rf = RandomForestClassifier(
    numTrees=NUM_TREES,
    maxDepth=MAX_DEPTH,
    labelCol=LABEL_COL,
    featuresCol="features",
    predictionCol="prediction",
    probabilityCol="probability"
)

# Create pipeline
pipeline = Pipeline(stages=indexers + encoders + [assembler, rf])

# Split data
train_df, test_df = final_df.randomSplit([TRAIN_SPLIT, 1 - TRAIN_SPLIT], seed=42)
print(f"✅ Data split:")
print(f"   Training: {train_df.count():,} records ({TRAIN_SPLIT*100}%)")
print(f"   Testing: {test_df.count():,} records ({(1-TRAIN_SPLIT)*100}%)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\nTraining model with MLflow tracking...")

# Train model with MLflow tracking
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

with mlflow.start_run(run_name="random_forest_fraud_detection") as run:
    # Log parameters
    mlflow.log_param("num_trees", NUM_TREES)
    mlflow.log_param("max_depth", MAX_DEPTH)
    mlflow.log_param("train_split", TRAIN_SPLIT)
    mlflow.log_param("num_features", len(input_features))
    
    # Train
    print("  Training Random Forest...")
    model = pipeline.fit(train_df)
    
    # Predict
    print("  Generating predictions...")
    predictions = model.transform(test_df)
    
    # Evaluate
    print("  Evaluating model...")
    evaluator_auc = BinaryClassificationEvaluator(labelCol=LABEL_COL, metricName="areaUnderROC")
    evaluator_acc = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol=LABEL_COL, metricName="f1")
    
    auc = evaluator_auc.evaluate(predictions)
    accuracy = evaluator_acc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    
    # Log metrics
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)
    
    # Log model
    mlflow.spark.log_model(model, "random_forest_model")
    
    print("\n" + "="*60)
    print("MODEL PERFORMANCE")
    print("="*60)
    print(f"AUC:        {auc:.4f}")
    print(f"Accuracy:   {accuracy:.4f}")
    print(f"F1 Score:   {f1:.4f}")
    print("="*60)
    print(f"\nMLflow Run ID: {run.info.run_id}")
    print(f"Model URI: runs:/{run.info.run_id}/random_forest_model")
    print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Generate Predictions

# CELL ********************

print("\nGenerating predictions on all data...")

# Using the trained model directly
all_predictions = model.transform(final_df)

# Save predictions
model_output = all_predictions.select("claim_no", "prediction", "probability")
model_output.write.mode("overwrite").saveAsTable("publish.frauddetection.claims_model_output")
output_count = model_output.count()

print(f"✅ Predictions saved: {output_count:,} records")
print("   Table: publish.frauddetection.claims_model_output")

# Show prediction distribution
print("\nPrediction distribution:")
all_predictions.groupBy("prediction").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Feature Importance

# CELL ********************

print("\nExtracting feature importance...")

# Extract feature importance from Random Forest
rf_model = model.stages[-1]  # Get the RF model from pipeline
feature_importance = rf_model.featureImportances

# Create feature importance DataFrame
import pandas as pd

importance_df = pd.DataFrame({
    'feature': input_features[:len(feature_importance)],
    'importance': feature_importance.toArray()
}).sort_values('importance', ascending=False)

print("\n" + "="*60)
print("TOP 20 FEATURE IMPORTANCES")
print("="*60)
print(importance_df.head(20).to_string(index=False))
print("="*60)

print("\n✅ Model training and prediction complete!")
print("Next: Use predictions in Power BI reports or run notebook 04_Semantic_Model_Prep (Batch Processing)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

