# Machine Learning Notebooks

This folder contains notebooks for feature engineering and fraud detection model training.

## Notebooks

| File | Purpose | Execution Order |
|------|---------|----------------|
| `07_Feature_Engineering_Create_ML_Features.py` | Create 50+ ML features | After Batch Processing 03 |
| `08_Fraud_Detection_Model_Training.py` | Train Random Forest model | After 07 |

## Quick Start

1. **First:** Run `04_Feature_Engineering_Create_ML_Features.py`
   - Creates feature store table
   - 50+ engineered features

2. **Then:** Run `05_Fraud_Detection_Model_Training.py`
   - Trains Random Forest classifier
   - Tracks with MLflow
   - Generates predictions

## How to Use

Each notebook is **copy-paste ready**:

1. Open the `.py` file
2. Copy ALL content (Ctrl+A, Ctrl+C)
3. In Fabric: Create notebook → View code → Paste
4. Attach lakehouse
5. Update configuration if needed:
   - MLflow experiment name
   - Model parameters
6. Run!

## Feature Engineering (07)

Creates features in these categories:

- **Temporal Features** (10+)
  - Holidays, weekends, time of day, day of week
  
- **Claims History** (5+)
  - Previous claims, incident counts
  
- **Financial Features** (8+)
  - Claim ratios, amounts, premium relationships
  
- **Policy Features** (6+)
  - Tenure, expiry, days in policy
  
- **Customer Features** (5+)
  - Age, payment methods
  
- **Geographic Features** (3+)
  - Zip code frequencies
  
- **Product Features** (4+)
  - Age, value, make-model frequencies

**Output:** `raw.frauddetection.vehicle_features`

## Model Training (08)

- **Algorithm:** Random Forest Classifier
- **Parameters:**
  - Num Trees: 100
  - Max Depth: 10
  - Train/Test Split: 80/20

- **Metrics Tracked:**
  - AUC (Area Under ROC Curve)
  - Accuracy
  - F1 Score

- **MLflow Integration:**
  - Model versioning
  - Experiment tracking
  - Model registry

**Output:** 
- MLflow model: `runs:/<run-id>/random_forest_model`
- Predictions: `publish.frauddetection.claims_model_output`

## Configuration

### Feature Engineering (04)
- Usually no configuration needed
- Uses tables from Raw layer

### Model Training (05)
- `MLFLOW_EXPERIMENT_NAME` - MLflow experiment name (default: "fraud_detection_experiment")
- Model parameters can be adjusted in Configuration section

## Execution Flow

```
Batch Processing (03)
    ↓
04_Feature_Engineering_Create_ML_Features
    ↓
05_Fraud_Detection_Model_Training
    ↓
Use predictions in Power BI or downstream systems
```

## Output Tables

- `raw.frauddetection.vehicle_features` - Feature store
- `publish.frauddetection.claims_model_output` - Predictions

## Related Notebooks

- **Batch Processing:** See `../Batch Processing/` for data preparation
- **Data Observability:** See `../Data Observability/` for monitoring model performance

## Notes

- **Feature Store:** Features saved to `raw.frauddetection.vehicle_features` for reuse
- **Model Versioning:** All models tracked in MLflow
- **Predictions:** Can be joined with fact tables for analysis

