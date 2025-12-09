"""
Configuration file for Fabric Fraud Detection Accelerator

This file contains placeholder values for all credentials and configuration.
Replace these with your actual values before running the notebooks.

SECURITY WARNING: 
- Never commit actual credentials to source control
- Use Azure Key Vault or Fabric environment variables in production
- This file should be added to .gitignore after adding real values
"""

import os

# =============================================================================
# AZURE STORAGE CONFIGURATION
# =============================================================================
# Storage account for claim PDF forms
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "<your-storage-account-name>")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY", "<your-storage-account-key>")
STORAGE_CONTAINER_CLAIMS = os.getenv("STORAGE_CONTAINER_CLAIMS", "claim-pdfs")
STORAGE_CONTAINER_FAILED = os.getenv("STORAGE_CONTAINER_FAILED", "claim-pdfs-failed")

# =============================================================================
# AZURE AI / COGNITIVE SERVICES CONFIGURATION
# =============================================================================
# Azure AI Foundry endpoint for document processing
AI_ENDPOINT = os.getenv("AI_ENDPOINT", "https://<your-ai-service>.services.ai.azure.com/models")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "Phi-4-multimodal-instruct")
AI_API_KEY = os.getenv("AI_API_KEY", "<your-ai-api-key>")

# =============================================================================
# AZURE EVENT HUB CONFIGURATION
# =============================================================================
# Event Hub for streaming claims
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING",
    "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy-name>;SharedAccessKey=<your-key>;EntityPath=<hub-name>"
)
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "<your-event-hub-name>")

# =============================================================================
# AZURE APPLICATION INSIGHTS CONFIGURATION
# =============================================================================
# Application Insights for observability and logging
APP_INSIGHTS_CONNECTION_STRING = os.getenv(
    "APP_INSIGHTS_CONNECTION_STRING",
    "InstrumentationKey=<your-instrumentation-key>;IngestionEndpoint=https://<region>.in.applicationinsights.azure.com/"
)

# =============================================================================
# MICROSOFT FABRIC CONFIGURATION
# =============================================================================
# Fabric workspace and lakehouse IDs
# These will be auto-populated when you attach lakehouses in Fabric
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "<your-workspace-id>")
RAW_LAKEHOUSE_ID = os.getenv("RAW_LAKEHOUSE_ID", "<your-raw-lakehouse-id>")
CURATE_LAKEHOUSE_ID = os.getenv("CURATE_LAKEHOUSE_ID", "<your-curate-lakehouse-id>")
PUBLISH_LAKEHOUSE_ID = os.getenv("PUBLISH_LAKEHOUSE_ID", "<your-publish-lakehouse-id>")

# OneLake paths (will be constructed based on workspace and lakehouse IDs)
def get_onelake_path(workspace_id: str, lakehouse_id: str, path: str = "") -> str:
    """Construct OneLake ABFSS path"""
    base = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
    return f"{base}/{path}" if path else base

# =============================================================================
# MLFLOW CONFIGURATION
# =============================================================================
# MLflow experiment names (these are safe to share)
MLFLOW_EXPERIMENT_FRAUD_DETECTION = "fraud_detection_experiment"
MLFLOW_EXPERIMENT_AUTOML = "automl_experiment"

# Model URIs - these reference specific trained model runs
# Replace with your actual model run IDs after training
MODEL_URI_VEHICLE_FRAUD = os.getenv(
    "MODEL_URI_VEHICLE_FRAUD",
    "runs:/<your-run-id>/random_forest_model"
)
MODEL_URI_PROPERTY_FRAUD = os.getenv(
    "MODEL_URI_PROPERTY_FRAUD",
    "runs:/<your-run-id>/logistic_regression_property_dataset"
)
MODEL_URI_FURNITURE_FRAUD = os.getenv(
    "MODEL_URI_FURNITURE_FRAUD",
    "runs:/<your-run-id>/logistic_regression_furniture_dataset"
)
MODEL_URI_MOBILE_FRAUD = os.getenv(
    "MODEL_URI_MOBILE_FRAUD",
    "runs:/<your-run-id>/logistic_regression_mobile_electronics_dataset"
)

# =============================================================================
# TABLE CONFIGURATIONS
# =============================================================================
# Raw layer tables
RAW_SCHEMA = "raw.frauddetection"
RAW_TABLES = {
    "claims": f"{RAW_SCHEMA}.claims",
    "customers": f"{RAW_SCHEMA}.customers",
    "policies": f"{RAW_SCHEMA}.policies",
    "products": f"{RAW_SCHEMA}.products",
    "product_incidents": f"{RAW_SCHEMA}.product_incidents",
    "customer_payment_methods": f"{RAW_SCHEMA}.customer_payment_methods",
}

# Curate layer tables
CURATE_SCHEMA = "curate.frauddetection"
CURATE_TABLES = {
    "fact_claims": f"{CURATE_SCHEMA}.fact_claims",
    "pipeline_watermark": f"{CURATE_SCHEMA}.pipeline_watermark",
}

# Publish layer tables
PUBLISH_SCHEMA = "publish.frauddetection"
PUBLISH_TABLES = {
    "fact_claims": f"{PUBLISH_SCHEMA}.fact_claims",
    "fact_customer_claims": f"{PUBLISH_SCHEMA}.fact_customer_claims",
    "fact_customer_lifecycle": f"{PUBLISH_SCHEMA}.fact_customer_lifecycle",
    "claim_settlements": f"{PUBLISH_SCHEMA}.claim_settlements",
    "fact_claims_per_customer_recent": f"{PUBLISH_SCHEMA}.fact_claims_per_customer_recent",
    "dim_policies": f"{PUBLISH_SCHEMA}.dim_policies",
}

# =============================================================================
# DATA OBSERVABILITY CONFIGURATION
# =============================================================================
# Freshness thresholds in seconds
FRESHNESS_THRESHOLDS = {
    "claims": 3600,  # 1 hour
    "customers": 2628000,  # ~1 month
    "products": 2628000,  # ~1 month
    "policies": 86400,  # 1 day
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def get_storage_connection_string(account_name: str = None, account_key: str = None) -> str:
    """Generate Azure Storage connection string"""
    account_name = account_name or STORAGE_ACCOUNT_NAME
    account_key = account_key or STORAGE_ACCOUNT_KEY
    return f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

def validate_config():
    """Validate that required configuration values are set"""
    required = [
        ("STORAGE_ACCOUNT_NAME", STORAGE_ACCOUNT_NAME),
        ("STORAGE_ACCOUNT_KEY", STORAGE_ACCOUNT_KEY),
        ("EVENT_HUB_CONNECTION_STRING", EVENT_HUB_CONNECTION_STRING),
    ]
    
    missing = []
    for name, value in required:
        if value.startswith("<") and value.endswith(">"):
            missing.append(name)
    
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    
    return True

