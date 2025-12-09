"""
Configuration package for Fabric Fraud Detection Accelerator
"""

from .config import (
    # Storage
    STORAGE_ACCOUNT_NAME,
    STORAGE_ACCOUNT_KEY,
    STORAGE_CONTAINER_CLAIMS,
    STORAGE_CONTAINER_FAILED,
    get_storage_connection_string,
    
    # AI Services
    AI_ENDPOINT,
    AI_MODEL_NAME,
    AI_API_KEY,
    
    # Event Hub
    EVENT_HUB_CONNECTION_STRING,
    EVENT_HUB_NAME,
    
    # Application Insights
    APP_INSIGHTS_CONNECTION_STRING,
    
    # Fabric
    WORKSPACE_ID,
    RAW_LAKEHOUSE_ID,
    CURATE_LAKEHOUSE_ID,
    PUBLISH_LAKEHOUSE_ID,
    get_onelake_path,
    
    # MLflow
    MLFLOW_EXPERIMENT_FRAUD_DETECTION,
    MODEL_URI_VEHICLE_FRAUD,
    MODEL_URI_PROPERTY_FRAUD,
    MODEL_URI_FURNITURE_FRAUD,
    MODEL_URI_MOBILE_FRAUD,
    
    # Tables
    RAW_SCHEMA,
    RAW_TABLES,
    CURATE_SCHEMA,
    CURATE_TABLES,
    PUBLISH_SCHEMA,
    PUBLISH_TABLES,
    
    # Observability
    FRESHNESS_THRESHOLDS,
    
    # Helpers
    validate_config,
)

__all__ = [
    "STORAGE_ACCOUNT_NAME",
    "STORAGE_ACCOUNT_KEY",
    "STORAGE_CONTAINER_CLAIMS",
    "STORAGE_CONTAINER_FAILED",
    "get_storage_connection_string",
    "AI_ENDPOINT",
    "AI_MODEL_NAME",
    "AI_API_KEY",
    "EVENT_HUB_CONNECTION_STRING",
    "EVENT_HUB_NAME",
    "APP_INSIGHTS_CONNECTION_STRING",
    "WORKSPACE_ID",
    "RAW_LAKEHOUSE_ID",
    "CURATE_LAKEHOUSE_ID",
    "PUBLISH_LAKEHOUSE_ID",
    "get_onelake_path",
    "MLFLOW_EXPERIMENT_FRAUD_DETECTION",
    "MODEL_URI_VEHICLE_FRAUD",
    "MODEL_URI_PROPERTY_FRAUD",
    "MODEL_URI_FURNITURE_FRAUD",
    "MODEL_URI_MOBILE_FRAUD",
    "RAW_SCHEMA",
    "RAW_TABLES",
    "CURATE_SCHEMA",
    "CURATE_TABLES",
    "PUBLISH_SCHEMA",
    "PUBLISH_TABLES",
    "FRESHNESS_THRESHOLDS",
    "validate_config",
]

