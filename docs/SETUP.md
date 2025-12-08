# Setup Guide

## Prerequisites

### Azure Resources

1. **Azure Storage Account**
   - Create a storage account for claim PDF forms
   - Create two containers: `claim-pdfs` and `claim-pdfs-failed`
   - Note the account name and access key

2. **Azure Event Hub**
   - Create an Event Hub namespace
   - Create an Event Hub for claims streaming
   - Create a shared access policy with Send/Listen permissions
   - Note the connection string

3. **Azure AI Foundry** (Optional - for PDF processing)
   - Deploy Phi-4-multimodal-instruct model
   - Note the endpoint URL and API key

4. **Application Insights** (Optional - for observability)
   - Create an Application Insights resource
   - Note the connection string

### Microsoft Fabric

1. **Workspace**
   - Create a new Fabric workspace
   - Note the workspace ID (from URL)

2. **Lakehouses**
   - Create three lakehouses: `raw`, `curate`, `publish`
   - Note each lakehouse ID

3. **Environment**
   - Create a Spark environment with required packages:
     ```yaml
     dependencies:
       - azure-eventhub
       - azure-storage-blob
       - azure-ai-inference
       - opencensus-ext-azure
       - pymupdf
       - holidays
     ```

## Configuration Steps

### 1. Clone and Configure

```bash
# Navigate to the project
cd fabric-fraud-detection-demo

# Copy environment template
cp config/env.template.txt config/.env

# Edit with your values
notepad config/.env
```

### 2. Update Notebook Metadata

In each notebook, update the metadata section:

```python
# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<your-lakehouse-id>",
# META       "default_lakehouse_workspace_id": "<your-workspace-id>",
# META     }
# META   }
# META }
```

### 3. Create Database Schemas

Run in a Fabric notebook:

```sql
-- Raw Layer
CREATE SCHEMA IF NOT EXISTS raw.frauddetection;
CREATE SCHEMA IF NOT EXISTS raw.dataquality;

-- Curate Layer
CREATE SCHEMA IF NOT EXISTS curate.frauddetection;

-- Publish Layer
CREATE SCHEMA IF NOT EXISTS publish.frauddetection;
```

### 4. Create Base Tables

```sql
-- Pipeline Watermark Table
CREATE TABLE IF NOT EXISTS curate.frauddetection.pipeline_watermark (
    pipeline_name STRING,
    table_name STRING,
    watermark_column STRING,
    last_processed_value TIMESTAMP,
    records_processed LONG,
    updated_at TIMESTAMP
);

-- Data Quality Tables
CREATE TABLE IF NOT EXISTS raw.dataquality.FreshnessChecks (
    Layer STRING,
    TableName STRING,
    LastRecordTimestamp TIMESTAMP,
    FreshnessCheckedAt TIMESTAMP,
    DelayDays INT,
    Status STRING,
    ThresholdDays INT
);
```

### 5. Load Sample Data

Upload sample data files to the raw lakehouse:
- `claims.json` - Sample claims data
- `customers.csv` - Customer records
- `policies.csv` - Policy records
- `products.csv` - Product catalog
- `product_incidents.csv` - Product incident history
- `customer_payment_methods.csv` - Payment methods

### 6. Import Notebooks

1. In Fabric workspace, create new notebooks
2. Copy content from the sanitized notebooks
3. Attach to the appropriate lakehouse

### 7. Create Pipelines

1. Create data pipelines using the JSON templates
2. Update notebook and pipeline IDs in the pipeline JSON
3. Set up pipeline schedules as needed

## Verification

### Test Streaming

```python
# Test Event Hub connection
from azure.eventhub import EventHubProducerClient
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STRING
)
# Should not raise an exception
```

### Test Storage

```python
# Test Storage connection
from azure.storage.blob import BlobServiceClient
blob_service = BlobServiceClient.from_connection_string(
    "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=..."
)
# List containers
containers = blob_service.list_containers()
```

### Test Tables

```sql
-- Verify schemas exist
SHOW SCHEMAS IN raw;
SHOW SCHEMAS IN curate;
SHOW SCHEMAS IN publish;
```

## Troubleshooting

### Common Issues

1. **"Table not found" errors**
   - Ensure schemas are created
   - Check lakehouse attachment in notebook

2. **Event Hub connection errors**
   - Verify connection string format
   - Check network/firewall settings

3. **AI model errors**
   - Verify endpoint URL is correct
   - Check API key permissions
   - Ensure model is deployed and running

4. **Spark session errors**
   - Check environment configuration
   - Verify package versions

### Logging

Enable verbose logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Next Steps

1. Run feature engineering notebook
2. Train ML models
3. Set up pipeline schedules
4. Configure Power BI reports
5. Set up alerting for data quality issues

