# Architecture Documentation

## System Overview

This fraud detection solution is built on Microsoft Fabric, implementing a lambda architecture with real-time (speed layer) and batch processing capabilities.

## Data Flow

```
                                    ┌──────────────────┐
                                    │  PDF Claim Forms │
                                    │  (Azure Blob)    │
                                    └────────┬─────────┘
                                             │
                                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SPEED LAYER                                     │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │  PDF Processing │───▶│  Azure AI       │───▶│  Event Hub      │         │
│  │  (PyMuPDF)      │    │  (Phi-4)        │    │  (Streaming)    │         │
│  └─────────────────┘    └─────────────────┘    └────────┬────────┘         │
│                                                         │                   │
└─────────────────────────────────────────────────────────┼───────────────────┘
                                                          │
                                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             BATCH LAYER                                      │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │   RAW Layer     │───▶│  CURATE Layer   │───▶│  PUBLISH Layer  │         │
│  │                 │    │                 │    │                 │         │
│  │  - claims       │    │  - fact_claims  │    │  - dim_*        │         │
│  │  - customers    │    │  - watermarks   │    │  - fact_*       │         │
│  │  - products     │    │                 │    │  - settlements  │         │
│  │  - policies     │    │                 │    │                 │         │
│  └─────────────────┘    └─────────────────┘    └────────┬────────┘         │
│                                                         │                   │
└─────────────────────────────────────────────────────────┼───────────────────┘
                                                          │
              ┌───────────────────────────────────────────┼───────────────────┐
              │                                           │                   │
              ▼                                           ▼                   ▼
┌─────────────────────┐              ┌─────────────────────┐    ┌─────────────────┐
│   ML LAYER          │              │   SEMANTIC MODEL    │    │  OBSERVABILITY  │
│                     │              │                     │    │                 │
│  - Feature Store    │              │  - Star Schema      │    │  - Freshness    │
│  - MLflow Models    │              │  - Measures         │    │  - Schema Drift │
│  - Predictions      │              │  - Power BI         │    │  - Volume       │
└─────────────────────┘              └─────────────────────┘    └─────────────────┘
```

## Component Details

### 1. Speed Layer

#### PDF Processing Pipeline
1. **Blob Trigger**: New PDFs uploaded to Azure Storage
2. **Download**: PDF downloaded to Lakehouse
3. **Conversion**: PDF pages converted to PNG images
4. **AI Extraction**: Phi-4 multimodal model extracts handwritten text
5. **Enrichment**: Extracted data joined with policies/customers
6. **Streaming**: Events sent to Event Hub
7. **Cleanup**: Processed PDFs deleted, failed PDFs quarantined

#### Key Components
- `ProcessClaimForms` class - Main processing orchestrator
- Azure AI Inference SDK - Model interaction
- Azure Event Hub SDK - Event streaming
- PyMuPDF - PDF to image conversion

### 2. Batch Layer

#### Medallion Architecture

| Layer | Purpose | Tables |
|-------|---------|--------|
| **Raw** | Source data as-is | claims, customers, products, policies |
| **Curate** | Cleaned and transformed | fact_claims, pipeline_watermark |
| **Publish** | Business-ready | dim_*, fact_*, claim_settlements |

#### Incremental Processing
- Uses watermark pattern for change detection
- `pipeline_watermark` table tracks last processed timestamp
- Only new/changed records processed each run

#### Data Pipeline Flow
```
Start Datapipeline
       ↓
Ingest Speed → Raw
       ↓
Merge Claims
       ↓
Publish Incremental Load
       ↓
ML Predictions
       ↓
Semantic Model Prep
       ↓
End Datapipeline
```

### 3. Machine Learning Layer

#### Feature Engineering
50+ features across categories:
- **Temporal**: holidays, weekends, time of day
- **Historical**: previous claims, claim frequency
- **Financial**: claim-to-value ratios, premium relationships
- **Policy**: tenure, days to expiry
- **Customer**: age, payment methods
- **Geographic**: zip code frequencies

#### Models
| Model | Use Case | Algorithm |
|-------|----------|-----------|
| Vehicle Fraud | Vehicle claims | Random Forest |
| Property Fraud | Property claims | Logistic Regression |
| Furniture Fraud | Furniture claims | Logistic Regression |
| Mobile Fraud | Electronics claims | Logistic Regression |
| Anomaly Detection | All claims | Isolation Forest |

#### MLflow Integration
- Experiment tracking
- Model versioning
- Model registry
- Hyperparameter logging

### 4. Data Observability

#### Monitoring Dimensions

| Dimension | Description | Threshold |
|-----------|-------------|-----------|
| **Freshness** | Time since last update | 7 days (streaming), 180 days (batch) |
| **Schema** | Column changes detected | Any change flagged |
| **Volume** | Record count changes | >30% drop critical |

#### Alerting
- Application Insights integration
- Custom dimensions for filtering
- Dashboard for visualization

## Security Architecture

### Credential Management
```
┌─────────────────┐
│  Environment    │
│  Variables      │◀─── Development/Local
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Azure Key      │
│  Vault          │◀─── Production
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Fabric         │
│  Environment    │◀─── Fabric Native
└─────────────────┘
```

### Network Security
- VNet integration for storage accounts
- Private endpoints for Event Hub
- Managed identity where possible

## Scalability Considerations

### Spark Optimization
- Partition columns for large tables
- Delta Lake for ACID transactions
- Z-order optimization for query patterns

### Event Hub Scaling
- Partition count based on throughput
- Capture for cold storage
- Consumer groups for parallel processing

## Disaster Recovery

### Data Protection
- Delta Lake time travel (30 days)
- Geo-redundant storage
- Cross-region replication for critical data

### Recovery Procedures
1. Point-in-time restore for Delta tables
2. Event Hub replay from capture
3. Pipeline re-execution from watermark

