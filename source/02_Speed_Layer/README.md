# Speed Layer Notebooks

This folder contains notebooks for real-time/streaming data processing.

## Notebooks

| File | Purpose | Execution Order |
|------|---------|----------------|
| `05_Speed_Layer_Stream_Claims_To_EventHub.py` | Stream claims to Azure Event Hub | After Batch Processing 01 |
| `06_Speed_Layer_Process_PDF_Claims_With_AI.py` | Process PDF forms with Azure AI | Independent |

## Quick Start

### Option 1: Stream Existing Claims
1. Run `02_Speed_Layer_Stream_Claims_To_EventHub.py`
   - Reads claims from lakehouse
   - Streams to Event Hub

### Option 2: Process PDF Forms
1. Run `06_Speed_Layer_Process_PDF_Claims_With_AI.py`
   - Processes handwritten PDFs
   - Extracts data with AI
   - Streams to Event Hub

## How to Use

Each notebook is **copy-paste ready**:

1. Open the `.py` file
2. Copy ALL content (Ctrl+A, Ctrl+C)
3. In Fabric: Create notebook → View code → Paste
4. Attach lakehouse
5. Update configuration:
   - Event Hub connection string
   - Storage account credentials (for PDF processing)
   - AI service credentials (for PDF processing)
6. Run!

## Configuration Required

### For Streaming (02)
- `EVENT_HUB_CONNECTION_STRING` - Your Event Hub connection string
- `EVENT_HUB_NAME` - Your Event Hub name

### For PDF Processing (06)
- `STORAGE_ACCOUNT_NAME` - Azure Storage account name
- `STORAGE_ACCOUNT_KEY` - Storage account key
- `AI_SERVICE_ENDPOINT` - Azure AI Foundry endpoint
- `AI_SERVICE_KEY` - AI service API key
- `EVENT_HUB_CONNECTION_STRING` - Event Hub connection string

## Integration with Batch Processing

After streaming, merge into batch layer:
1. Run Speed Layer notebook (05 or 06 - streams to Event Hub)
2. Run `../Batch Processing/02_Merge_Speed_To_Raw_Claims.py`
   - Merges streaming data from speed layer into batch tables

## Data Flow

```
PDF Forms → Azure Storage
    ↓
06_Speed_Layer_Process_PDF_Claims_With_AI
    ↓
Azure Event Hub
    ↓
Batch Processing (02_Speed_To_Raw_Claims)
```

OR

```
Lakehouse Claims
    ↓
02_Speed_Layer_Stream_Claims_To_EventHub
    ↓
Azure Event Hub
    ↓
Batch Processing (02_Speed_To_Raw_Claims)
```

## Related Notebooks

- **Batch Processing:** See `../Batch Processing/` for merging streaming data
- **Data Observability:** See `../Data Observability/` for monitoring streaming

## Notes

- **Event Hub:** Claims are streamed to Event Hub for real-time processing
- **State Management:** Streaming position tracked in `/lakehouse/default/Files/stream_read_row.txt`
- **PDF Processing:** Requires Azure AI Foundry with Phi-4-multimodal-instruct model

