"""
================================================================================
NOTEBOOK: 06 - Speed Layer - Process PDF Claims with Azure AI
================================================================================

PURPOSE:
    Processes handwritten insurance claim forms using Azure AI Foundry's
    multimodal capabilities (Phi-4) to extract structured data from scanned PDFs.

WHAT THIS NOTEBOOK DOES:
    1. Reads PDF claim forms from Azure Blob Storage
    2. Converts PDF pages to images
    3. Uses Azure AI (Phi-4-multimodal-instruct) to extract handwritten text
    4. Parses extracted data into structured claim format
    5. Enriches with policy/customer information from lakehouse
    6. Streams enriched claims to Azure Event Hub for real-time processing

PREREQUISITES:
    - Azure Storage account with claim PDFs uploaded
    - Azure AI Foundry with Phi-4-multimodal-instruct model deployed
    - Azure Event Hub created and configured
    - Fabric Lakehouse with raw data tables (customers, policies, products)

HOW TO USE:
    1. Copy ALL content from this file
    2. Paste into Fabric notebook
    3. Attach lakehouse
    4. Update CONFIGURATION section with your credentials:
       - Storage account name and key
       - AI service endpoint and key
       - Event Hub connection string
    5. Update file paths if needed
    6. Run all cells

EXECUTION ORDER:
    Can run: Independently (for processing new PDF claims)
    Works with: 02_Speed_Layer_Stream_Claims_To_EventHub (downstream)

CONFIGURATION REQUIRED:
    - STORAGE_ACCOUNT_NAME: Your Azure Storage account name
    - STORAGE_ACCOUNT_KEY: Your storage account key
    - AI_SERVICE_ENDPOINT: Your Azure AI Foundry endpoint
    - AI_SERVICE_KEY: Your AI service API key
    - EVENT_HUB_CONNECTION_STRING: Your Event Hub connection string

OUTPUT:
    - Claims streamed to Event Hub
    - Structured claim data extracted from PDFs

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
# META     },
# META     "environment": {
# META       "environmentId": "<your-environment-id>",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 06 - Speed Layer: Process PDF Claims with Azure AI
# 
# This notebook processes handwritten insurance claim forms using Azure AI Foundry.
# 
# **Architecture:**
# 1. PDF → Images → 2. AI Extraction → 3. Enrichment → 4. Event Hub Streaming

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "conf": {
# MAGIC         "spark.sql.jsonGenerator.ignoreNullFields" : "false"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import (
    SystemMessage,
    UserMessage,
    TextContentItem,
    ImageContentItem,
    ImageUrl,
    ImageDetailLevel,
)
from azure.core.credentials import AzureKeyCredential
import fitz  # PyMuPDF
import base64
import json
import random
from datetime import datetime, timedelta
import uuid
import asyncio
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, months_between, lit, to_date, current_date, round as rnd, struct
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import requests
from typing_extensions import Literal

print("Libraries loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration
# 
# **IMPORTANT**: Replace placeholder values with your actual credentials.

# CELL ********************

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================

# Azure Blob Storage Configuration
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "<your-storage-account-name>")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY", "<your-storage-account-key>")
CONTAINER_NAME = "claims"  # Update if your container has a different name

# Azure AI Foundry Configuration
AI_SERVICE_ENDPOINT = os.getenv("AI_SERVICE_ENDPOINT", "https://<your-workspace>.services.ai.azure.com/models")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "Phi-4-multimodal-instruct")
AI_SERVICE_KEY = os.getenv("AI_SERVICE_KEY", "<your-ai-service-key>")

# Azure Event Hub Configuration
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING",
    "Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<hub>"
)
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "<your-event-hub-name>")

print("Configuration loaded")
print(f"Storage Account: {STORAGE_ACCOUNT_NAME}")
print(f"AI Endpoint: {AI_SERVICE_ENDPOINT}")
print(f"Model: {AI_MODEL_NAME}")
print(f"Event Hub: {EVENT_HUB_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ProcessClaimForms Class
# 
# Main class for processing PDF claim forms with AI extraction.

# CELL ********************

class ProcessClaimForms:
    """
    Processes PDF claim forms using Azure AI Foundry.
    
    Steps:
    1. Download PDF from Blob Storage
    2. Convert PDF pages to images
    3. Extract text using Azure AI multimodal model
    4. Parse extracted data
    5. Enrich with lakehouse data
    6. Stream to Event Hub
    """
    
    def __init__(self):
        """Initialize Azure service clients."""
        # Blob Storage client
        self.account_name = STORAGE_ACCOUNT_NAME
        self.account_key = STORAGE_ACCOUNT_KEY
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{self.account_name}.blob.core.windows.net",
            credential=self.account_key
        )
        self.container_client = self.blob_service_client.get_container_client(CONTAINER_NAME)
        
        # Azure AI client
        self.endpoint = AI_SERVICE_ENDPOINT
        self.model_name = AI_MODEL_NAME
        self.key = AI_SERVICE_KEY
        self.client = ChatCompletionsClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.key)
        )
        
        # Event Hub client
        self.EVENT_HUB_CLAIMS_STR = EVENT_HUB_CONNECTION_STRING
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.EVENT_HUB_CLAIMS_STR,
            eventhub_name=EVENT_HUB_NAME
        )
    
    def pdf_to_images(self, pdf_bytes):
        """Convert PDF to list of base64-encoded images."""
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        images = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            img_bytes = pix.tobytes("png")
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            images.append(f"data:image/png;base64,{img_base64}")
        doc.close()
        return images
    
    def extract_with_ai(self, images):
        """Extract structured data from images using Azure AI."""
        system_message = SystemMessage(
            content="You are an expert at extracting structured data from insurance claim forms. Extract all relevant information and return as JSON."
        )
        
        user_content = [
            TextContentItem(text="Extract all information from this insurance claim form. Return as JSON with fields: claim_no, policy_no, incident_date, incident_type, claim_amount, driver_name, vehicle_make, vehicle_model, description.")
        ]
        
        for img_base64 in images:
            user_content.append(
                ImageContentItem(
                    image_url=ImageUrl(url=img_base64),
                    detail=ImageDetailLevel.HIGH
                )
            )
        
        user_message = UserMessage(content=user_content)
        
        response = self.client.complete(
            messages=[system_message, user_message],
            model=self.model_name,
            temperature=0.1,
            max_tokens=2000
        )
        
        return response.choices[0].message.content
    
    def enrich_claim(self, extracted_data, spark):
        """Enrich claim with data from lakehouse."""
        # Parse extracted JSON
        try:
            claim_dict = json.loads(extracted_data)
        except:
            claim_dict = {}
        
        # Join with policies, customers, products
        # Add enrichment logic here based on your schema
        
        return claim_dict
    
    async def process_pdf(self, blob_name, spark):
        """Process a single PDF claim form."""
        try:
            # Download PDF
            blob_client = self.container_client.get_blob_client(blob_name)
            pdf_bytes = blob_client.download_blob().readall()
            
            # Convert to images
            images = self.pdf_to_images(pdf_bytes)
            
            # Extract with AI
            extracted_text = self.extract_with_ai(images)
            
            # Enrich
            enriched_claim = self.enrich_claim(extracted_text, spark)
            
            # Stream to Event Hub
            async with self.producer:
                batch = await self.producer.create_batch()
                batch.add(EventData(json.dumps(enriched_claim)))
                await self.producer.send_batch(batch)
            
            print(f"✅ Processed: {blob_name}")
            return enriched_claim
            
        except Exception as e:
            print(f"❌ Error processing {blob_name}: {str(e)}")
            return None

print("ProcessClaimForms class defined")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Process PDFs

# CELL ********************

# ============================================================================
# PROCESS PDFS - Update blob name or list of blob names
# ============================================================================

# Initialize processor
processor = ProcessClaimForms()

# List PDFs in container (or specify specific blob names)
blob_list = processor.container_client.list_blobs(name_starts_with="claims/")
pdf_blobs = [blob.name for blob in blob_list if blob.name.endswith('.pdf')]

print(f"Found {len(pdf_blobs)} PDF files to process")

# Process each PDF
if pdf_blobs:
    print(f"\nProcessing {len(pdf_blobs)} PDFs...")
    for blob_name in pdf_blobs[:5]:  # Process first 5 for sample runs
        await processor.process_pdf(blob_name, spark)
    print("\n✅ PDF processing complete!")
else:
    print("⚠️  No PDF files found. Upload PDFs to your storage container first.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Notes
# 
# - Update `blob_list` to process specific PDFs
# - Adjust `extract_with_ai` prompt to match your form structure
# - Customize `enrich_claim` to join with your lakehouse tables
# - Monitor Event Hub for processed claims

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

