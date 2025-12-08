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

# # Handwritten Claims Processing with Azure AI Foundry
# 
# This notebook processes handwritten insurance claim forms using Azure AI's 
# multimodal capabilities (Phi-4) to extract structured data from scanned PDFs.
# 
# ## Architecture
# 1. PDF forms uploaded to Azure Blob Storage
# 2. Forms converted to images
# 3. Azure AI extracts handwritten text
# 4. Data enriched with policy/customer information
# 5. Claims streamed to Event Hub for real-time processing
# 
# ## Prerequisites
# - Azure Storage account with claim PDFs
# - Azure AI Foundry with Phi-4-multimodal-instruct deployed
# - Azure Event Hub for streaming
# - Fabric Lakehouse with raw data tables

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration
# 
# **IMPORTANT**: Replace placeholder values with your actual credentials.
# In production, use Azure Key Vault or Fabric environment variables.

# CELL ********************

# =============================================================================
# CONFIGURATION - Replace with your actual values
# =============================================================================

# Azure Storage Configuration
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "<your-storage-account-name>")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY", "<your-storage-account-key>")
STORAGE_CONTAINER_CLAIMS = os.getenv("STORAGE_CONTAINER_CLAIMS", "claim-pdfs")
STORAGE_CONTAINER_FAILED = os.getenv("STORAGE_CONTAINER_FAILED", "claim-pdfs-failed")

# Azure AI Foundry Configuration
AI_ENDPOINT = os.getenv("AI_ENDPOINT", "https://<your-ai-service>.services.ai.azure.com/models")
AI_MODEL_NAME = os.getenv("AI_MODEL_NAME", "Phi-4-multimodal-instruct")
AI_API_KEY = os.getenv("AI_API_KEY", "<your-ai-api-key>")

# Event Hub Configuration
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING",
    "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<hub>"
)
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "<your-event-hub-name>")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## AI Prompt Configuration

# CELL ********************

# System prompt for the AI model to extract handwritten form data
SYSTEM_PROMPT = """
You are an intelligent document processing assistant specialized in extracting handwritten information from scanned or photographed insurance claim forms.

You will be provided with an image of a handwritten insurance claims form. Your task is to extract the following fields exactly as written in the image and return them in the JSON format shown below.

Fields to extract:

Customer Details
- First Name  
- Last Name  
- Phone Number  
- Email Address  
- Address  

Claim Details
- Number of Witnesses  
- Incident Date  
- Incident Hour  
- Incident Type  
- Incident Zip  
- Number of Vehicles Involved  
- Type of Collision
- Insured Relationship
- Driver's License Issue Date
- Product Type
- Policy No

Claim Amounts
- Vehicle
- Property
- Injury

Output Format (JSON):
{
  "Customer_details": {
    "First_name": "",
    "Last_name": "",
    "Phone_number": "",
    "Email_address": "",
    "Address": ""
  },
  "Claim_details": {
    "Number_of_witnesses": "",
    "Incident_date": "",
    "Incident_hour": "",
    "Incident_type": "",
    "Incident_zip": "",
    "Number_of_vehicles_involved": "",
    "Type_of_collision": "",
    "Insured_relationship": "",
    "Drivers_license_issue_date": "",
    "Product_type": "",
    "Policy_no": ""
  },
  "Claim_amounts": {
    "Vehicle": "",
    "Property": "",
    "Injury": ""
  }
}

If any field is illegible or missing in the image, return an empty string "" for that field. Do not infer or guess any values.
"""

USER_PROMPT = "Please extract the required fields from this handwritten insurance claim form image."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ProcessClaimForms Class

# CELL ********************

class ProcessClaimForms:
    """
    Processes handwritten insurance claim forms using Azure AI.
    
    Flow:
    1. Retrieve PDF forms from Azure Blob Storage
    2. Convert PDFs to images
    3. Use Azure AI to extract handwritten text
    4. Enrich data with policy/customer information from Lakehouse
    5. Stream processed claims to Event Hub
    """
    
    def __init__(self, demo_form_policy=None):
        """
        Initialize the claim form processor.
        
        Args:
            demo_form_policy: Optional policy number for demo/testing
        """
        self.demo_form_policy = demo_form_policy
        
        # Storage configuration
        self.account_name = STORAGE_ACCOUNT_NAME
        self.account_key = STORAGE_ACCOUNT_KEY
        self.container_name = STORAGE_CONTAINER_CLAIMS
        self.fail_container_name = STORAGE_CONTAINER_FAILED

        # AI client configuration
        self.endpoint = AI_ENDPOINT
        self.model_name = AI_MODEL_NAME
        self.key = AI_API_KEY
        self.client = ChatCompletionsClient(
            endpoint=self.endpoint, 
            credential=AzureKeyCredential(self.key)
        )

        # Event Hub configuration
        self.event_hub_connection_string = EVENT_HUB_CONNECTION_STRING
        self.event_hub_name = EVENT_HUB_NAME

    def _get_storage_connection_string(self, account_name=None, account_key=None):
        """Generate Azure Storage connection string."""
        account_name = account_name or self.account_name
        account_key = account_key or self.account_key
        return f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    def get_blob_files(self, account_name=None, account_key=None, container_name=None):
        """
        Get list of PDF files from Azure Blob Storage.
        
        Returns:
            list: List of blob file names
        """
        account_name = account_name or self.account_name
        account_key = account_key or self.account_key
        container_name = container_name or self.container_name
        
        connect_str = self._get_storage_connection_string(account_name, account_key)
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(container_name)

        blob_list = []
        for blob_i in container_client.list_blobs():
            blob_list.append(blob_i.name)
        return blob_list

    def move_form_to_lakehouse(self, pdf):
        """
        Download PDF from blob storage and save to Lakehouse.
        
        Args:
            pdf: Name of the PDF file
            
        Returns:
            str: Local path to the saved PDF
        """
        # Generate SAS token for secure access
        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            blob_name=pdf,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True, delete=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )

        sas_url = f"https://{self.account_name}.blob.core.windows.net/{self.container_name}/{pdf}?{sas_token}"
        resp = requests.get(sas_url)

        # Save to lakehouse
        output_dir = "/lakehouse/default/Files/claim_pdf_forms"
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, pdf)
        with open(file_path, "wb") as f:
            f.write(resp.content)

        return file_path

    def convert_pdf_to_png(self, pdf_path):
        """
        Convert PDF pages to PNG images for AI processing.
        
        Args:
            pdf_path: Path to the PDF file
        """
        with fitz.open(pdf_path) as doc:
            for index, page in enumerate(doc, start=1):
                pix = page.get_pixmap()
                out_image_filename = f"page_{index}.png"
                output_dir = "/lakehouse/default/Files/pdf_images"
                os.makedirs(output_dir, exist_ok=True)
                out_image_path = os.path.join(output_dir, out_image_filename)
                pix.save(out_image_path)
    
    def read_form_images(self):
        """
        Use Azure AI to extract text from form images.
        
        Returns:
            str: JSON string with extracted form data
        """
        image_pg_1 = "/lakehouse/default/Files/pdf_images/page_1.png"
        image_pg_2 = "/lakehouse/default/Files/pdf_images/page_2.png"
        
        response = self.client.complete(
            model=self.model_name,
            messages=[
                SystemMessage(SYSTEM_PROMPT),
                UserMessage([
                    TextContentItem(text=USER_PROMPT),
                    ImageContentItem(
                        image_url=ImageUrl.load(
                            image_file=image_pg_1,
                            image_format="png",
                            detail=ImageDetailLevel.HIGH,
                        ),
                    ),
                    ImageContentItem(
                        image_url=ImageUrl.load(
                            image_file=image_pg_2,
                            image_format="png",
                            detail=ImageDetailLevel.HIGH,
                        ),
                    ),
                ]),
            ],
        )

        return response["choices"][0]["message"]["content"]

    def update_dict_for_df(self, resp_dict):
        """
        Add additional fields and cast data types for DataFrame conversion.
        
        Args:
            resp_dict: Dictionary with extracted form data
            
        Returns:
            dict: Enhanced dictionary with additional fields
        """
        # Add generated fields
        resp_dict["Claim_details"]["latitude"] = random.uniform(-180, 180)
        resp_dict["Claim_details"]["longitude"] = random.uniform(-180, 180)
        resp_dict["Claim_details"]["severity"] = random.choice(["Low", "Medium", "High"])
        resp_dict["Claim_details"]["claim_datetime"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        resp_dict["Claim_details"]["record_timestamp"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + ".000Z"
        resp_dict["Claim_details"]["index"] = 1

        # Generate unique claim number
        segments = [
            uuid.uuid4().hex[:8], 
            uuid.uuid4().hex[:4], 
            uuid.uuid4().hex[:4], 
            uuid.uuid4().hex[:4], 
            uuid.uuid4().hex[:12]
        ]
        resp_dict["Claim_details"]["claim_no"] = "-".join(segments)

        # Cast numeric fields
        claim_details = resp_dict["Claim_details"]
        claim_amounts = resp_dict["Claim_amounts"]
        
        claim_details["Incident_hour"] = int(claim_details["Incident_hour"]) if claim_details["Incident_hour"] else None
        claim_details["Number_of_vehicles_involved"] = int(claim_details["Number_of_vehicles_involved"]) if claim_details["Number_of_vehicles_involved"] else None
        claim_details["Number_of_witnesses"] = int(claim_details["Number_of_witnesses"]) if claim_details["Number_of_witnesses"] else None
        
        claim_amounts["Injury"] = round(float(claim_amounts["Injury"]), 1) if claim_amounts["Injury"] else None
        claim_amounts["Vehicle"] = round(float(claim_amounts["Vehicle"]), 1) if claim_amounts["Vehicle"] else None
        claim_amounts["Property"] = round(float(claim_amounts["Property"]), 1) if claim_amounts["Property"] else None

        # Calculate total claim amount
        values = [v for v in claim_amounts.values() if v is not None]
        claim_amounts["Total"] = sum(values)

        return resp_dict

    def convert_resp_dict_to_df(self, resp_dict):
        """
        Convert extracted data dictionary to Spark DataFrame.
        
        Args:
            resp_dict: Dictionary with form data
            
        Returns:
            DataFrame: Spark DataFrame with form data
        """
        cust_dets_schema = StructType([
            StructField("First_name", StringType(), True),
            StructField("Last_name", StringType(), True),
            StructField("Phone_number", StringType(), True),
            StructField("Email_address", StringType(), True),
            StructField("Address", StringType(), True)
        ])

        claim_dets_schema = StructType([
            StructField("Number_of_witnesses", IntegerType(), True),
            StructField("Incident_date", StringType(), True),
            StructField("Incident_hour", IntegerType(), True),
            StructField("Incident_type", StringType(), True),
            StructField("Incident_zip", StringType(), True),
            StructField("Number_of_vehicles_involved", IntegerType(), True),
            StructField("Type_of_collision", StringType(), True),
            StructField("Insured_relationship", StringType(), True),
            StructField("Drivers_license_issue_date", StringType(), True),
            StructField("Product_type", StringType(), True),
            StructField("Policy_no", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("severity", StringType(), True),
            StructField("claim_datetime", StringType(), True),
            StructField("record_timestamp", StringType(), True),
            StructField("index", StringType(), True),
            StructField("claim_no", StringType(), True)
        ])

        claim_amts_schema = StructType([
            StructField("Vehicle", FloatType(), True),
            StructField("Property", FloatType(), True),
            StructField("Injury", FloatType(), True),
            StructField("Total", FloatType(), True)
        ])

        main_schema = StructType([
            StructField("Customer_details", cust_dets_schema, True),
            StructField("Claim_details", claim_dets_schema, True),
            StructField("Claim_amounts", claim_amts_schema, True)
        ])

        input_data = [resp_dict]
        form_df = spark.createDataFrame(input_data, schema=main_schema)

        return form_df
        
    def create_events_from_form_df(self, form_df, resp_dict):
        """
        Create Event Hub events by enriching form data with Lakehouse data.
        
        Args:
            form_df: DataFrame with form data
            resp_dict: Original response dictionary
            
        Returns:
            list: List of JSON event strings
        """
        # Get customer policy
        policy_no = resp_dict['Claim_details']['Policy_no']
        policies = spark.sql(f"SELECT * FROM frauddetection.policies WHERE policy_no = '{policy_no}'").drop("record_timestamp")
    
        # Check if matching policy exists
        if policies.count() == 0:
            return []  # No matching policy found

        # Get associated product data
        policy_prod_ids = [row.product_id for row in policies.select("product_id").collect()]
        in_string = "('" + "', '".join(policy_prod_ids) + "')"
        products = spark.sql(f"SELECT * FROM frauddetection.products WHERE product_id IN {in_string}").drop("record_timestamp")

        # Get associated customer data
        cust_id = policies.select("cust_id").limit(1).collect()[0].cust_id
        customer = spark.sql(f"SELECT * FROM frauddetection.customers WHERE customer_id = '{cust_id}'").drop("record_timestamp")

        # Combine data
        full_df = form_df.join(policies, (form_df.Claim_details.Policy_no == policies.policy_no), "inner")
        full_df = full_df.join(products, "product_id", "inner")
        full_df = full_df.join(customer, (full_df.cust_id == customer.customer_id), "inner")

        # Create driver age
        full_df = full_df.withColumn(
            "driver_age",
            rnd(months_between(current_date(), to_date(full_df.date_of_birth, "yyyy-MM-dd"), True) / 12, 0).cast("int")
        )

        # Create months_as_customer column
        full_df = full_df.withColumn(
            "months_as_customer",
            rnd(months_between(current_date(), to_date(full_df.customer_since, "yyyy-MM-dd"), True), 0).cast("int")
        )

        # Create nested event columns
        full_df = full_df.withColumn(
            "collision",
            struct(
                full_df.Claim_details.Number_of_vehicles_involved.alias("number_of_vehicles_involved"),
                full_df.Claim_details.Type_of_collision.alias("type")
            )
        )

        full_df = full_df.withColumn(
            "driver",
            struct(
                full_df.driver_age.alias("age"),
                full_df.Claim_details.Insured_relationship.alias("insured_relationship"),
                col("Claim_details.Drivers_license_issue_date").alias("license_issue_date")
            )
        )

        full_df = full_df.withColumn(
            "incident",
            struct(
                full_df.Claim_details.Incident_date.alias("date"),
                full_df.Claim_details.Incident_hour.alias("hour"),
                full_df.Claim_details.latitude.alias("latitude"),
                full_df.Claim_details.longitude.alias("longitude"),
                full_df.Claim_details.severity.alias("severity"),
                full_df.Claim_details.Incident_type.alias("type"),
                full_df.Claim_details.Incident_zip.alias("zip_code")
            )
        )

        full_df = full_df.withColumn(
            "claim_amount",
            struct(
                full_df.Claim_amounts.Injury.alias("injury"),
                full_df.Claim_amounts.Property.alias("property"),
                full_df.Claim_amounts.Total.alias("total"),
                full_df.Claim_amounts.Vehicle.alias("vehicle")
            )
        )

        final_df = full_df.select(
            "product_id",
            "claim_amount",
            col("Claim_details.claim_datetime").alias("claim_datetime"),
            col("Claim_details.claim_no").alias("claim_no"),
            "collision",
            "driver",
            "incident",
            "months_as_customer",
            col("Claim_details.number_of_witnesses").alias("number_of_witnesses"),
            "policy_no",
            col("Claim_details.record_timestamp").alias("record_timestamp"),
            (lit(None).cast("boolean")).alias("suspicious_activity"),
            "product_type",
            "product_subtype",
            "make",
            "model",
            "model_year",
            "serial_no",
            "product_value",
            col("Claim_details.index").alias("index")
        )

        event_list = final_df.toJSON().collect()
        return event_list

    async def send_to_event_hub(self, claim_events):
        """
        Send claim events to Azure Event Hub.
        
        Args:
            claim_events: List of claim event JSON strings
        """
        producer = EventHubProducerClient.from_connection_string(
            conn_str=self.event_hub_connection_string, 
            eventhub_name=self.event_hub_name
        )

        async with producer:
            batch = await producer.create_batch()
            for event in claim_events:
                batch.add(EventData(event))
            await producer.send_batch(batch)

    def del_blob_files(self, blob_name, account_name=None, account_key=None, container_name=None):
        """Delete a blob file from storage."""
        account_name = account_name or self.account_name
        account_key = account_key or self.account_key
        container_name = container_name or self.container_name
        
        connect_str = self._get_storage_connection_string(account_name, account_key)
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client(container_name)
        container_client.delete_blob(blob_name)
    
    def quarantine_failed_form(self, blob_name, account_name=None, account_key=None, 
                               src_container_name=None, tgt_container_name=None):
        """
        Move failed form to quarantine container for manual review.
        
        Args:
            blob_name: Name of the blob to quarantine
        """
        account_name = account_name or self.account_name
        account_key = account_key or self.account_key
        src_container_name = src_container_name or self.container_name
        tgt_container_name = tgt_container_name or self.fail_container_name

        # Generate SAS token
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=src_container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True, delete=True, write=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )

        connect_str = self._get_storage_connection_string(account_name, account_key)
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Copy to quarantine container
        src_cont_client = blob_service_client.get_container_client(src_container_name)
        src_blob_client = src_cont_client.get_blob_client(blob_name)
        src_blob_url = src_blob_client.url + '?' + sas_token

        tgt_blob_client = blob_service_client.get_blob_client(container=tgt_container_name, blob=blob_name)
        tgt_blob_client.upload_blob_from_url(source_url=src_blob_url, overwrite=True)

        # Delete from source
        src_blob_client.delete_blob()

    def process_claim_forms(self):
        """
        Main processing loop for claim forms.
        
        Processes all PDFs in the blob container:
        1. Downloads and converts to images
        2. Extracts data using AI
        3. Enriches with Lakehouse data
        4. Sends to Event Hub
        5. Handles failures by quarantining
        """
        blob_list = self.get_blob_files()
        print(f"Found {len(blob_list)} form(s) to process: {blob_list}")
        
        for f in blob_list:
            try:
                print(f"\nProcessing form: {f}")
                
                # Clean up previous form data
                if notebookutils.fs.exists('file:/lakehouse/default/Files/claim_pdf_forms'):
                    notebookutils.fs.rm('file:/lakehouse/default/Files/claim_pdf_forms', True)

                # Download and save PDF
                pdf_path = self.move_form_to_lakehouse(f)
                print("Form downloaded to lakehouse")

                # Clean image folder
                if notebookutils.fs.exists('file:/lakehouse/default/Files/pdf_images'):
                    notebookutils.fs.rm('file:/lakehouse/default/Files/pdf_images', True)

                # Convert PDF to images
                self.convert_pdf_to_png(pdf_path)
                print("PDF converted to images")
                
                # Extract data using AI
                model_response = self.read_form_images()
                print("AI model response received")
                
                resp_dict = json.loads(model_response)
                resp_dict = self.update_dict_for_df(resp_dict)
                print(f"Extracted data: {resp_dict}")

                # Convert to DataFrame
                form_df = self.convert_resp_dict_to_df(resp_dict)
                print(f"DataFrame created with {form_df.count()} row(s)")

                # Create and send events
                events = self.create_events_from_form_df(form_df, resp_dict)
                print(f"Created {len(events)} event(s)")
                
                if len(events) > 0:
                    asyncio.run(self.send_to_event_hub(events))
                    print("Events sent to Event Hub")

                    # Clean up processed form
                    self.del_blob_files(f)
                    print(f"Form deleted from blob: {f}")
                else:
                    print("No events created - likely invalid policy number. Quarantining form.")
                    self.quarantine_failed_form(f)
                    
            except Exception as e:
                print(f"Error processing form {f}: {e}")
                self.quarantine_failed_form(f)
                print(f"Form quarantined: {f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Processing

# CELL ********************

# Process all claim forms in the blob container
processor = ProcessClaimForms()
processor.process_claim_forms()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

