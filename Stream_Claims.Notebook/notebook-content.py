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

# # Stream Claims to Event Hub
# 
# This notebook simulates streaming insurance claims to Azure Event Hub.
# It reads claims data from the lakehouse and sends batches to Event Hub.
# 
# ## Use Cases
# - Demo/testing real-time claim processing
# - Load testing Event Hub and downstream systems
# - Simulating claim arrival patterns

# CELL ********************

import os
import asyncio
import json

from pyspark.sql.window import Window
import pyspark.sql.functions as F

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

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

# =============================================================================
# CONFIGURATION - Replace with your actual values
# =============================================================================

# Event Hub Configuration
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "EVENT_HUB_CONNECTION_STRING",
    "Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=<policy>;SharedAccessKey=<key>;EntityPath=<hub>"
)
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "<your-event-hub-name>")

# Fabric Configuration
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "<your-workspace-id>")
LAKEHOUSE_ID = os.getenv("RAW_LAKEHOUSE_ID", "<your-raw-lakehouse-id>")

# Construct OneLake path for products table
PRODUCTS_TABLE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/Tables/frauddetection/products"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## State Management
# 
# Track which rows have been streamed to ensure consistent replay.

# CELL ********************

# Initialize or read the streaming position tracker
# This file tracks which claims have already been streamed
STREAM_POSITION_FILE = "/lakehouse/default/Files/stream_read_row.txt"

def get_current_position():
    """Get the current streaming position."""
    try:
        with open(STREAM_POSITION_FILE, "r") as f:
            return int(f.readline().strip())
    except FileNotFoundError:
        return 1

def save_position(position):
    """Save the current streaming position."""
    with open(STREAM_POSITION_FILE, "w") as f:
        f.write(str(position))

# Initialize position
READ_ROW = get_current_position()
print(f"Starting from position: {READ_ROW}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Reading Functions

# CELL ********************

def read_samples(sample_size: int = 300):
    """
    Read a batch of claims to stream.
    
    Args:
        sample_size: Number of claims to read in this batch
        
    Returns:
        list: List of claim JSON strings
    """
    global READ_ROW
    
    # Read claims data (assumes claims_25k.json exists in Files)
    claims = spark.read.json("Files/claims_25k.json")
    
    # Read products for enrichment
    products = spark.read.format('delta').load(PRODUCTS_TABLE_PATH)
    
    # Join claims with products
    claims_products = claims.join(products, "product_id", "inner")
    
    # Add row index for tracking
    window = Window.orderBy("claim_no")
    claims_indexed = claims_products.withColumn("index", F.row_number().over(window))
    
    # Get the batch of claims
    claims_sample = claims_indexed.where(
        (claims_indexed.index >= READ_ROW) & 
        (claims_indexed.index < READ_ROW + sample_size)
    )
    
    # Convert to JSON list
    claims_list = claims_sample.toJSON().collect()
    
    # Update position
    READ_ROW += sample_size
    save_position(READ_ROW)
    
    print(f"Read {len(claims_list)} claims, next position: {READ_ROW}")
    return claims_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Event Hub Streaming

# CELL ********************

async def stream_claims(sample_size: int = 300):
    """
    Stream a batch of claims to Event Hub.
    
    Args:
        sample_size: Number of claims to stream
    """
    # Create Event Hub producer
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STRING, 
        eventhub_name=EVENT_HUB_NAME
    )

    # Read claims to stream
    claim_events = read_samples(sample_size)
    
    if not claim_events:
        print("No claims to stream")
        return

    async with producer:
        # Create and send batch
        batch = await producer.create_batch()
        
        for event in claim_events:
            batch.add(EventData(event))
        
        await producer.send_batch(batch)
        print(f"Successfully streamed {len(claim_events)} claims to Event Hub")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Streaming

# CELL ********************

# Stream a batch of claims
# Adjust sample_size as needed for your demo
await stream_claims(sample_size=300)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Utility: Reset Stream Position
# 
# Uncomment and run to reset streaming to the beginning.

# CELL ********************

# # Reset streaming position to start from the beginning
# save_position(1)
# print("Stream position reset to 1")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

