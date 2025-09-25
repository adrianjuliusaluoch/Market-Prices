# Import Packages
from google.cloud import bigquery
import pandas as pd
import numpy as np
import janitor
import requests
from io import StringIO
import urllib3
import os
import time

# Initialize BigQuery client
client = bigquery.Client(project='project-adrian-aluoch')

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define Commodities
commodities = [
    1, # Dry Maize
    249,  # Maize Flour
    265, # Wheat Flour
    4, # Rice
    73, # Meat Beef
    72, # Eggs
    153, # Milk
    154, # Kales
    61,  # Tomatoes
    158,  # Dry Onions
]

# Create New Empty DataFrame
bigdata = pd.DataFrame()

# Loop through commodities
for commodity in commodities:
    base_url = "https://kamis.kilimo.go.ke/site/market{}?product=" + str(commodity)+ "&per_page=3000"

    # Define Offset
    offset = 0

    # Run
    while True:
        try:
            # Handle first page (no offset in URL)
            url = base_url.format("" if offset == 0 else f"/{offset}")
            print(f"Fetching: {url}")
            
            response = requests.get(url, verify=False)
            market_prices = pd.read_html(StringIO(response.text))

        except Exception as e:
            print(f"Error fetching data: {e}")
            break
        
        market_prices = market_prices[0]
        
        bigdata = pd.concat([bigdata, market_prices], ignore_index=True)
        offset += 3000

print(f"Collected {len(bigdata)} rows in total")

# Clean Names
bigdata = bigdata.clean_names()

# Standardize Data Types
bigdata['date'] = pd.to_datetime(bigdata['date'])
bigdata['wholesale'] = pd.to_numeric(bigdata['wholesale'].str.extract(r'(\d+\.?\d*)')[0], errors='coerce')
bigdata['retail'] = pd.to_numeric(bigdata['retail'].str.extract(r'(\d+\.?\d*)')[0], errors='coerce')

# Drop Variables
bigdata.drop(columns=['grade', 'sex'], inplace=True)

# Define Table ID
table_id = 'project-adrian-aluoch.food_basket.market_prices'

# Export Data to BigQuery
job = client.load_table_from_dataframe(bigdata, table_id)
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Define SQL Query to Retrieve Open Weather Data from Google Cloud BigQuery
sql = (
    'SELECT *'
    'FROM `project-adrian-aluoch.food_basket.market_prices`'
      )
    
# Run SQL Query
data = client.query(sql).to_dataframe()

# Check Shape of data from BigQuery
print(f"Shape of dataset from BigQuery : {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=['commodity', 'classification', 'market', 'wholesale',
       'retail', 'supply_volume', 'county', 'date']).sum()
    
# Remove Duplicate Records
data.drop_duplicates(subset=['commodity', 'classification', 'market', 'wholesale',
       'retail', 'supply_volume', 'county', 'date'], inplace=True)

# Define the dataset ID and table ID
dataset_id = 'food_basket'
table_id = 'market_prices'
    
# Define the table schema for new table
schema = [
    bigquery.SchemaField("commodity", "STRING"),
    bigquery.SchemaField("classification", "STRING"),
    bigquery.SchemaField("market", "STRING"),
    bigquery.SchemaField("wholesale", "FLOAT"),
    bigquery.SchemaField("retail", "FLOAT"),
    bigquery.SchemaField("supply_volume", "FLOAT"),
    bigquery.SchemaField("county", "STRING"),
    bigquery.SchemaField("date", "DATE") 
]
    
# Define the table reference
table_ref = client.dataset(dataset_id).table(table_id)
    
# Create the table object
table = bigquery.Table(table_ref, schema=schema)

try:
    # Create the table in BigQuery
    table = client.create_table(table)
    print(f"Table {table.table_id} created successfully.")
except Exception as e:
    print(f"Table {table.table_id} failed")

# Define the BigQuery table ID
table_id = 'project-adrian-aluoch.food_basket.market_prices'

# Load the data into the BigQuery table
job = client.load_table_from_dataframe(data, table_id)

# Wait for the job to complete
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Return Data Info
print(f"Food Basket data of shape {data.shape} has been successfully retrieved, saved, and appended to the BigQuery table.")





