# Import Packages
from google.cloud import bigquery
import pandas as pd
import numpy as np
import janitor
import requests
from io import StringIO
import urllib3


# Initialize BigQuery client
client = bigquery.Client(project='crypto-stocks-01')

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

base_url = "https://kamis.kilimo.go.ke/site/market{}?product=1&per_page=3000"

bigdata = []
offset = 0

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
    
    maize = market_prices[0]
    
    bigdata.append(maize)
    offset += 3000

# Combine all pages into one DataFrame
bigdata = pd.concat(bigdata, ignore_index=True)
print(f"Collected {len(bigdata)} rows in total")


# Define Table ID
table_id = 'crypto-stocks-01.storage.market_prices'

# Export Data to BigQuery
job = client.load_table_from_dataframe(bigdata, table_id)
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Define SQL Query to Retrieve Open Weather Data from Google Cloud BigQuery
sql = (
    'SELECT *'
    'FROM `crypto-stocks-01.storage.market_prices`'
           )
    
# Run SQL Query
data = client.query(sql).to_dataframe()

# Check Shape of data from BigQuery
print(f"Shape of dataset from BigQuery : {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=[
    'timestamp', 
    'name', 
    'symbol', 
    'price_usd', 
    'vol_24h', 
    'total_vol', 
    'chg_24h', 
    'chg_7d', 
    'market_cap']).sum()
    
# Remove Duplicate Records
data.drop_duplicates(subset=[
    'timestamp', 
    'name', 
    'symbol', 
    'price_usd', 
    'vol_24h', 
    'total_vol', 
    'chg_24h', 
    'chg_7d', 
    'market_cap'], inplace=True)

# Define the dataset ID and table ID
dataset_id = 'storage'
table_id = 'top_cryptocurrency'
    
# Define the table schema for new table
schema = [
        bigquery.SchemaField("timestamp", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("price_usd", "STRING"),
        bigquery.SchemaField("vol_24h", "STRING"),
        bigquery.SchemaField("total_vol", "STRING"),
        bigquery.SchemaField("chg_24h", "STRING"),
        bigquery.SchemaField("chg_7d", "STRING"),
        bigquery.SchemaField("market_cap", "STRING"),
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
table_id = 'crypto-stocks-01.storage.top_cryptocurrency'

# Load the data into the BigQuery table
job = client.load_table_from_dataframe(data, table_id)

# Wait for the job to complete
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Return Data Info
print(f"Data {data.shape} has been successfully retrieved, saved, and appended to the BigQuery table.")

# Exit 
print(f'Cryptocurrency Data Export to Google BigQuery Successful')



















