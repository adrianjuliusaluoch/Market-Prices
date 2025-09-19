# Import Packages
from google.cloud import bigquery
from serpapi import GoogleSearch
import pandas as pd
import time
import os

# Initialize BigQuery client
client = bigquery.Client(project='project-adrian-aluoch')

# Define Parameters
params = {
  "engine": "google_trends_trending_now",
  "geo": "KE",
  "api_key": os.getenv("SERPAPI_KEY")
}

# Get Data
search = GoogleSearch(params)
results = search.get_dict()
trending_searches = results["trending_searches"]

# Extract Data
records = []
for t in trending_searches:
    # safely extract end_timestamp
    end_ts = t.get("end_timestamp")

    records.append({
        "query": t["query"],
        "start_date": pd.to_datetime(int(t["start_timestamp"]), unit="s"),
        "end_date": pd.to_datetime(int(end_ts), unit="s") if end_ts else pd.NaT,
        "active": t.get("active", False),
        "search_volume": t.get("search_volume"),
        "increase_percentage": t.get("increase_percentage"),
        "categories": ", ".join([c["name"] for c in t.get("categories", [])]),
        "trend_breakdown": ", ".join(t.get("trend_breakdown", [])),
        "google_trends_link": t.get("serpapi_google_trends_link"),
        "news_link": t.get("serpapi_news_link")
    })

# Assign Dataframe
bigdata = pd.DataFrame(records)

# Drop Variables
bigdata.drop(columns=['google_trends_link', 'news_link'], inplace=True)

# Define Table ID
table_id = 'project-adrian-aluoch.google_trends.google_trending_in_kenya'

# Export Data to BigQuery
job = client.load_table_from_dataframe(bigdata, table_id)
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Define SQL Query to Retrieve Open Weather Data from Google Cloud BigQuery
sql = (
    'SELECT *'
    'FROM `project-adrian-aluoch.google_trends.google_trending_in_kenya`'
      )
    
# Run SQL Query
data = client.query(sql).to_dataframe()

# Check Shape of data from BigQuery
print(f"Shape of dataset from BigQuery : {data.shape}")

# Delete Original Table
client.delete_table(table_id)
print(f"Table deleted successfully.")

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=['query', 'start_date', 'end_date', 'active', 
   'search_volume', 'increase_percentage', 
   'categories', 'trend_breakdown']
  ).sum()
    
# Remove Duplicate Records
data.drop_duplicates(subset=['query', 'start_date', 'end_date', 'active', 
   'search_volume', 'increase_percentage', 
   'categories', 'trend_breakdown']
  , inplace=True)

# Define the dataset ID and table ID
dataset_id = 'google_trends'
table_id = 'google_trending_in_kenya'
    
# Define the table schema for Google Trends dataset
schema = [
    bigquery.SchemaField("query", "STRING"),
    bigquery.SchemaField("start_date", "TIMESTAMP"),
    bigquery.SchemaField("end_date", "TIMESTAMP"),
    bigquery.SchemaField("active", "BOOL"),
    bigquery.SchemaField("search_volume", "INT64"),
    bigquery.SchemaField("increase_percentage", "INT64"),
    bigquery.SchemaField("categories", "STRING"),
    bigquery.SchemaField("trend_breakdown", "STRING")
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
table_id = 'project-adrian-aluoch.google_trends.google_trending_in_kenya'

# Load the data into the BigQuery table
job = client.load_table_from_dataframe(data, table_id)

# Wait for the job to complete
while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

# Return Data Info
print(f"Google Trending Now data of shape {data.shape} has been successfully retrieved, saved, and appended to the BigQuery table.")



