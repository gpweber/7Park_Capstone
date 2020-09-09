#Set path to credentials
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=".credentials/7Park-MGM-Studios-bff1a6ac9ced.json"

#Import bigquery
from google.cloud import bigquery

#Start bigquery client
client = bigquery.Client()

#Setup query, high-empire-220313 is our project name
query_job = client.query(
    """
    SELECT *
    FROM `high-empire-220313.test_construction.test`
    LIMIT 10"""
)

#Get results from a query
results = query_job.result()  # Waits for job to complete.

#Prints results
for row in results:
    print(f"{row.period_id}")