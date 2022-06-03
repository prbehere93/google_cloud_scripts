from google.cloud import bigquery

project = 'advance-rush-349804'

client = bigquery.Client(project=project,location = 'US')

try:
    query_job = client.query(
    """
    SELECT *
    FROM `advance-rush-349804.test_dataset_42.test_table_42`
    LIMIT 1"""
    )
    
    results = query_job.result() #will wait for upto 100s for the query to complete
    
    for row in results:
        print(row) 

except Exception as e:
    print(e)