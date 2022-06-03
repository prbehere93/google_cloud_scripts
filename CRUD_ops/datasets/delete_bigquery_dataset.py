from google.cloud import bigquery

client = bigquery.Client(project='advance-rush-349804')
# client = bigquery.Client(project='advance-rush-349804') #use this if you want to mention the project name

dataset_id = 'test_dataset_28'

try:
    client.delete_dataset(
        dataset_id, delete_contents=True, not_found_ok=True
    ) 

    print("Deleted dataset '{}'.".format(dataset_id))

except Exception as e:
    print(e)