from google.cloud import bigquery

client = bigquery.Client() #initializing BQ client
# client = bigquery.Client(project='advance-rush-349804') #use this if you want to mention the project name

dataset_id = f'test_dataset_42'

#creating a dataset object
dataset = client.dataset(dataset_id)
dataset.location = "US"

try:
    dataset = client.create_dataset(dataset, timeout=30)
    print(f'Created dataset {dataset_id} in project {client.project}')
except Exception as e:
    print(e)

