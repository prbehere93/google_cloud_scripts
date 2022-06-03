from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

dataset_id = 'test_dataset_42'

try:
    dataset = client.get_dataset(dataset_id)  # Make an API request.
    dataset.description = "Updated description of the dataset to check if we have Update permissions"
    dataset = client.update_dataset(dataset, ["description"])  # Make an API request.

    full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
    
    print("Updated dataset '{}' with description '{}'.".format(full_dataset_id, dataset.description))

except Exception as e:
    print(e)

