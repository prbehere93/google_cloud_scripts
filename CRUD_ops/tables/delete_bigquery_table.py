from google.cloud import bigquery


# Construct a BigQuery client object.
client = bigquery.Client()

table_id = 'advance-rush-349804.test_dataset_42.test_table_42'

try:
    client.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))
except Exception as e:
    print(e)