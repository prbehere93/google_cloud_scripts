from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()
# client = bigquery.Client(project='advance-rush-349804') #use this if you want to mention the project name

project_name = f'{client.project}'
dataset_name = 'test_dataset_42'
table_name = 'test_table_42'

table_id = f"{project_name}.{dataset_name}.{table_name}"

schema = [
    bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="REQUIRED"),
]

try:
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
except Exception as e:
    print(e)