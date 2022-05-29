from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "advance-rush-349804.test_dataset_28.test-table-4"

#using the use_avro_logical_types arg will ensure that the logical types are chosen while loading data into the bq tables
job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO,use_avro_logical_types = True)
uri = "gs://test-bucket-1173/files/twitter2.avro"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))
