from google.cloud import dataproc_v1
from google.cloud import storage
import re

project_id = "advance-rush-349804"
region = "us-central1"
cluster_name = "example-cluster"
gcs_bucket = 'test-bucket-1173'
spark_filename = "write_avro_to_bigquery.py"

# Create the job client.
job_client = dataproc_v1.JobControllerClient(
    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
)

# Create the job config.
job = {
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": "gs://{}/{}".format(gcs_bucket, spark_filename)},
}

operation = job_client.submit_job_as_operation(
    request={"project_id": project_id, "region": region, "job": job}
)
response = operation.result()

# Dataproc job output is saved to the Cloud Storage bucket
# allocated to the job. Use regex to obtain the bucket and blob info.
matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

output = (
    storage.Client()
    .get_bucket(matches.group(1))
    .blob(f"{matches.group(2)}.000000000")
    .download_as_string()
)

print(f"Job finished successfully: {output}\r\n")