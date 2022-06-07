from google.cloud import storage

"""Deletes a blob from the bucket."""
bucket_name = "test-bucket-1173"
blob_name = "test2.csv"

storage_client = storage.Client()
try:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print(f"Blob {blob_name} deleted.")
except Exception as e:
    print(e)