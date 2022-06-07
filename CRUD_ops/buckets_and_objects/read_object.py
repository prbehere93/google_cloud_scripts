from google.cloud import storage

"""Lists all the blobs in the bucket."""
bucket_name = "test-bucket-1173"

storage_client = storage.Client()

try:
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        print(blob.name)
except Exception as e:
    print(e)