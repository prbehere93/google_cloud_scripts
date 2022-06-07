from google.cloud import storage

"""Renaming an object/files name"""

bucket_name = "test-bucket-1173"
blob_name = "test.csv"
new_name = "test2.csv"

storage_client = storage.Client()

try:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print(f"Blob {blob.name} has been renamed to {new_blob.name}")

except Exception as e:
    print(e)