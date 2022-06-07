from google.cloud import storage


bucket_name = "some-testing-bucket-42"

storage_client = storage.Client()

try:
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()

    print(f"Bucket {bucket.name} deleted")

except Exception as e:
    print(e)