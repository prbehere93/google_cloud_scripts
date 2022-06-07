from google.cloud import storage
"""Listing all files in the bucket to check read access"""

bucket_name = 'test-bucket-1173'
client = storage.Client()

try:
    blobs = client.list_blobs(bucket_name, prefix = "")
    for i in blobs:
        print(i)
except Exception as e:
    print(e)
