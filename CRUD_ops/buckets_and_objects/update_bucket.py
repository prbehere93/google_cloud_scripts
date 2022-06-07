import pprint
from google.cloud import storage

"""
Updating metadata (labels) of the storage bucket to see if we have update permissions
"""

bucket_name = "some-testing-bucket-42"

storage_client = storage.Client()

try:
    bucket = storage_client.get_bucket(bucket_name)
    labels = bucket.labels
    labels["example"] = "adding"
    bucket.labels = labels
    bucket.patch()

    print(f"Updated labels on {bucket.name}.")
    pprint.pprint(bucket.labels)

except Exception as e:
    print(e)