from google.cloud import storage
from pathlib import Path
from datetime import datetime, timezone

def run():
    client = storage.Client()
    bucket_name = 'test-bucket-1173'
    # prefix = "random-stuff/" #not using this currently 
    # bucket = client.bucket(bucket_name)

    blobs = client.list_blobs(bucket_name, prefix = "")

    current_time = datetime.now(tz=timezone.utc) #this is required because the timezone of the object created in the bucket is UTC

    try:
        for blob in blobs:
            blob_created = blob.time_created
            d = current_time - blob_created
            if d.days >= 7:
                blob.delete()

    except Exception as e:
        print(e)

if __name__ == '__main__':
    run()