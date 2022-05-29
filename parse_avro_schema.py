
def get_avro_schema_from_file(bucket_name,file_name):
import os
from google.cloud import storage
import avro
from avro.datafile import DataFileReader

client = storage.Client()

bucket_name = bucket_name
file_name = file_name

bucket = client.lookup_bucket(bucket_name)
if bucket is None:
    raise ValueError('Could not find bucket %s' % bucket_name)

blob = [i for i in bucket.list_blobs() if i.name==file_name][0] #temporary, we can use a file_name to get the blob later

blob.download_to_filename(file_name, start=0, end=100000)
schema = avro.schema.parse(open("twitter.avsc", "rb").read())



field_names,types = ([i['name'] for i in schema.to_json()['fields']],
    [i['type'] for i in schema.to_json()['fields']])

os.remove(file_name)
    return (field_names,types)