from google.cloud import storage
import pandas as pd 
import io

bucket_name = 'test-bucket-1173'
client = storage.Client()
bucket = client.bucket(bucket_name)
# blobs = bucket.list_blobs()

blob = bucket.get_blob('schema_csvs/schema_csv.csv')
data = blob.download_as_string()
csv_data = pd.read_csv(io.BytesIO(data), header = None, names = ['field_name','data_type','is_nullable'])

print(csv_data)

# for i,j in data.iterrows():
#     print(j[0],type(j[1]),type(j[2]))
