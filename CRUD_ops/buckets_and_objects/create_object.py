from google.cloud import storage
import os
import pandas as pd

df = pd.DataFrame(data=[{1,2,3},{4,5,6}],columns=['a','b','c'])
bucket_name = 'test-bucket-1173'
client = storage.Client()

try:
    bucket = client.get_bucket(bucket_name)    
    bucket.blob('test.csv').upload_from_string(df.to_csv(), 'text/csv')
    print('object created successfully')
except Exception as e:
    print(e)