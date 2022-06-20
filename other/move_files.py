from google.cloud import storage

bucket_name = 'test-bucket-1173'


#os.environ['GOOGLE_APPLICATION_CREDENTIALS']="demo1-348613-061205ebe47e.json"

#storage_client = storage.Client.from_service_account_json('demo1-348613-0eeb0919a5d7.json')



def move_blob():
    
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    
    blob_list = list(storage_client.list_blobs(bucket_name, prefix='')) #folder and the prefix of file 
    blob_files_list = [i for i in blob_list if i.name.endswith('.avro')]
    
    destination_bucket = storage_client.bucket(bucket_name) #hardcoded it for now since source and dest buckets are the same

    for blob in blob_files_list:
        blob_copy = source_bucket.copy_blob(
            blob, destination_bucket, f'files/{blob.name}' #automatically creates the files/ folder if not already present
            )
        source_bucket.delete_blob(blob.name)

    

    print(f"moved the files to new folder{blob_copy}")


if __name__ == "__main__":
    move_blob()