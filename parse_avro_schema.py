
def get_avro_schema_from_file(bucket_name,schema_file_name):
    import os
    from google.cloud import storage
    import avro
    from avro.datafile import DataFileReader

    client = storage.Client()

    bucket_name = bucket_name #bucket name
    file_name = schema_file_name #schema file name(.avsc) 

    bucket = client.lookup_bucket(bucket_name)
    if bucket is None:
        raise ValueError('Could not find bucket %s' % bucket_name)

    blob = [i for i in bucket.list_blobs() if i.name==file_name][0] #temporary, we can use a file_name to get the blob later

    blob.download_to_filename(file_name, start=0, end=100000) #write the avsc file to the cloud shell file
    schema = avro.schema.parse(open(file_name, "rb").read()) #read the file



    field_names,types = ([i['name'] for i in schema.to_json()['fields']],
        [i['type'] for i in schema.to_json()['fields']])

    os.remove(file_name)
    return (field_names,types)

def convert_to_named_tuple(file_name,field_names,types):
    from typing import NamedTuple

    type_mapping = {'string':'str',
                    'long':'int',
                    'boolean':'bool',
                    'float':'float',
                    'int':'int',
                    'double':'float'}
                    
    native_types = [type_mapping[i] for i in types] #converting avro types to python native types

    typing_info = [(i,j) for (i,j) in zip(field_names,native_types)] #used to define the types in namedtuple

    class_name = file_name.replace('.avsc','') #for naming the namedtuple class

    return NamedTuple(class_name,typing_info)

if __name__=='__main__':
    bucket_name = 'test-bucket-1173'
    schema_file_name = 'twitter.avsc'

    field_names,types = get_avro_schema_from_file(bucket_name, schema_file_name)

    Schema = convert_to_named_tuple(schema_file_name,field_names,types)

    print(Schema)