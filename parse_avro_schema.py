
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


import apache_beam as beam
import apache_beam.transforms.sql as sql
from apache_beam import coders
from collections import namedtuple
from typing import NamedTuple

argv = [
'--project=advance-rush-349804',
'--staging_location=gs://test-bucket-1173/f1',
'--temp_location=gs://test-bucket-1173/f1',
'--region=asia-south2',
'--runner=DataflowRunner',
'--save_main_session'
]

bucket_name = 'test-bucket-1173'
schema_file_name1 = 'twitter.avsc'
# schema_file_name2 = 'twitter.avsc'

field_names1,types1 = get_avro_schema_from_file(bucket_name, schema_file_name1)
# field_names2,types2 = get_avro_schema_from_file(bucket_name, schema_file_name2)

Schema1 = convert_to_named_tuple(schema_file_name1,field_names1,types1)
# Schema2 = convert_to_named_tuple(schema_file_name2,field_names2,types2)



table_spec = "gs://test-bucket-1173/twitter.avro"
target = "advance-rush-349804:test_dataset_28.test-table-5000"

p = beam.Pipeline(argv=argv)


coders.registry.register_coder(Schema1,coders.RowCoder)
# coders.registry.register_coder(Schema2,coders.RowCoder)


source_pipeline = 'left_table'
left_table = (p
                |'ReadAvroFromGCS_A' >> beam.io.avroio.ReadFromAvro(table_spec)
                |'MapLeft' >> beam.Map(lambda x: beam.Row(**x)).with_output_types(Schema1)
            )

join_pipeline = 'right_table'
right_table = (p
                |'ReadAvroFromGCS_B' >> beam.io.avroio.ReadFromAvro(table_spec)
                |'MapRight' >> beam.Map(lambda x: beam.Row(**x)).with_output_types(Schema1)
            )

pipelines_dict = {source_pipeline:left_table,
                    join_pipeline:right_table}

output = (pipelines_dict
            |sql.SqlTransform("""select * from left_table as l inner join right_table as r on l.username=r.username""")
            |beam.Map(lambda row: row._asdict())
            |beam.io.WriteToBigQuery(target,schema="SCHEMA_AUTODETECT",
                                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                    )
        )
try:
    p.run().wait_until_finish()
    print(output)
except Exception as e:
    print(e)
finally:
    print('Sure')