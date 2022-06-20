import apache_beam as beam
import apache_beam.transforms.sql as sql
import typing
from typing import NamedTuple
from collections import namedtuple
from apache_beam import coders

class TestSchema(NamedTuple):
    username: str
    tweet: str
    timestamp: int
    
argv = [
    '--project=advance-rush-349804',
    '--staging_location=gs://test-bucket-1173/f1',
    '--temp_location=gs://test-bucket-1173/f1',
    '--region=asia-south2',
    '--runner=DataflowRunner',
    '--save_main_session'
]

table_spec = "gs://test-bucket-1173/twitter.avro"
target = "advance-rush-349804:test_dataset_28.test-table-500"

coders.registry.register_coder(TestSchema, coders.RowCoder)

p = beam.Pipeline(argv=argv)

(p
    |'ReadAvroFromGCS_A' >> beam.io.avroio.ReadFromAvro(table_spec)
    |'WriteToRow' >> beam.Map(lambda x: beam.Row(**x)).with_output_types(TestSchema)
    |'Transform' >> sql.SqlTransform("""select * from PCOLLECTION""")
    )


# output = (pipelines_dict
#             |sql.SqlTransform("""select * 
#                                 from left_table as l inner join right_table as r on l.username = r.username""")
#             |beam.Map(lambda row: row.as_dict())
#             |beam.io.WriteToBigQuery(target,schema="SCHEMA_AUTODETECT",
#                                     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
#                                     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#                                     )
#         )
try:
    result = p.run()
    result.wait_until_finish()
    # print(output)
except Exception as e:
    print(e)
# finally:
#     print('Sure')