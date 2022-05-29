import apache_beam as beam
import apache_beam.transforms.sql as sql
import typing
from typing import NamedTuple
from collections import namedtuple
from apache_beam import coders


argv = [
    '--project=advance-rush-349804',
    '--staging_location=gs://test-bucket-1173/f1',
    '--temp_location=gs://test-bucket-1173/f1',
    '--region=asia-south2',
    '--runner=DataflowRunner',
    '--save_main_session'
]

class TestSchema(NamedTuple):
    username: str
    tweet: str
    timestamp: int

table_spec = "gs://test-bucket-1173/twitter.avro"
target = "advance-rush-349804:test_dataset_28.test-table-500"

p = beam.Pipeline(argv=argv)

coders.registry.register_coder(TestSchema,coders.RowCoder)


source_pipeline = 'left_table'
left_table = (p
                |'ReadAvroFromGCS_A' >> beam.io.avroio.ReadFromAvro(table_spec)
                |'MapLeft' >> beam.Map(lambda x: beam.Row(**x)).with_output_types(TestSchema)
             )

join_pipeline = 'right_table'
right_table = (p
                |'ReadAvroFromGCS_B' >> beam.io.avroio.ReadFromAvro(table_spec)
                |'MapRight' >> beam.Map(lambda x: beam.Row(**x)).with_output_types(TestSchema)
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