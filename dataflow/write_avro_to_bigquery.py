import apache_beam as beam
# table_schema = bigquery.TableReference(
#     projectId = "advance-rush-349804",
#     datasetId = "advance-rush-349804.test_dataset_28",
#     tableId = "test-table25"
# )

table_spec = "gs://test-bucket-1173/twitter.avro"
target = "advance-rush-349804:test_dataset_28.test-table-499080"



argv = [
    '--project=advance-rush-349804',
    '--staging_location=gs://test-bucket-1173/f1',
    '--temp_location=gs://test-bucket-1173/f1',
    '--region=asia-south2',
    '--runner=DataflowRunner'
]

p = beam.Pipeline(argv=argv)

(p 
    | f'ReadAvro' >> beam.io.avroio.ReadFromAvro(table_spec)
    | f'WriteToBigQuery' >> beam.io.WriteToBigQuery(target,
        schema="SCHEMA_AUTODETECT",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
)

p.run()