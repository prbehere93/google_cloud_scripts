import apache_beam as beam
import apache_beam.transforms.sql as sql

argv = [
    '--project=advance-rush-349804',
    '--staging_location=gs://test-bucket-1173/f1',
    '--temp_location=gs://test-bucket-1173/f1',
    '--region=us-central1',
    '--runner=DataflowRunner',
    '--save_main_session'
]

table_spec = "gs://test-bucket-1173/timestamp.csv"
# target = "advance-rush-349804:test_dataset_28.test-table-49"
target = "gs://test-bucket-1173/timestamp2.csv"

p = beam.Pipeline(argv=argv)

# schema = ({'fields': [{'name': 'date', 'type': 'STRING', 'mode': 'REQUIRED'}]})

def convertDate(data):
    data = data.split(' ')[0] # code the date conversion here
    return data

(p 
    | f'CreateDateTime' >> beam.Create(['17-06-2022 12:10','17-06-2022 12:55','18-06-2022 13:10'])
    | f'ConvertDate' >> beam.Map(convertDate)
    | f'writecsv' >> beam.io.WriteToText(target,num_shards=1)
)

try:
    p.run().wait_until_finish()
except Exception as e:
    print(e)
finally:
    print('Sure')