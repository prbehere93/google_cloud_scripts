from google.cloud import bigquery

project = 'advance-rush-349804'
dataset = 'test_dataset_42'
table_name = 'test_table_42'

client = bigquery.Client()

try:
    table = client.get_table("{}.{}.{}".format(project, dataset, table_name))

    rows_to_insert = [{u"full_name": "pratyush", u"age": 18},
                        {u"full_name": "gaunterodimm", u"age": 108}]

    errors = client.insert_rows_json(table, rows_to_insert)
    if errors == []:
        print(f"successfully inserted rows into the {table_name} table")
    else:
        print(errors)
except Exception as e:
    print(e)
