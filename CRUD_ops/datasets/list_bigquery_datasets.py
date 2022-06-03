from google.cloud import bigquery

client = bigquery.Client()
# client = bigquery.Client(project='advance-rush-349804') #use this if you want to mention the project name

datasets = list(client.list_datasets()) 
project = client.project

if datasets:
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))
else:
    print("{} project does not contain any datasets.".format(project))