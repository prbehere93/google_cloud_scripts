from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option(
    'project','advance-rush-349804').option(
        'table','test_dataset_42.transaction_demo').load()

teller = df.select(*['Type','Transaction_Date','Branch','Organization',
    'Acc_no','Cash_Amount','Transaction_type','Application_ID']).filter(F.col('Type')=='Teller').toPandas()

row_iterator = teller.iterrows()
for i, row in row_iterator:
    df = df.withColumn('Is_Match', F.when((F.col("Transaction_Date") == row['Transaction_Date']) & (F.col("Branch") ==row['Branch']) & (F.col("Organization") == row['Organization']), 1).otherwise(F.col('Is_Match'))

df.show()
# def func1(x,row):
#     if ((x['Transaction_Date']==row['Transaction_Date']
#         ) and (x['Branch']==row['Branch']) and (x['Organization']==row['Organization']):
#         x['Is_Match']=1
#     return (x)

# for i, row in row_iterator:
#     df=df.rdd.map(lambda x,row: func1(x,row)) 
    


# df.createOrReplaceTempView('df') #create a temp view for running sql
# columns = ['username']
# newRow = spark.createDataFrame([("pratyush"),("pratyush2"),("prb3")])
# new_df = spark.sql('INSERT INTO df(username) VALUES("pratyush"),("pratyush2"),("pratyush3") ')

# df.union(newRow)

# Saving the data to BigQuery
# df.write.format('bigquery').option(
#     'table', 'test_dataset_42.test_spark_output').option(
#         "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
#             'append').save()

df.write.format('bigquery').option(
    'table', 'test_dataset_42.test_spark_output3').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'overwrite').save()

# gcloud dataproc jobs submit pyspark pyspark/write_to_bigquery.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'