from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
uri = "gs://test-bucket-1173/twitter.avro"
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option('project','advance-rush-349804').option('table','test_dataset_42.test-table-4') \
  .load().select(F.col('username'))

# df.createOrReplaceTempView('df') #create a temp view for running sql
# columns = ['username']
# newRow = spark.createDataFrame([("pratyush"),("pratyush2"),("prb3")])
# new_df = spark.sql('INSERT INTO df(username) VALUES("pratyush"),("pratyush2"),("pratyush3") ')

# df.union(newRow)

# Saving the data to BigQuery
df.write.format('bigquery').option(
    'table', 'test_dataset_42.test_spark_output').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'append').save()

# gcloud dataproc jobs submit pyspark pyspark/write_avro_to_bigquery.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'