from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option(
    'project','advance-rush-349804').option(
        'table','test_dataset_42.transaction_demo').load()

#Updating a preexisting column
df = df.withColumn('Is_Match', F.when((F.col("Transaction_Date") == '2022-06-14') & (F.col("Branch") ==1), 1).otherwise(0))

#Updating a new column
df = df.withColumn('New_Is_Match', F.when((F.col("Transaction_Date") == '2022-06-14') & (F.col("Branch") ==1), 1).otherwise(0))

#Updating column using a sql expression
query = """case when Transaction_Date='2022-06-14' and Branch=1 then 1 else 0 end"""
df = df.withColumn('Is_Match_2', F.expr(query))


# change the mode to 'append' to add new data to an existing bq table
df.write.format('bigquery').option(
    'table', 'test_dataset_42.test_spark_output4').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'overwrite').save()

# gcloud dataproc jobs submit pyspark pyspark/write_to_bigquery.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'