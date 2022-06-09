from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
uri = "gs://test-bucket-1173/twitter.avro"
df = spark.read.format("avro").load(uri).select(F.col('username'))

gcs_bucket = 'dataproc-bucket-42'


# Saving the data to BigQuery
df.write.format('bigquery').option(
    'table', 'test_dataset_42.test_spark_output').option(
        "temporaryGcsBucket", gcs_bucket).mode(
            'overwrite').save()

# gcloud dataproc jobs submit pyspark pyspark/write_avro_to_bigquery.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'