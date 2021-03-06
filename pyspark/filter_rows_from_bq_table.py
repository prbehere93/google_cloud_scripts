from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option(
    'project','advance-rush-349804').option(
        'table','test_dataset_42.transaction_demo').load()

#selecting multiple columns and filtering them on the basis of some condition
df = df.select(*['Type','Transaction_Date','Branch','Organization',
    'Acc_no','Cash_Amount','Transaction_type','Application_ID']).filter(F.col('Type')=='Teller')

df.show()

# df.createOrReplaceTempView("tempdf")
# df = spark.sql("SELECT * FROM tempdf WHERE Type='Teller'")


#The below code can be used to write the filtered df into a bigquery table
# df.write.format('bigquery').option(
#     'table', 'test_dataset_42.test_spark_output4').option(
#         "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
#             'overwrite').save()

# gcloud dataproc jobs submit pyspark pyspark/write_to_bigquery.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'