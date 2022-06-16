from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option(
    'project','advance-rush-349804').option(
        'table','test_dataset_42.transaction_demo').load()

teller = df.select(*['Type','Transaction_Date','Branch','Organization',
    'Acc_no','Cash_Amount','Transaction_type','Application_ID']).filter(F.col('Type')=='Teller')

#this can be complicated when joining the same 2 tables, so when you're doing that always mention an alias for the 2 tables and rename columns if necessary
df_updated = df.alias('a').join(teller.alias('b'), F.col("a.Type")==F.col("b.Type"), "inner").select(
    F.col('a.Type').alias("Type"),F.col('b.Type').alias('Type2'))

df_updated.show()
# .select(df.Type,df.Transaction_Date,df.Branch,df.Is_Match)


df_updated.write.format('bigquery').option(
    'table','test_dataset_42.test_join').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'overwrite').save()

# gcloud dataproc jobs submit pyspark pyspark_joins.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'