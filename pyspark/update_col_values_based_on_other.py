from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option(
    'project','advance-rush-349804').option(
        'table','test_dataset_42.transaction_demo').load()

df_CTR = df.filter(F.col('Type')=='Teller').toPandas()

df_CTR.iterrows()

for index,row in df_CTR.iterrows():
    df = df.withColumn('Is_Match', F.when((F.col("Transaction_Date") == row['Transaction_Date']
    ) & (F.col("Branch") == row['Branch']) & (
        F.col("Acc_No") == row['Acc_No']), 1).otherwise(df.Is_Match))


# change the mode to 'append' to add new data to an existing bq table
df.write.format('bigquery').option(
    'table', 'test_dataset_42.test_spark_output4').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'overwrite').save()

# gcloud dataproc jobs submit pyspark update_col_values_based_on_other.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'