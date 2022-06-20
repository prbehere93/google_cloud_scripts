from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType

"""To do this we need to construct a new 'Rows' of data then convert them 
to a dataframe which is then appended to an existing BQ table. The schema of the 
rows has to match the BQ table, you can also explicitly mention the schema. 
"""

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

# schema = StructType() \
#       .add("username",StringType(),True)

row = [Row(username = 'Pratyush')]
# newRow = spark.createDataFrame(row,schema)
newRow = spark.createDataFrame(row)

# change the mode to 'append' to add new data to an existing bq table
newRow.write.format('bigquery').option(
    'table', 'test_dataset_42.test_spark_output').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'append').save()

# gcloud dataproc jobs submit pyspark insert_row_bq --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1 --properties spark.jars.packages='org.apache.spark:spark-avro_2.12:2.4.1'