from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('test').getOrCreate()
gcs_bucket = 'dataproc-bucket-42'

df = spark.read.format("bigquery").option(
    'project','advance-rush-349804').option(
        'table','test_dataset_42.transaction_demo').load()

#selecting multiple columns and filtering them on the basis of some condition
df = df.drop(F.col("Type"))

df.show()


