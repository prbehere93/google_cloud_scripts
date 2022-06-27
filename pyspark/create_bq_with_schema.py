import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
import pyspark.sql.functions as F

# conf = pyspark.SparkConf().setAll([('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar')])

spark = SparkSession.builder.appName('test').getOrCreate()


# print(spark.sparkContext.getConf().getAll())
gcs_bucket = 'dataproc-bucket-42'

schema = StructType() \
      .add("RecordNumber",IntegerType(),True) \
      .add("TaxReturnsFiled",StringType(),True) \
      .add("EstimatedPopulation",IntegerType(),True) \
      .add("TotalWages",IntegerType(),True) \
      .add("Notes",StringType(),True) \
      .add("Date",DateType(),True)    

emp_RDD = spark.sparkContext.emptyRDD()

df = spark.createDataFrame(data=emp_RDD,schema=schema)

# change the mode to 'append' to add new data to an existing bq table
df.write.format('bigquery').option(
    'table', 'test_dataset_42.test_output_7777').option(
        "temporaryGcsBucket", gcs_bucket).mode( #mentioning a temp bucket is necessary otherwise the job does not execute
            'overwrite').save()

# gcloud dataproc jobs submit pyspark create_bq_with_schema.py --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar --cluster example-cluster --region us-central1