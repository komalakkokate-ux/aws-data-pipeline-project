import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .appName("AWS S3 Pipeline") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .getOrCreate()

# AWS keys removed for security
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", IntegerType(), True)
])

df = spark.read.csv(
    "data.csv",
    header=True,
    inferSchema=True
)

print("Original Data:")
df.show()

df_filtered = df.filter(df.amount > 150)

print("Filtered Data:")
df_filtered.show()

df_filtered.toPandas().to_csv("output.csv", index=False)

print("Pipeline executed successfully")