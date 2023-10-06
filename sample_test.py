# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("My App").getOrCreate()

rdd = spark.read.csv("sample_incremental.csv", header=True, inferSchema=True)


print("THE COUNT IS HERE: ", rdd.count())
# Stop the SparkSession
spark.stop()
