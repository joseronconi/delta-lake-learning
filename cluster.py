import pyspark
import time
from delta import *

start = time.time()

spark = pyspark.sql.SparkSession.builder.appName("MyApp")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core:2.3.0")\
    .getOrCreate()

#spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "gs://delta_lake_bucket/delta-table"
delta_table = DeltaTable.forPath(spark, path)
print("Delta Table is already created")


# read data from csv2
incremental_data = (
    spark.read.csv('gs://delta_lake_bucket/sample_incremental2.csv', sep=',',header=True, inferSchema=True)
)
# compare reports and MERGE / UPDATE
(
    delta_table.alias("old")
    .merge(
        incremental_data.alias("new"),
        "old.header1 = new.header1 and old.header2 = new.header2")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# Write back delta table
#delta_table.write.format("delta").mode("overwrite").save("delta-table")

end = time.time()

print("END of MERGE: ", end - start, " seconds")

spark.stop()