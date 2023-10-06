""" 
from delta import DeltaTable
from pyspark.sql import functions as f
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

#conf.set("spark.jars.packages", "io.delta:delta-core:2.4.0")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set(
    "spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
)
spark = SparkSession.builder.appName("bq_test").config(conf=conf).getOrCreate()
"""

import pyspark
import time
from delta import *

start = time.time()

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# read data from csv
#sample_data = (
#    spark.read.csv('sample.csv', header=True, inferSchema=True)
#)
#sample_data.write.format("delta").mode("overwrite").save("delta-table")

path = "delta-table"
if DeltaTable.isDeltaTable(spark, path):
    delta_table = DeltaTable.forPath(spark, path)
    print("Delta Table is already created")
else:
    # read data from csv
    sample_data = (
        spark.read.csv('sample.csv', header=True, inferSchema=True)
    )
    sample_data.write.format("delta").mode("overwrite").save("delta-table")

# read data from csv2
incremental_data = (
    spark.read.csv('sample_incremental2.csv', sep=',',header=True, inferSchema=True)
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

# ---------------- firula -----------------

enable_firula = False
if enable_firula == True:
    # load delta table to df
    df = (spark.read.format("delta").load("delta-table"))

    # Transform to Pandas    
    df_pandas = df.toPandas()

    # Write df to csv
    (df_pandas.to_csv("last_delta_table.csv", index=False)
    )
spark.stop()