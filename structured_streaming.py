from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Session
spark_session = (
    SparkSession.builder.appName("Structured_Streaming_random_CSV")
    .master("local[4]")
    .getOrCreate()
)

# Structured streaming will always require the specification of schema

schema1 = StructType(
    [
        StructField("IMSI", IntegerType(), True),
        StructField("Activation_date", IntegerType(), True),
        StructField("account_number", IntegerType(), True),
    ]
)


server_1 = (
    spark_session.readStream.format("csv")
    .schema(schema1)
    .option("header", True)
    .option("maxFilesPerTrigger", 1)
    .load("/home/yash/learning/pyspark/data")
)

print(f"Server_1 Streaming??? -> {server_1.isStreaming}")
print("=================================================")
print(f"SCHEMA --------> ")
print(server_1.printSchema())


account_wise_data = server_1.groupBy("account_number").agg(
    collect_list('IMSI').alias('IMSI'),count("account_number").alias("count")
).sort(asc("account_number"))
# print("Accout wise data --->")

query = account_wise_data.writeStream.format("console").outputMode("complete").start()
query.awaitTermination()
    