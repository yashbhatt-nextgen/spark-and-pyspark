from pyspark.sql import SparkSession
from pyspark.sql.functions import window

# Create a Spark session
spark = SparkSession.builder \
    .appName("BasicDataStreamExample") \
    .getOrCreate()

# Create a streaming context with a batch interval of 2 seconds
streaming_context = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Performing some basic operations on the streaming data
# For example, counting the occurrences of each value in a column
result_df = streaming_context.groupBy("value").count()

# Writing the results to the console
query = result_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Awaiting termination of the streaming query
query.awaitTermination()
