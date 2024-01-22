from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

def perform_window_operation(file_location):
    spark_session = SparkSession.builder.appName("WindowSimpleDemo").getOrCreate()

    data = spark_session.read.csv(file_location, header=True, inferSchema=True)

    print("DataFrame Schema:")
    data.printSchema()

    print("DataFrame Content:")
    data.show()

    window_spec = Window().partitionBy('account_number').orderBy("Activation_date")

    changed_data = data.withColumn("IMSI_count_per_account", count("IMSI").over(window_spec))

    print("DataFrame After Window Operation:")
    changed_data.show()

    return changed_data, spark_session

if __name__ == '__main__':
    file_location = "/home/yash/learning/pyspark/data/data_1.csv"
    result_df, spark = perform_window_operation(file_location)

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Termination signal received. Stopping Spark session.")
        spark.stop()
