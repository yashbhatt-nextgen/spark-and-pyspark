from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class WindowDemo:
    def __init__(self, file_path):
        self.spark_session = SparkSession.builder.appName("Window_Simple_Demo").getOrCreate()
        self.file_path = file_path
        self.df = self.read_csv()

    def read_csv(self):
        df = self.spark_session.read.csv(self.file_path, header=True, inferSchema=True)
        df = df.withColumn('Activation_date', to_date('Activation_date', 'dd-MM-yyyy'))
        return df

    def define_window(self):
        return Window().orderBy('Activation_date').rangeBetween(-180*60, 0)

    def process_data(self):
        window_spec = self.define_window()
        result_df = self.df.withColumn("in_last_3_minutes", max("timestamp").over(window_spec))
        final_result = result_df.filter(result_df["in_last_3_minutes"])
        return final_result

    def show_result(self):
        print("DataFrame ->")
        self.df.show()
        print("======================================================")
        self.process_data().show()

if __name__ == "__main__":
    file_path = '/home/yash/learning/pyspark/data/data_1.csv'
    window_demo = WindowDemo(file_path)
    window_demo.show_result()
