from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Spark Context with two working threads
sc = SparkContext('local[2]', appName='DsteamDemo')

# Local Streaming Context with batch interval of 1 sec
ssc = StreamingContext(sc, 5)

# Now we create a local Dstream that will connect to the stream of input lines from Localhost:9000
stream_lines = ssc.socketTextStream('localhost', 9000)

# Splitting lines into the words
words = stream_lines.flatMap(lambda x: x.split(" "))

# Counting every word in each batches
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x,y: x+y)

# Printing first 10 elements of each RDD generated in this DStream to the console
wordCounts.pprint()


# Start computation
ssc.start()

ssc.awaitTermination()




"""
To run this, put both files in same directory.

Terminal-1 : nc -lk 9000
Terminal-2 : spark-submit word_count_Dstream.py localhost:9000

"""


