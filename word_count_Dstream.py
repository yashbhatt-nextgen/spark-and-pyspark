from pyspark import SparkContext
from pyspark.streaming import StreamingContext

class DStreamDemo:
    _sc = None

    def __init__(self, host, port, batch_interval):
        if not DStreamDemo._sc:
            DStreamDemo._sc = SparkContext('local[2]', appName='DstreamDemo')

        self.ssc = StreamingContext(DStreamDemo._sc, batch_interval)
        self.stream_lines = self.ssc.socketTextStream(host, port)

    def process_stream(self):
        words = self.stream_lines.flatMap(lambda x: x.split(" "))
        pairs = words.map(lambda word: (word, 1))
        word_counts = pairs.reduceByKey(lambda x, y: x + y)
        word_counts.pprint()

    def start_streaming(self):
        self.ssc.start()
        self.ssc.awaitTermination()

if __name__ == "__main__":
    host = 'localhost'
    port = 9000
    batch_interval = 5

    dstream_demo = DStreamDemo(host, port, batch_interval)
    dstream_demo.process_stream()
    dstream_demo.start_streaming()
