from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from afinn import Afinn

if __name__ == "__main__":

    sc = SparkContext()
    ssc = StreamingContext(sc, 1)

    # A sentiment analysis model
    model = Afinn()

    raw_stream = KafkaUtils.createStream(ssc, "zookeeper:2181", "consumer-group", {"tweets_stream": 1}) \
                           .window(60, 5)

    raw_stream.pprint(20)


    # Start it
    ssc.start()
    ssc.awaitTermination()
