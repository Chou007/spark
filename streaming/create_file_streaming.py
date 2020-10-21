from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf.setAppName('TestDStream')
conf.setMaster('local[2]')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)
lines = ssc.textFileStream('file:///usr/local/spark/mycode/streaming/logfile')
words = lines.map(lambda line: line.split())
wordsCount = words.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
wordsCount.pprint()
ssc.start()
ssc.awaitTermination()