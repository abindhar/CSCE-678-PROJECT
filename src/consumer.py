from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os

if __name__ == "__main__":

    sc = SparkContext(appName="TweetCount")
    ssc = StreamingContext(sc, 10)
    topics = ["twitterstream"]
    params = {"bootstrap.servers":"localhost:9092"}
    kafkaStream = KafkaUtils.createDirectStream(ssc,topics,params)
    parsed = kafkaStream.map(lambda v: v[1])
    #parsed.count().map(lambda x:'Tweets in this batch: %s' % x).pprint()

    #hashtags_tweets=kafkaStream.filter(lambda t: len(t['entities']['hashtags']) > 0)
    #withtags=hashtags_tweets.count()
    #withtags.pprint()
    
    words = parsed.flatMap(lambda line: line.split(" "))
    words_with_hashes = words.filter(lambda x: x.startswith('#'))
    pairs = words_with_hashes.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    wordCounts.pprint()
    #wordCounts.saveAsTextFiles('first.csv')

    ssc.start()
    ssc.awaitTermination()
