from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os
from textblob import TextBlob
import timeit
import time
if __name__ == "__main__":
    
    sc = SparkContext(appName="TweetCount")
    ssc = StreamingContext(sc, 1800)
    topics = ["twitterstream"]
    params = {"bootstrap.servers":"localhost:9092"}
    kafkaStream = KafkaUtils.createDirectStream(ssc,topics,params)
    acc=sc.accumulator(0) 
    parsed = kafkaStream.map(lambda v: v[1])
    
    def get_sentiment(tweet):
        
        start=time.time()
        analysis=TextBlob(tweet)
        end=time.time()
        acc.add(end-start)
        
        if analysis.sentiment.polarity>0:
            return 1
        elif analysis.sentiment.polarity==0:
            return 0
        else: 
            return -1

    def sentiment(tup):
        start=time.time()
        hashs,sentiment = tup
        count,sen = sentiment
        end=time.time()
        acc.add(end-start)
        if sen >= 1:
            return (hashs,count,'positive')
        elif sen == 0:
            return (hashs,count,'neutral')
        else:
            return (hashs,count,'negative')

    def fun(tweet):
        start=time.time()
        hashes = tweet.split(' ')
        trend = [x for x in hashes if x.startswith('#')]
        end=time.time()
        acc.add(end-start)
        if trend:
            return (str(trend[0]).lower(),(1,get_sentiment(tweet)))
        return (str(trend),(0,0))
        
    def trending(rdd):
        start=time.time()
        m = rdd.sortBy(lambda x: x[1], ascending=False).take(10)
        end=time.time()
        acc.add(end-start)
        return rdd.filter(lambda x: x in m)

    hashtags = parsed.map(lambda tweet: fun(tweet))
    hashtagsCounts = hashtags.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1]))
    temp_trending = hashtagsCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    trendinghashtags = temp_trending.transform(lambda rdd: trending(rdd))
    hashtagsSentiments = trendinghashtags.map(lambda rdd: sentiment(rdd))
    time_time=hashtagsSentiments.transform(lambda rdd: print("Cumulative Processing time is ({0}s)".format(acc.value)) or rdd)
    time_time.pprint()
    
    ssc.start()
    ssc.awaitTermination()
