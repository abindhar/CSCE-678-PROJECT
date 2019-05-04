from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import sys
import os
from textblob import TextBlob

if __name__ == "__main__":

    sc = SparkContext(appName="TweetCount")
    ssc = StreamingContext(sc, 10)
    topics = ["twitterstream"]
    params = {"bootstrap.servers":"localhost:9092"}
    kafkaStream = KafkaUtils.createDirectStream(ssc,topics,params)
    parsed = kafkaStream.map(lambda v: v[1])

    def get_sentiment(tweet):
        analysis=TextBlob(tweet)
        if analysis.sentiment.polarity>0:
            return 1
        elif analysis.sentiment.polarity==0:
            return 0
        else: 
            return -1

    def sentiment(tup):
        hashs,sentiment = tup
        count,sen = sentiment
        if sen >= 1:
            return (hashs,count,'positive')
        elif sen == 0:
            return (hashs,count,'neutral')
        else:
            return (hashs,count,'negative')

    def fun(tweet):
        hashes = tweet.split(' ')
        trend = [x for x in hashes if x.startswith('#')]
        if trend:
            return (str(trend[0]).lower(),(1,get_sentiment(tweet)))
        return (str(trend),(0,0))
        
    def trending(rdd):
        m = rdd.sortBy(lambda x: x[1], ascending=False).take(10)
        return rdd.filter(lambda x: x in m)
    
    hashtags = parsed.map(lambda tweet: fun(tweet))
    #hashtags.pprint(100)
    hashtagsCounts = hashtags.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+y[1]))
    temp_trending = hashtagsCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    trendinghashtags = temp_trending.transform(lambda rdd: trending(rdd))
    hashtagsSentiments = trendinghashtags.map(lambda rdd: sentiment(rdd))
    hashtagsSentiments.pprint(100)

    ssc.start()
    ssc.awaitTermination()
